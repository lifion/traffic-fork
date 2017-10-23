package main

/*
this package is a base for a web service
features:
- reverse proxy: forward all requests to the destination server; this server is called production
- duplicate traffic: forward all requests to both destinations; first to production and then to staging
replace request production cookies and csrf token with cached staging cookies and csrf token
- morf input before sending the request:
* morf URI: if the request is to "/api/ui-web-service/" a character from the remainder of the URI is changed
* morf header: replace a random character in a random header value
*/

import (
   "./forktraffic"
   "./ping"
   "bytes"
   "crypto/tls"
   "encoding/json"
   "fmt"
   "io/ioutil"
   "log"
   "net"
   "net/http"
   "net/http/httputil"
   "net/url"
   "os"
   "os/signal"
   "runtime/pprof"
   "strings"
   "syscall"
   "time"
)

const ListenerDefaultPort string = ":8888"
const TransportTimeoutSec int = 60
const IdleConnectionsLimit int = 2000
const NumPendingRequests int = 10000
const MaxHeaderKb int = 8

//
// define the input parameters
type InputParams struct {
   Port                string
   Production, Staging string
   LogFlags            int
   forktraffic.TestOptions
   CpuProfileFilename  string
   HeapProfileFilename string
}

//
// read the configuration from the configuration file
func readConfigFile(configFileName string, inputParams *InputParams) InputParams {
   // read the configuration file
   fileInput, err := ioutil.ReadFile(configFileName)
   if err != nil {
      log.Printf("Warning - configuration file: %+v", err)
   } else {
      json.Unmarshal(fileInput, inputParams)
   }

   return *inputParams
}

//
// display help
//
func printHelp() {
   fmt.Println("usage:")
   fmt.Println(os.Args[0], " :port production [staging] [-H,--morfHeader] [-U,--morfUri] [[-f,--file] [file]] [--help]")
   fmt.Println("   :port              TCP port to listen on; default = 8888")
   fmt.Println("   production         http://destination:port/ the location of the next hop to forward all requests")
   fmt.Println("   staging            http://staging:port/ optional destination to duplicate the traffic to")
   fmt.Println("   -q, --quiet        no logging; quiet mode")
   fmt.Println("   -l, --logFlags     [date, time, microsec, longfile, shortfile, UTC]; see the Golang log package")
   fmt.Println("   -U, --morfUri      test option: perform URI morfing when destination is " + forktraffic.DefaultMorfUriBase)
   fmt.Println("   -H, --morfHeader   test option: make one change in a single random header value")
   fmt.Printf("   -f, --file[=file]  read program parameters from configuration file; default: ./redirector.json\n")
   fmt.Println("   -?, --help         display this help and exit")
   os.Exit(0)
}

//
// get input parameters
//
// declare the supported input options
type inputOption int

const (
   unknown inputOption = iota
   setLogFlags
   inputFile
   cpuProfile
   heapProfile
   displayHelp
   morfHeaderFlag
   morfUriFlag
)

func getInputParams() InputParams {
   runOptions := [...]struct {
      key, name string
      hasValue  bool
      inputOption
   }{
      {"-U", "--morfUri", true, morfUriFlag},
      {"-H", "--morfHeader", false, morfHeaderFlag},
      {"-l", "--logLevel", true, setLogFlags},
      {"-f", "--file", true, inputFile},
      {"", "--CpuProfileFilename", true, cpuProfile},
      {"", "--HeapProfileFilename", true, heapProfile},
      {"-?", "--help", false, displayHelp},
   }

   userInput := InputParams{
      Port: ListenerDefaultPort,
      Production: "http://router/",
      Staging: "",
      LogFlags: log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile | log.LUTC,
      TestOptions: forktraffic.TestOptions{ MorfUri: false, MorfHeader: false, MorfUriBase: forktraffic.DefaultMorfUriBase},
      CpuProfileFilename: "",
      HeapProfileFilename: ""}

   configFileName := "./redirector.json"
   iInParam := 0
   for iArg := 1; iArg < len(os.Args); iArg++ {
      if os.Args[iArg][0] == '-' {
         inOption := unknown
         inArg := os.Args[iArg]
         for iOpt := range runOptions {
            // we have a "-" key option "-?" and it matches our input
            if (runOptions[iOpt].key != "" && inArg == runOptions[iOpt].key) ||
               // or, we have a "--" key name as the prefix of the key=value
               (len(inArg) >= len(runOptions[iOpt].name) &&
                  (strings.EqualFold(inArg[:len(runOptions[iOpt].name)], runOptions[iOpt].name))) {

               // this is our input option
               inOption = runOptions[iOpt].inputOption

               //
               // get the input value
               inValue := ""
               if runOptions[iOpt].hasValue {
                  if len(inArg) > len(runOptions[iOpt].name) {
                     inValue = inArg[len(runOptions[iOpt].name):]
                     if inValue[0] == '=' || inValue[0] == ' ' {
                        inValue = inValue[1:]
                     }
                     // there are still parameters on the command line -> then this one is a value
                  } else if iArg < (len(os.Args)-1) &&
                     os.Args[iArg+1][0] != '-' {
                     iArg++
                     inValue = os.Args[iArg]
                  }
               }

               //
               // get the input run options
               if inOption == displayHelp {
                  printHelp()
               } else if inOption == inputFile {
                  // input is "--file="
                  if inValue != "" { // input filename
                     configFileName = inValue
                  }
                  userInput = readConfigFile(configFileName, &userInput)
               } else if inOption == morfUriFlag {
                  userInput.MorfUri = true
                  if inValue != "" {
                     userInput.MorfUriBase = inValue
                  }
               } else if inOption == morfHeaderFlag {
                  userInput.MorfHeader = true
               } else if inOption == setLogFlags {
                  type logFlagDescription struct {
                     flag int
                     name string
                  }
                  logFlagsData := []logFlagDescription{
                     {log.Ldate, "date"},
                     {log.Ltime, "time"},
                     {log.Lmicroseconds, "microsec"},
                     {log.Llongfile, "longfile"},
                     {log.Lshortfile, "shortfile"},
                     {log.LUTC, "UTC"},
                  }
                  logFlags := 0
                  for _, strFlag := range logFlagsData {
                     if 0 == strings.Compare(inValue, strFlag.name) {
                        logFlags |= strFlag.flag
                     }
                  }
                  log.SetFlags(logFlags)
               } else if inOption == cpuProfile {
                  if inValue != "" {
                     userInput.CpuProfileFilename = inValue
                  } else {
                     log.Printf("Warning - CPU profiling requires a profile output file")
                  }
               } else if inOption == heapProfile {
                  if inValue != "" {
                     userInput.HeapProfileFilename = inValue
                  } else {
                     log.Printf("Warning - Heap profiling requires a profile output file")
                  }
               }
            }
         }

         // check that the input is a valid key
         if inOption == unknown {
            log.Printf("Warning: invalid input option: %v\n\n", inArg)
            // printHelp()
         }
      } else if os.Args[iArg] != "" {
         iInParam++
         switch iInParam {
         case 1:
            userInput.Port = os.Args[iArg]
            if userInput.Port[0] != ':' {
               userInput.Port = ":" + userInput.Port
            }
         case 2:
            userInput.Production = os.Args[iArg]
         case 3:
            userInput.Staging = os.Args[iArg]
         default:
            log.Print("error: too many arguments\n\n")
            printHelp() // program will exit here
         }
      }
   }

   b, err := json.Marshal(userInput)
   if err == nil {
      var out bytes.Buffer
      json.Indent(&out, b, "", "  ")
      log.Printf("program input:")
      out.WriteTo(os.Stdout)
      log.Printf("program input: %#v", userInput)
   }
   return userInput
}

//
// program start
//
func main() {
   progInput := getInputParams()

   log.Print("listen port = ", progInput.Port)
   log.Print("production = ", progInput.Production)
   log.Print("staging = ", progInput.Staging)
   log.Printf("testing: { \"morfUri\":%v, \"morfHeader\":%v, \"morfUriBase\":%s}", progInput.MorfUri, progInput.MorfHeader, progInput.MorfUriBase)

   // the production path is required and needs to be a valid url
   destProduction, err := url.Parse(progInput.Production)
   if err != nil || destProduction.Scheme == "" || destProduction.Host == "" {
      log.Printf("production path need to be a valid destination")
      log.Fatal(err)
   } else {
      // add root path if missing
      if destProduction.Path == "" {
         destProduction.Path = "/"
      }

      destStaging, err := url.Parse(progInput.Staging)
      if err != nil {
         log.Fatal(err)
      } else {

         // check that the staging destination is valid
         if progInput.Staging != "" {
            if destStaging.Scheme == "" || destStaging.Host == "" {
               log.Print("error: staging path is invalid")
               os.Exit(1)
            }
            if destStaging.Path == "" {
               destStaging.Path = "/"
            }
         }

         //
         tr := new(http.Transport)
         tr.MaxIdleConns = IdleConnectionsLimit
         tr.MaxIdleConnsPerHost = IdleConnectionsLimit
         tr.IdleConnTimeout = 15 * time.Second
         tr.DisableCompression = true
         tr.Proxy = nil
         tr.ResponseHeaderTimeout = time.Duration(TransportTimeoutSec) * time.Second
         tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
         tr.DialContext = (&net.Dialer{
            Timeout:   time.Duration(TransportTimeoutSec) * time.Second,
            KeepAlive: time.Duration(TransportTimeoutSec) * time.Second,
            DualStack: false,
         }).DialContext

         //
         // ping handler
         pingMgr := &ping.Manger{ ServiceName: "forktraffic", StatusOk: false }
         pingMgr.Init()

         //
         // this is our main data structure
         //
         destStag := &http.Client{Transport: tr, CheckRedirect: nil, Timeout: time.Duration(TransportTimeoutSec) * time.Second}
         reqManager := &forktraffic.RequestManager{
            UrlProduction:   destProduction,
            DestProduction:  httputil.NewSingleHostReverseProxy(destProduction),
            UrlStaging:      destStaging,
            DestStaging:     destStag,
            TestOptions:     progInput.TestOptions,
            CacheData:       make(map[string]*forktraffic.StagKeys),
            PendingRequests: make(chan *forktraffic.PendingRequest, NumPendingRequests)}
         emptyKey := new(forktraffic.StagKeys)
         reqManager.CacheData[""] = emptyKey
         reqManager.DestProduction.Transport = tr
         reqManager.Init()

         // start staging transport handler
         go reqManager.StagingHandler()

         // for staging certificate
         http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

         // define server properties
         httpServer := &http.Server{
            Addr:              progInput.Port,
            Handler:           nil,
            ReadTimeout:       time.Duration(TransportTimeoutSec) * time.Second,
            WriteTimeout:      time.Duration(TransportTimeoutSec) * time.Second,
            IdleTimeout:       time.Duration(TransportTimeoutSec) * time.Second,
            ReadHeaderTimeout: time.Duration(TransportTimeoutSec) * time.Second,
            MaxHeaderBytes:    MaxHeaderKb * 1024,
         }
         httpServer.SetKeepAlivesEnabled(true)

         // setup signals handler and shutdown
         signals := make(chan os.Signal, 1)
         signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
         go func() {
            for sig := range signals { // wait for signal
               log.Printf("received signal: %+v; stopping program...", sig)
               httpServer.Shutdown(nil)
            }
         }()

         // start CPU profiling
         if progInput.CpuProfileFilename != "" {
            fCpuProfile, err := os.Create(progInput.CpuProfileFilename)
            if err == nil {
               pprof.StartCPUProfile(fCpuProfile)
               defer func() { pprof.StopCPUProfile(); fCpuProfile.Close() }()
            }
         }

         // start the listener, now we serve requests
         pingMgr.Set(true)
         log.Printf("%v started...", os.Args[0])
         status := httpServer.ListenAndServe()

         // server stopped ...

         // dump heap profiling
         if progInput.HeapProfileFilename != "" {
            fHeapProf, err := os.Create(progInput.HeapProfileFilename)
            if err != nil {
               pprof.WriteHeapProfile(fHeapProf)
               fHeapProf.Close()
            }
         }
         log.Printf("program stopped.")
         if status != nil {
            log.Fatal(status)
         }
      }
   }
}
