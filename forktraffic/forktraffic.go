package forktraffic

import (
   "../ping"
   "bytes"
   "container/heap"
   "crypto/rand"
   "encoding/base64"
   "io"
   "io/ioutil"
   "log"
   "math"
   "math/big"
   "net/http"
   "net/http/httputil"
   "net/url"
   "strconv"
   "strings"
   "sync/atomic"
   "time"
   "unicode"
)

//
// extend time
func UnixMs(t time.Time) int64 {
   return t.UnixNano() / 1000000
}

//
// extend random
func randInt(maxRand int) int {
   bigCh, _ := rand.Int(rand.Reader, big.NewInt(int64(maxRand)))
   return int(bigCh.Uint64())
}

//
// our http headers
const httpNameHeader string = "Http-Splitter"
const httpForwardedHeader string = "X-Forwarded-By"
const httpDuplicateHeader string = "X-Duplicate-By"
const DefaultMorfUriBase string = "/api/"

//
// test options
type TestOptions struct {
   MorfUri     bool
   MorfHeader  bool
   MorfUriBase string
}

//
// staging data to replace production keys when forwarding to staging
type StagKeys struct {
   sessionKey, sessionTtl string
   csrfToken              string
   Expiration             int64
}

// heap of session tokens expiration
type tokenExpiration struct {
   time  int64  // time of expiration
   token string // token string
   // index is needed by update and is maintained by the heap.Interface methods
   index int
}

// priority queue for keeping track of tokens expiration
type tokenExpirationQueue []*tokenExpiration

//
// required by heap for priority-queue implementation
func (teq *tokenExpirationQueue) Len() int { return len(*teq) }
func (teq *tokenExpirationQueue) Less(i, j int) bool {
   return (*teq)[i] != nil && (*teq)[j] != nil && (*teq)[i].time < (*teq)[j].time
}
func (teq *tokenExpirationQueue) Swap(i, j int) {
   (*teq)[i], (*teq)[j] = (*teq)[j], (*teq)[i]
   (*teq)[i].index = i
   (*teq)[j].index = j
}
func (teq *tokenExpirationQueue) Push(x interface{}) {
   n := teq.Len()
   item := x.(*tokenExpiration)
   item.index = n
   *teq = append(*teq, item)
}
func (teq *tokenExpirationQueue) Pop() interface{} {
   old := *teq
   n := old.Len()
   item := old[n-1]
   item.index = -1 // for safety
   *teq = old[0 : n-1]
   return item
}

// update modifies the time and token of a tokenExpiration in the queue
func (teq *tokenExpirationQueue) update(item *tokenExpiration, token string, time int64) {
   item.token = token
   item.time = time
   heap.Fix(teq, item.index)
}

// queued request to send to staging
type PendingRequest struct {
   req        *http.Request
   body       io.ReadCloser
   requestKey string
   sessionKey string
   keyExpires int64
}

//
// handle request forwarding to staging
//
type RequestManager struct {
   // production
   UrlProduction  *url.URL
   DestProduction *httputil.ReverseProxy

   // staging
   UrlStaging  *url.URL
   DestStaging *http.Client

   // ping manager
   pingManager ping.Manger

   // test scenarios
   TestOptions

   // staging cached keys
   cacheId       int64
   forwardPrefix string
   CacheData     map[string]*StagKeys

   tokensExpirationList tokenExpirationQueue

   // pending requests to send to staging
   PendingRequests chan *PendingRequest
}

//
// initialize the request manager
// - set path handlers and response handler
func (reqMgr *RequestManager) Init() {
   reqMgr.cacheId = 0
   i64rand, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
   reqMgr.forwardPrefix = strconv.FormatUint(i64rand.Uint64()+uint64(time.Now().UnixNano()), 16) + "-"

   http.HandleFunc("/", reqMgr.handleRequest)

   reqMgr.DestProduction.ModifyResponse = reqMgr.respHandler
   reqMgr.DestProduction.FlushInterval = 0

   reqMgr.tokensExpirationList = make(tokenExpirationQueue, 0)
   heap.Init(&reqMgr.tokensExpirationList)
}

// update the unique id
func (reqMgr *RequestManager) createReqId() string {
   id := atomic.AddInt64(&reqMgr.cacheId, 1)
   return reqMgr.forwardPrefix + strconv.FormatInt(id, 16)
}

// cache the response keys
func (reqMgr *RequestManager) cacheResponse(prodSessionKey string, resp *http.Response, prodKeyExpiration int64) {

   // update the staging keys data base
   if prodSessionKey == "" || resp.StatusCode >= http.StatusBadRequest { // 400
      return
   }

   // find our key
   var stagKey *StagKeys
   var newKey bool = false
   stagKey = reqMgr.CacheData[prodSessionKey]
   if stagKey == nil {
      newKey = true
      stagKey = new(StagKeys)
   }

   // check if this is a staging response
   var stagKeyExpiration int64 = 0
   var stagKeyMaxAge int = 0
   if resp != nil {
      for _, cc := range resp.Cookies() {
         if strings.EqualFold(cc.Name, "csrfToken") {
            stagKey.csrfToken = cc.Value
         } else if strings.EqualFold(cc.Name, "sessionKey") {
            stagKey.sessionKey = cc.Value
            stagKeyExpiration = UnixMs(cc.Expires)
            stagKeyMaxAge = cc.MaxAge
         } else if strings.EqualFold(cc.Name, "sessionTtl") {
            stagKey.sessionTtl = cc.Value
         }
      }
   }

   // check for a token expiration
   if prodKeyExpiration != 0 && stagKey.Expiration != prodKeyExpiration {
      stagKey.Expiration = prodKeyExpiration
   } else if stagKey.Expiration == 0 {
      stagKey.Expiration = UnixMs(time.Now()) + 60*20000
   }

   // logout, delete the session
   tNow := UnixMs(time.Now())
   if stagKey.sessionKey == "" && !(stagKeyExpiration > tNow || stagKeyMaxAge > 0) {
      log.Printf("stagKeyExpiration: %+v", stagKeyExpiration)
      delete(reqMgr.CacheData, prodSessionKey)
      return
   }

   // this is a new key, add it to the cache and expiration priority queue
   if newKey {
      // keep staging keys
      reqMgr.CacheData[prodSessionKey] = stagKey

      // expiration item
      listItem := &tokenExpiration{
         time:  prodKeyExpiration,
         token: prodSessionKey,
      }

      // can we reuse old token?
      reUseItem := false
      if len(reqMgr.tokensExpirationList) > 0 {
         item := reqMgr.tokensExpirationList[0]
         if item.token == prodSessionKey {
            reUseItem = true
         } else {
            if item.time <= tNow {
               firstKey := reqMgr.CacheData[item.token]
               if firstKey == nil || firstKey.Expiration <= tNow {
                  reUseItem = true // item expired
               } else {
                  // fix the first item, put it back into the expiration list in its new place
                  reqMgr.tokensExpirationList.update(reqMgr.tokensExpirationList[0], item.token, firstKey.Expiration)
               }
            }
         }
      }

      if reUseItem {
         // reuse the first item in the expiration list
         reqMgr.tokensExpirationList.update(reqMgr.tokensExpirationList[0], prodSessionKey, prodKeyExpiration)
      } else {
         // add a new item to the expiration list
         heap.Push(&reqMgr.tokensExpirationList, listItem)
      }
   }
}

//
// this is the main request handler for "/" path
// reverse proxy to production and store POST data to forward to staging
//
func (reqMgr *RequestManager) handleRequest(respw http.ResponseWriter, req *http.Request) {

   var bodyReader io.ReadCloser = nil
   if reqMgr.UrlStaging.Scheme != "" && strings.EqualFold(req.Method, "POST") && req.Body != nil {
      // copy the request body
      bodyBuf, _ := ioutil.ReadAll(req.Body)

      bodyReader = ioutil.NopCloser(bytes.NewBuffer(bodyBuf))

      // Restore the io.ReadCloser to its original state
      req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBuf))
   }

   // morf the request URI
   if reqMgr.MorfUri {
      morfUri(req, reqMgr.MorfUriBase)
   }

   // morf a request header
   if reqMgr.MorfHeader {
      morfHeader(req)
   }

   // send the request to production
   req.Host = reqMgr.UrlProduction.Host
   reqMgr.DestProduction.ServeHTTP(respw, req)

   // send to staging
   respHdr := respw.Header()
   reqMgr.forwardHandler(req, respHdr, bodyReader)
}

//
// morf the request URI
// replace one character from ui-web-service request
//
func morfUri(req *http.Request, morfUriBase string) {
   l := len(req.URL.Path)
   if l > len(morfUriBase) && req.URL.Path[:len(morfUriBase)] == morfUriBase {
      b := []byte(req.URL.Path)
      iCh := randInt(l - len(morfUriBase))
      b[len(morfUriBase)+iCh] = byte(randInt(256))
      req.URL.Path = string(b)
   }
}

//
// morf a request header
// change one character in one value of the headers
//
func morfHeader(req *http.Request) {
   // get a header number to morf
   iHdr := randInt(len(req.Header))

   // go over the headers
   for key, vals := range req.Header {
      // we count down til we get to the required header
      if iHdr == 0 {
         // get a value to update
         iVals := randInt(len(vals))
         for iv := range vals {
            val := vals[iv]
            if iv == iVals {
               iVal := randInt(len(val))
               b := []byte(val)
               b[iVal] = byte(randInt(256))
               val = string(b)
            }

            // update / add the new value
            if iv == 0 {
               req.Header.Set(key, val) // set removes all the other values
            } else {
               req.Header.Add(key, val)
            }
         }
         return
      }
      iHdr--
   }
}

//
// prepare response with http error + error information
//
func ResponseHttpError(respw http.ResponseWriter, httpStatus int, message string) {
   http.Error(respw, http.StatusText(httpStatus)+message, httpStatus)
}

//
// response handler; update the response before it is sent to the client
//
func (reqMgr *RequestManager) respHandler(resp *http.Response) error {

   if (resp.StatusCode / 100) == 4 {
      // log.Printf("%+v; %+v", resp.Request, resp)
   }

   return nil
}

//
// queue the request to forward to the staging server
//
func (reqMgr *RequestManager) forwardHandler(req *http.Request, respHdr http.Header, stagBody io.ReadCloser) {

   // do we have a staging server
   if reqMgr.UrlStaging.Scheme == "" ||
      reqMgr.UrlStaging.Host == "" {
      return
   }

   cookies := respHdr["Set-Cookie"]
   updateSessionKey, updateKeyExpires := getRespSessionKey(cookies)
   prodSessionKey, _ := getSessionKey(req.Cookies())

   // prepare a request to queue
   sendReq := new(PendingRequest)
   sendReq.req = req
   sendReq.body = stagBody
   sendReq.requestKey = prodSessionKey
   sendReq.sessionKey = updateSessionKey
   sendReq.keyExpires = updateKeyExpires

   // forward to staging
   go reqMgr.sendStaging(sendReq)
}

//
// get session key from response
//
func getRespSessionKey(cookies []string) (string, int64) {
   for ic := range cookies {
      cookie := cookies[ic]
      if len(cookie) >= 11 && strings.EqualFold(cookie[:11], "sessionKey=") {
         tokens := strings.Split(cookie, ";")
         prodSessionKey := tokens[0][11:]
         for it := 1; it < len(tokens); it++ {
            token := strings.TrimSpace(tokens[it])
            if len(token) >= 8 && strings.EqualFold(token[:8], "Expires=") {
               timeExpires, err := time.Parse(time.RFC1123, token[8:])
               if err == nil {
                  prodKeyExpires := UnixMs(timeExpires)
                  return prodSessionKey, prodKeyExpires
               }
            }
         }
         return prodSessionKey, 0
      }
   }
   return "", 0
}

//
// get request sessionKey id
//
func getSessionKey(cookies []*http.Cookie) (string, int64) {
   // get the sessionKey cookie
   for ic := range cookies {
      cookie := cookies[ic]
      if strings.EqualFold(cookie.Name, "sessionKey") {
         return cookie.Value, UnixMs(cookie.Expires)
      }
   }

   return "", 0
}

//
// push the new request to the PendingRequests channel (queue)
// - typicaly this function is called asynchronously
//
func (reqMgr *RequestManager) sendStaging(sendReq *PendingRequest) {

   // handle full queue
   if len(reqMgr.PendingRequests) < 100 {
      reqMgr.pingManager.Set(false)

      // remove the oldest request, and add the new one
      delReq := <-reqMgr.PendingRequests

      // log the removed URI path (limit to 80 chars)
      l := len(delReq.req.URL.Path)
      if l > 80 {
         l = 80
      }
      log.Printf("error: pending requests overflow! removing: %+v", delReq.req.URL.Path[:l])
   } else {
      reqMgr.pingManager.Set(true)
   }

   reqMgr.PendingRequests <- sendReq
}

//
// this function handles the PendingRequests channel (queue)
// and delivers the request in the same order they are queued
// - this function runs asynchronously
//
func (reqMgr *RequestManager) StagingHandler() {
   for true {
      sendReq := <-reqMgr.PendingRequests

      reqSend := reqMgr.buildForwardRequest(sendReq.req, sendReq.requestKey, sendReq.body)

      go reqMgr.sendRequest(reqSend, sendReq)
   }
}

//
// send the request
//
func (reqMgr *RequestManager) sendRequest(reqSend *http.Request, sendReq *PendingRequest) {
   resp, err := reqMgr.DestStaging.Do(reqSend)
   if err != nil {
      log.Printf("error sending message to staging: %+v", err)
   } else {
      reqMgr.cacheResponse(sendReq.sessionKey, resp, sendReq.keyExpires)

      // log the response
      buf := new(bytes.Buffer)
      buf.ReadFrom(resp.Body)
      newStr := buf.String()
      var isGraphic bool = true
      lng := len(newStr)
      if lng > 70 {
         lng = 70
         newStr = newStr[:70]
      }
      runes := []rune(newStr)
      for i := 0; i < lng; i++ {
         if !unicode.IsGraphic(runes[i]) {
            isGraphic = false
            break
         }
      }
      if !isGraphic {
         newStr = base64.StdEncoding.EncodeToString([]byte(newStr))
      }
      mx := len(reqSend.URL.Path)
      if mx > 70 {
         mx = 70
      }
      log.Printf("%v: %+v", reqSend.URL.Path[:mx], newStr)

      // cleanup
      resp.Body.Close()
   }
}

//
// build the forward request
//
func (reqMgr *RequestManager) buildForwardRequest(req *http.Request, prodSessionKey string, stagBody io.ReadCloser) *http.Request {
   // prepare request for staging
   stagReq, err := http.NewRequest(req.Method, req.URL.Path, stagBody)

   if err != nil {
      log.Print("error creating new request: ", err)
      return nil
   } else {
      stagReq.URL = reqMgr.UrlStaging
      stagReq.URL.Path = req.URL.Path
      stagReq.Host = reqMgr.UrlStaging.Host

      // copy headers from production request to staging
      StagKeys := reqMgr.CacheData[prodSessionKey]
      for key, vals := range req.Header {
         if !strings.EqualFold(key, httpForwardedHeader) {
            for i := range vals {
               val := vals[i]

               // replace the csrf-token with staging
               if StagKeys != nil {
                  if strings.EqualFold(key, "X-Csrf-Token") && StagKeys.csrfToken != "" {
                     val = StagKeys.csrfToken
                  }
               }

               // add headers except cookies; these are handled in separately
               if !strings.EqualFold(key, "Cookie") {
                  stagReq.Header.Add(key, val)
               }
            }
         }
      }

      // fix cookies; replace production values with staging
      if StagKeys != nil {
         for _, cc := range req.Cookies() {
            if strings.EqualFold(cc.Name, "csrfToken") {
               cc.Value = StagKeys.csrfToken
            } else if strings.EqualFold(cc.Name, "sessionKey") {
               cc.Value = StagKeys.sessionKey
            } else if strings.EqualFold(cc.Name, "sessionTtl") {
               cc.Value = StagKeys.sessionTtl
            }
            // if we have a cookie value add it to the request
            if cc.Value != "" {
               stagReq.AddCookie(cc)
            }
         }
      }
   }

   stagReq.Header.Add(httpDuplicateHeader, httpNameHeader)

   return stagReq
}
