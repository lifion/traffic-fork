package ping

import (
   "encoding/json"
   "net/http"
)

//
// ping data; this data is sent back to the client
type Manger struct {
   ServiceName string
   StatusOk    bool
}

//
// initialize the ping handler
func (pm *Manger) Init() {

   http.HandleFunc("/ping", pm.handler)
}

//
// handle "/ping" path; return name and status
func (pm *Manger) handler(w http.ResponseWriter, r *http.Request) {

   buf, _ := json.Marshal(pm)
   w.Write(buf)
}

// set ping status
func (pm *Manger) Get() bool {
   return pm.StatusOk
}

// get ping status
func (pm *Manger) Set(ok bool) {
   pm.StatusOk = ok
}
