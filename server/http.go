package server

import (
	"github.com/gorilla/mux"
	"net/http"
)

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func NewHTTPServer(addr string) *http.Server {
	httpSrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpSrv.handleProduce)
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}
