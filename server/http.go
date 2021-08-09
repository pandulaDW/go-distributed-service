package server

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

type httpServer struct {
	log *Log
}

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

func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	reqBody := new(ProduceRequest)
	err := json.NewDecoder(r.Body).Decode(reqBody)
	if err != nil {
		http.Error(w, "error in parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Printf("received produce request for value: %s", reqBody.Record.Value)

	offset, err := s.log.Append(reqBody.Record)
	if err != nil {
		http.Error(w, "error in appending log: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	response := &ProduceResponse{Offset: offset}
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, "something went wrong: "+err.Error(), http.StatusInternalServerError)
	}
}

func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	reqBody := new(ConsumeRequest)
	err := json.NewDecoder(r.Body).Decode(reqBody)
	if err != nil {
		http.Error(w, "error in parsing request: "+err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Printf("received consume request to extract log record at: %d\n", reqBody.Offset)

	record, err := s.log.Read(reqBody.Offset)
	if err != nil {
		http.Error(w, "error in reading log: "+err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := &ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		http.Error(w, "something went wrong: "+err.Error(), http.StatusInternalServerError)
	}
}

func newHttpServer() *httpServer {
	return &httpServer{log: NewLog()}
}

func NewHTTPServer(addr string) *http.Server {
	httpSrv := newHttpServer()

	r := mux.NewRouter()
	r.HandleFunc("/", httpSrv.handleProduce).Methods(http.MethodPost)
	r.HandleFunc("/", httpSrv.handleConsume).Methods(http.MethodGet)

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}
