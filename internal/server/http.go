package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(addr string) *http.Server {
	httpserver := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpserver.handleProduce).Methods("POST")
	r.HandleFunc("/", httpserver.handleConsume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
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

func (httpserver *httpServer) handleConsume(response http.ResponseWriter, request *http.Request) {
	var req ConsumeRequest

	err := json.NewDecoder(request.Body).Decode(&req)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := httpserver.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(response, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(response).Encode(res)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (httpserver *httpServer) handleProduce(response http.ResponseWriter, request *http.Request) {
	var req ProduceRequest

	err := json.NewDecoder(request.Body).Decode(&req)

	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	off, err := httpserver.Log.Append(req.Record)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
	}

	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(response).Encode(res)
	if err != nil {
		http.Error(response, err.Error(), http.StatusInternalServerError)
		return
	}
}
