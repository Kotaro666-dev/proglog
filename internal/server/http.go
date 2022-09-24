package server

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func NewHTTPServer(address string) *http.Server {
	server := newHTTPServer()
	router := mux.NewRouter()
	router.HandleFunc("/", server.handleProduce).Methods("POST")
	router.HandleFunc("/", server.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    address,
		Handler: router,
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

func (server *httpServer) handleProduce(writer http.ResponseWriter, request *http.Request) {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(request.Body)

	var produceRequest ProduceRequest
	err := json.NewDecoder(request.Body).Decode(&produceRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := server.Log.Append(produceRequest.Record)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	ProduceResponse := ProduceResponse{Offset: offset}
	err = json.NewEncoder(writer).Encode(ProduceResponse)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (server *httpServer) handleConsume(writer http.ResponseWriter, request *http.Request) {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(request.Body)

	var consumeRequest ConsumeRequest
	err := json.NewDecoder(request.Body).Decode(&consumeRequest)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := server.Log.Read(consumeRequest.Offset)
	if err == ErrorOffsetNotFound {
		http.Error(writer, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	consumeResponse := ConsumeResponse{Record: record}
	err = json.NewEncoder(writer).Encode(consumeResponse)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}
