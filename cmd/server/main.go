package main

import (
	"github/Kotaro666-dev/prolog/internal/server"
	"log"
)

const PORT = ":8080"

func main() {
	httpServer := server.NewHTTPServer(PORT)
	log.Fatal(httpServer.ListenAndServe())
}
