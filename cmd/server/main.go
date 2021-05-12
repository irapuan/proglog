package main

import (
	"log"

	"github.com/irapuan/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8090")
	log.Fatal(srv.ListenAndServe())
}
