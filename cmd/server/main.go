package main

import (
	"log"

	"github.com/ankur-anand/meleelog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
