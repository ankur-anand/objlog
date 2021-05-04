package main

import (
	"log"

	"github.com/ankur-anand/melee/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
