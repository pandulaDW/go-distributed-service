package main

import (
	"fmt"
	"github.com/pandulaDW/go-distributed-service/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	blocker := make(chan struct{})

	go func() {
		log.Fatal(srv.ListenAndServe())
	}()

	fmt.Println("server started...")
	<-blocker
}
