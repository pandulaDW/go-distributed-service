package main

import (
	"fmt"
	"go-microservices/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	blocker := make(chan struct{})

	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("server started...")
	<-blocker
}
