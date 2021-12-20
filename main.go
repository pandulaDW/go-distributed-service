package main

import (
	"fmt"
	"github.com/pandulaDW/go-distributed-service/server"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	httpServer := server.NewHTTPServer(":8080")
	isServerRunning := make(chan bool)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	go func() {
		<-sig
		err := httpServer.Close()
		if err != nil {
			fmt.Println("error closing the server: ", err)
		}
	}()

	go func() {
		defer func() {
			isServerRunning <- false
		}()
		fmt.Println("server listening to requests at port 8080...")
		err := httpServer.ListenAndServe()
		if err != http.ErrServerClosed && err != nil {
			fmt.Println("server unexpectedly closed: ", err)
		} else {
			fmt.Println("closed the server successfully...")
		}
	}()

	<-isServerRunning
	fmt.Println("exiting the process...")
	os.Exit(1)
}
