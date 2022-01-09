package main

import (
	"fmt"
	"github.com/pandulaDW/go-distributed-service/internal/log"
	"github.com/pandulaDW/go-distributed-service/internal/server"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	listener, _ := net.Listen("tcp", ":50051")
	isServerClosed := make(chan bool)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	lConfig := log.Config{}
	lConfig.Segment.MaxStoreBytes = 90000
	lConfig.Segment.MaxIndexBytes = 9000

	l, err := log.NewLog("data", lConfig)
	if err != nil {
		fmt.Println("error creating the log: ", err)
		os.Exit(1)
	}

	c := &server.Config{CommitLog: l}
	s, _ := server.NewGRPCServer(c)

	go func() {
		<-sig

		fmt.Println("Gracefully shutting down the server...")
		s.GracefulStop()
		_ = listener.Close()

		fmt.Println("Closing the Log...")
		err = l.Close()
		if err != nil {
			fmt.Println("error closing the log: ", err)
		}

		isServerClosed <- true
	}()

	go func() {
		fmt.Println("server listening to requests at port 50051...")
		err = s.Serve(listener)
		if err != nil {
			fmt.Println(err)
		}
	}()

	<-isServerClosed
	fmt.Println("exiting the process...")
	os.Exit(1)
}
