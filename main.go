package main

import (
	"fmt"
	api "github.com/pandulaDW/go-distributed-service/api/v1"
	"github.com/pandulaDW/go-distributed-service/internal/log"
	"github.com/pandulaDW/go-distributed-service/internal/server"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	listener, _ := net.Listen("tcp", ":50001")

	isServerRunning := make(chan bool)
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)

	l, err := log.NewLog("data", log.Config{})
	if err != nil {
		fmt.Println("error creating the log: ", err)
		os.Exit(1)
	}

	c := &server.Config{CommitLog: l}
	grpcServer, _ := server.NewgrpcServer(c)

	s := grpc.NewServer()
	api.RegisterLogServer(s, grpcServer)

	go func() {
		<-sig
		fmt.Println("Gracefully shutting down the server...")
		s.GracefulStop()
	}()

	go func() {
		defer func() {
			isServerRunning <- false
		}()
		fmt.Println("server listening to requests at port 50001...")
		err = s.Serve(listener)
		if err != nil {
			fmt.Println(err)
		}
	}()

	<-isServerRunning
	fmt.Println("exiting the process...")
	os.Exit(1)
}
