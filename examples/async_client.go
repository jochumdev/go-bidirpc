package main

import (
	"fmt"
	"github.com/pcdummy/gosrpc"
	"github.com/pcdummy/gosrpc/bsonrpc"
	"log"
	"net"
	"sync"
)

type EchoArgs struct {
	EchoMe string `bson:"echoMe"`
}

type BounceArgs struct {
	Args []string `bson:"args"`
}

type MultiplyArgs struct {
	A, B int
}

func call(client *srpc.Protokoll, wg *sync.WaitGroup) {
	// Asynchronous call
	//var reply string
	var intReply int
	divCall := client.Go("Arith.Multiply", &MultiplyArgs{A: 9, B: 3}, &intReply, make(chan *srpc.Call, 1))
	//divCall := client.Go("echo", &EchoArgs{EchoMe: "Hello world!"}, &reply, make(chan *srpc.Call, 1))
	// divCall := client.Go("bounce", &BounceArgs{Args: []string{"echo", "test"}}, &reply, make(chan *srpc.Call, 1))

	// Waiting for the answer here.
	<-divCall.Done

	if divCall.Error != nil {
		log.Fatal("error:", divCall.Error)
	}

	fmt.Printf("Reply is: %d\n", intReply)

	wg.Done()
}

func main() {
	conn, err := net.Dial("tcp", "localhost:9990")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	client := bsonrpc.NewSClient(conn)
	defer client.Close()

	wg := &sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go call(client, wg)
	}

	wg.Wait()

}
