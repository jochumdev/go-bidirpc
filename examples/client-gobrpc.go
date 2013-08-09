// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"github.com/pcdummy/go-bidirpc"
	"github.com/pcdummy/go-bidirpc/gobrpc"
	"log"
	"net"
)

type EchoArgs struct {
	EchoMe string `bson:"echoMe"`
}

type EchoReply struct {
	Result string
}

type Arith struct {
	count int
	p     *bidirpc.Protocol
}

func (t *Arith) SetProtocol(p *bidirpc.Protocol) {
	t.p = p
}

func (t *Arith) Echo(args *EchoArgs, reply *EchoReply) error {
	fmt.Println("Echo from Server:", args.EchoMe)

	reply.Result = args.EchoMe
	return nil
}

func main() {
	conn, err := net.Dial("tcp", "localhost:9990")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	client := gobrpc.NewClient(conn)
	client.Register(new(Arith))
	defer client.Close()

	var reply EchoReply
	err = client.Call("Arith.Echo", &EchoArgs{EchoMe: "Hello world!"}, &reply)

	if err != nil {
		log.Fatal("error:", err)
	}

	fmt.Printf("Reply is: %s\n", reply.Result)
}
