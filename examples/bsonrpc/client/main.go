// Copyright 2023 René Jochum. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"log"
	"net"

	"github.com/jochumdev/go-bidirpc/encoding/bson"
	"github.com/jochumdev/go-bidirpc/protocol"
)

type EchoArgs struct {
	EchoMe string `bson:"echoMe"`
}

type EchoReply struct {
	Result string
}

type Arith struct {
	p     *protocol.Protocol
}

func (t *Arith) SetProtocol(p *protocol.Protocol) {
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

	cc := bson.NewCodec(conn)
	client := protocol.NewClientWithCodec(cc)

	client.Register(new(Arith))
	defer client.Close()

	var reply EchoReply
	err = client.Call("Arith.Echo", &EchoArgs{EchoMe: "Hello world!"}, &reply)

	if err != nil {
		log.Fatal("error:", err)
	}

	fmt.Printf("Reply is: %s\n", reply.Result)
}
