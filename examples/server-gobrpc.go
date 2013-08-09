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

type Arith struct {
	count int
	p     *bidirpc.Protocol
}

type EchoArgs struct {
	EchoMe string `bson:"echoMe"`
}

type EchoReply struct {
	Result string
}

// The protocol excutes this method when it finds it.
// this allows the Server to do t.p.Call() / t.p.Go() on the client.
func (t *Arith) SetProtocol(p *bidirpc.Protocol) {
	t.p = p
}

func (t *Arith) Echo(args *EchoArgs, reply *EchoReply) (err error) {
	fmt.Println("Echo from client:", args.EchoMe)

	// Run a call on the client
	var nreply EchoReply
	err = t.p.Call("Arith.Echo", &EchoArgs{EchoMe: "Hey client how are you?"}, &nreply)

	reply.Result = args.EchoMe
	return err
}

func main() {
	l, err := net.Listen("tcp", ":9990")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}

		// Every connection needs a codec, a new Protocol
		// and the methods needs to be Registered at every session.
		// The methods need to be goroutine save and they
		// are unique per session.
		c := gobrpc.NewCodec(conn)
		s := bidirpc.NewProtocol(c)
		s.Register(new(Arith))
		go s.Serve()
	}
}
