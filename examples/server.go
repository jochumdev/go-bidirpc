// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"github.com/pcdummy/gosrpc"
	"github.com/pcdummy/gosrpc/bsonrpc"
	"log"
	"net"
	"time"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B

	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B

	time.Sleep(time.Duration(100-args.A) * time.Second)

	return nil
}

func main() {
	arith := new(Arith)
	srpc.Register(arith)

	listener, err := net.Listen("tcp", ":9990")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("accept error:", err)
		}
		rpcCodec := bsonrpc.NewCodec(conn)
		go srpc.ServeCodec(rpcCodec)
	}
}
