// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bidirpc

import (
	"github.com/pcdummy/go-bidirpc"
	"github.com/pcdummy/go-bidirpc/bsonrpc"
	"github.com/pcdummy/go-bidirpc/gobrpc"
	"github.com/pcdummy/go-bidirpc/msgpackrpc"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var (
	serverBSON, serverGob, serverMsgPack string
	onceBSON, onceGob, onceMsgPack       sync.Once
)

type Args struct {
	A, B int
}

type Reply struct {
	C int
}

type Arith struct {
}

func (t *Arith) Add(args Args, reply *Reply) error {
	reply.C = args.A + args.B
	return nil
}

func listenTCP() (net.Listener, string) {
	l, e := net.Listen("tcp", "127.0.0.1:0") // any available address
	if e != nil {
		log.Fatalf("net.Listen tcp :0: %v", e)
	}
	return l, l.Addr().String()
}

func startBSONServer() {
	var l net.Listener
	l, serverBSON = listenTCP()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
			}

			c := bsonrpc.NewCodec(conn)
			s := bidirpc.NewProtocol(c)
			s.Register(new(Arith))
			go s.Serve()
		}
	}()
}

func startGobServer() {
	var l net.Listener
	l, serverGob = listenTCP()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
			}

			c := gobrpc.NewCodec(conn)
			s := bidirpc.NewProtocol(c)
			s.Register(new(Arith))
			go s.Serve()
		}
	}()
}

func startMsgPackServer() {
	var l net.Listener
	l, serverMsgPack = listenTCP()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Fatal("accept error:", err)
			}

			c := msgpackrpc.NewCodec(conn)
			s := bidirpc.NewProtocol(c)
			s.Register(new(Arith))
			go s.Serve()
		}
	}()
}

func TestBSONConnection(t *testing.T) {
	onceBSON.Do(startBSONServer)

	conn, err := net.Dial("tcp", serverBSON)
	if err != nil {
		log.Fatal("error dialing:", err)
	}

	args := &Args{7, 8}
	reply := new(Reply)
	client := bsonrpc.NewClient(conn)
	err = client.Call("Arith.Add", args, reply)
	if err != nil {
		t.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
	}
	client.Close()
}

func TestGobConnection(t *testing.T) {
	onceGob.Do(startGobServer)

	conn, err := net.Dial("tcp", serverGob)
	if err != nil {
		log.Fatal("error dialing:", err)
	}

	args := &Args{7, 8}
	reply := new(Reply)
	client := gobrpc.NewClient(conn)
	err = client.Call("Arith.Add", args, reply)
	if err != nil {
		t.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
	}
	client.Close()
}

func TestMsgPackConnection(t *testing.T) {
	onceMsgPack.Do(startMsgPackServer)

	conn, err := net.Dial("tcp", serverMsgPack)
	if err != nil {
		log.Fatal("error dialing:", err)
	}

	args := &Args{7, 8}
	reply := new(Reply)
	client := msgpackrpc.NewClient(conn)
	err = client.Call("Arith.Add", args, reply)
	if err != nil {
		t.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
	}
	if reply.C != args.A+args.B {
		t.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
	}
	client.Close()
}

func BenchmarkConnections(b *testing.B) {
	b.StopTimer()
	onceBSON.Do(startBSONServer)
	procs := runtime.GOMAXPROCS(-1)
	N := int32(b.N)
	var wg sync.WaitGroup
	wg.Add(procs)
	b.StartTimer()

	for p := 0; p < procs; p++ {
		go func() {
			for atomic.AddInt32(&N, -1) >= 0 {
				conn, err := net.Dial("tcp", serverBSON)
				if err != nil {
					log.Fatal("error dialing:", err)
				}

				args := &Args{7, 8}
				reply := new(Reply)
				client := bsonrpc.NewClient(conn)
				err = client.Call("Arith.Add", args, reply)
				if err != nil {
					b.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
				}
				if reply.C != args.A+args.B {
					b.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
				}
				client.Close()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkEndToEnd(b *testing.B) {
	b.StopTimer()
	onceBSON.Do(startBSONServer)

	conn, err := net.Dial("tcp", serverBSON)
	if err != nil {
		log.Fatal("error dialing:", err)
	}

	client := bsonrpc.NewClient(conn)
	defer client.Close()

	// Synchronous calls
	args := &Args{7, 8}
	procs := runtime.GOMAXPROCS(-1)
	N := int32(b.N)
	var wg sync.WaitGroup
	wg.Add(procs)
	b.StartTimer()

	for p := 0; p < procs; p++ {
		go func() {
			reply := new(Reply)
			for atomic.AddInt32(&N, -1) >= 0 {
				err := client.Call("Arith.Add", args, reply)
				if err != nil {
					b.Fatalf("rpc error: Add: expected no error but got string %q", err.Error())
				}
				if reply.C != args.A+args.B {
					b.Fatalf("rpc error: Add: expected %d got %d", reply.C, args.A+args.B)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
