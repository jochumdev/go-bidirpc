// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bsonrpc

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/pcdummy/go-bidirpc"
	"io"
	"labix.org/v2/mgo/bson"
)

func NewClient(conn io.ReadWriteCloser) (c *bidirpc.Protocol) {
	cc := NewCodec(conn)
	c = bidirpc.NewClientWithCodec(cc)
	return
}

func NewCodec(conn io.ReadWriteCloser) (cc bidirpc.Codec) {
	rBuf := bufio.NewReader(conn)
	wBuf := bufio.NewWriter(conn)
	cc = &codec{
		conn: conn,
		r:    rBuf,
		w:    wBuf,
		wBuf: wBuf,
	}
	return
}

type codec struct {
	conn io.ReadWriteCloser
	r    io.Reader
	w    io.Writer
	wBuf *bufio.Writer
}

func (c *codec) Write(rs *bidirpc.RepReq, v interface{}) (err error) {
	if err = c.encode(rs); err != nil {
		return
	}

	if err = c.encode(v); err != nil {
		return
	}

	return c.wBuf.Flush()
}

func (c *codec) ReadHeader(res *bidirpc.RepReq) (err error) {
	return c.decode(res)
}

func (c *codec) ReadBody(v interface{}) (err error) {
	return c.decode(v)
}

func (c *codec) Close() (err error) {
	return c.conn.Close()
}

func (c *codec) encode(v interface{}) (err error) {
	buf, err := bson.Marshal(v)
	if err != nil {
		return
	}

	// Write the message.
	_, err = c.w.Write(buf)

	return
}

func (c *codec) decode(pv interface{}) (err error) {
	var lbuf [4]byte
	n, err := c.r.Read(lbuf[:])
	if n == 0 {
		err = io.EOF
		return
	}
	if n != 4 {
		err = errors.New(fmt.Sprintf("Corrupted BSON stream: could only read %d", n))
		return
	}
	if err != nil {
		return
	}

	length := (int(lbuf[0]) << 0) |
		(int(lbuf[1]) << 8) |
		(int(lbuf[2]) << 16) |
		(int(lbuf[3]) << 24)

	buf := make([]byte, length)
	copy(buf[0:4], lbuf[:])
	_, err = io.ReadFull(c.r, buf[4:])
	if err != nil {
		return
	}

	return bson.Unmarshal(buf, pv)
}
