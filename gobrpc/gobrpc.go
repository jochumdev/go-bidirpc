// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gobrpc

import (
	"bufio"
	"encoding/gob"
	"github.com/pcdummy/go-bidirpc"
	"io"
)

func NewClient(conn io.ReadWriteCloser) (c *bidirpc.Protocol) {
	cc := NewCodec(conn)
	c = bidirpc.NewClientWithCodec(cc)
	return
}

func NewCodec(conn io.ReadWriteCloser) (cc bidirpc.Codec) {
	wBuf := bufio.NewWriter(conn)
	cc = &codec{
		conn: conn,
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(wBuf),
		wBuf: wBuf,
	}
	return
}

type codec struct {
	conn io.ReadWriteCloser
	dec  *gob.Decoder
	enc  *gob.Encoder
	wBuf *bufio.Writer
}

func (c *codec) Write(rs *bidirpc.RepReq, v interface{}) (err error) {
	if err = c.enc.Encode(rs); err != nil {
		return
	}

	if err = c.enc.Encode(v); err != nil {
		return
	}

	return c.wBuf.Flush()
}

func (c *codec) ReadHeader(res *bidirpc.RepReq) (err error) {
	return c.dec.Decode(res)
}

func (c *codec) ReadBody(v interface{}) (err error) {
	return c.dec.Decode(v)
}

func (c *codec) Close() (err error) {
	return c.conn.Close()
}
