// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gobrpc

import (
	"bufio"
	"encoding/gob"
	"github.com/pcdummy/bidirpc"
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

	err = c.wBuf.Flush()

	return
}

func (c *codec) ReadHeader(res *bidirpc.RepReq) (err error) {
	err = c.dec.Decode(res)
	return
}

func (c *codec) ReadBody(v interface{}) (err error) {
	err = c.dec.Decode(v)
	return
}

func (c *codec) Close() (err error) {
	err = c.conn.Close()
	return
}
