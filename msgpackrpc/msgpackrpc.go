// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package msgpackrpc

import (
	"bufio"
	"github.com/pcdummy/go-bidirpc"
	"github.com/ugorji/go/codec"
	"io"
)

func NewClient(conn io.ReadWriteCloser) (c *bidirpc.Protocol) {
	cc := NewCodec(conn)
	c = bidirpc.NewClientWithCodec(cc)
	return
}

func NewCodec(conn io.ReadWriteCloser) (cc bidirpc.Codec) {
	h := &codec.MsgpackHandle{}
	wBuf := bufio.NewWriter(conn)
	cc = &rpcCodec{
		conn: conn,
		dec:  codec.NewDecoder(bufio.NewReader(conn), h),
		enc:  codec.NewEncoder(wBuf, h),
		wBuf: wBuf,
	}
	return
}

type rpcCodec struct {
	conn io.ReadWriteCloser
	dec  *codec.Decoder
	enc  *codec.Encoder
	wBuf *bufio.Writer
}

func (c *rpcCodec) Write(rs *bidirpc.RepReq, v interface{}) (err error) {
	if err = c.enc.Encode(rs); err != nil {
		return
	}

	if err = c.enc.Encode(v); err != nil {
		return
	}

	return c.wBuf.Flush()
}

func (c *rpcCodec) ReadHeader(res *bidirpc.RepReq) (err error) {
	return c.dec.Decode(res)
}

func (c *rpcCodec) ReadBody(v interface{}) (err error) {
	return c.dec.Decode(v)
}

func (c *rpcCodec) Close() (err error) {
	return c.conn.Close()
}
