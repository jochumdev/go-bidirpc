package bsonrpc

import (
	"encoding/binary"
	"errors"
	"github.com/pcdummy/gosrpc"
	"io"
	"labix.org/v2/mgo/bson"
)

func NewClient(conn io.ReadWriteCloser) (c *srpc.Protokoll) {
	cc := NewServerCodec(conn)
	c = srpc.NewClientWithCodec(cc)
	return
}

func NewClientCodec(conn io.ReadWriteCloser) (cc srpc.ClientCodec) {
	cc = &codec{conn: conn}
	return
}

func NewServerCodec(conn io.ReadWriteCloser) (sc srpc.ServerCodec) {
	sc = &codec{conn: conn}
	return
}

type bSONRepReq struct {
	T string      `bson:"T"` // Type
	M string      `bson:"M"` // Method
	V interface{} `bson:"V"` // Value
	I uint64      `bson:"I"` // ID
	S int         `bson:"S"` // Status
}

type bSONIncoming struct {
	T string   `bson:"T"` // Type
	M string   `bson:"M"` // Method
	V bson.Raw `bson:"V"` // Value
	I uint64   `bson:"I"` // ID
	S int      `bson:"S"` // Status
}

type codec struct {
	conn io.ReadWriteCloser
	body *bson.Raw
}

func (c *codec) WriteResponse(rs *srpc.Response, v interface{}) (err error) {
	br := new(bSONRepReq)
	br.T = "rep"
	br.M = rs.ServiceMethod
	br.I = rs.Seq
	if rs.Error != "" {
		br.V = rs.Error
		br.S = -1
	} else {
		br.V = v
		br.S = 0
	}

	c.encode(br)

	return
}

func (c *codec) WriteRequest(req *srpc.Request, v interface{}) (err error) {
	br := new(bSONRepReq)
	br.T = "req"
	br.M = req.ServiceMethod
	br.V = v
	br.I = req.Seq
	br.S = 0

	err = c.encode(br)
	return
}

func (c *codec) ReadHeader(res *srpc.RepReq) (err error) {
	r := bSONIncoming{}
	err = c.decode(&r)
	if err != nil {
		return
	}

	switch r.T {
	case "req":
		res.Type = srpc.REQUEST
		res.ServiceMethod = r.M
		res.Seq = r.I

		c.body = &r.V
		break
	case "rep":
		res.Type = srpc.RESPONSE
		res.Seq = r.I
		if r.S != 0 {
			// This is an error.
			r.V.Unmarshal(&res.Error)
			return errors.New(res.Error)
		}

		c.body = &r.V
		break
	}

	return
}

func (c *codec) ReadBody(v interface{}) (err error) {
	err = c.body.Unmarshal(v)
	c.body = nil
	return
}

func (c *codec) Close() (err error) {
	err = c.conn.Close()
	return
}

func (c *codec) encode(v interface{}) (err error) {
	buf, err := bson.Marshal(v)
	if err != nil {
		return
	}

	// Write message size.
	var slen uint32 = uint32(len(buf))
	err = binary.Write(c.conn, binary.BigEndian, slen)
	if err != nil {
		return
	}

	// Write the message.
	_, err = c.conn.Write(buf)

	return
}

func (c *codec) decode(pv interface{}) (err error) {
	// Read message size
	var length uint32
	err = binary.Read(c.conn, binary.BigEndian, &length)
	if err != nil {
		return
	}

	// Create the buffer for BSON and read the message.
	buf := make([]byte, length)
	_, err = io.ReadFull(c.conn, buf[:])
	if err != nil {
		return
	}

	err = bson.Unmarshal(buf, pv)
	return
}
