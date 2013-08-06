// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package srpc

import (
	"errors"
	"fmt"
	"log"
	"sync"
)

// Contains per Session data / locks.
// ServeCodec creates one instance per Session / Connection.
type Session struct {
	mutex    sync.Mutex // protects pending, seq, request
	sending  sync.Mutex
	request  Request
	codec    Codec
	seq      uint64
	pending  map[uint64]*Call
	closing  bool
	shutdown bool
}

func (s *Session) Close() error {
	s.mutex.Lock()
	if s.shutdown || s.closing {
		s.mutex.Unlock()
		return ErrShutdown
	}
	s.closing = true
	s.mutex.Unlock()
	return s.codec.Close()
}

// Go invokes the function asynchronously.  It returns the Call structure representing
// the invocation.  The done channel will signal when the call is complete by returning
// the same Call object.  If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
func (s *Session) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel.  If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	s.sendRequest(call)
	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (s *Session) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-s.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

func (s *Session) sendRequest(call *Call) {
	s.sending.Lock()
	defer s.sending.Unlock()

	// Register this call.
	s.mutex.Lock()
	if s.shutdown || s.closing {
		call.Error = ErrShutdown
		s.mutex.Unlock()
		call.done()
		return
	}
	seq := s.seq
	s.seq++
	s.pending[seq] = call
	s.mutex.Unlock()

	// Encode and send the request.
	s.request.Seq = seq
	s.request.ServiceMethod = call.ServiceMethod
	err := s.codec.WriteRequest(&s.request, call.Args)
	if err != nil {
		s.mutex.Lock()
		call = s.pending[seq]
		delete(s.pending, seq)
		s.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (s *Session) handleResponse(err error, repReq *RepReq) {
	seq := repReq.Seq
	s.mutex.Lock()
	call := s.pending[seq]
	delete(s.pending, seq)
	s.mutex.Unlock()

	switch {
	case call == nil:
		// We've got no pending call. That usually means that
		// WriteRequest partially failed, and call was already
		// removed; response is a server telling us about an
		// error reading request body.
		if err != nil {
			err = errors.New("Unknown response id: " + string(seq))
		}
	case repReq.Error != "":
		// We've got an error response. Give this to the request;
		// any subsequent requests will get the ReadResponseBody
		// error if there is one.
		call.Error = ServerError(repReq.Error)
		call.done()
	default:
		err = s.codec.ReadBody(call.Reply)
		if err != nil {
			call.Error = errors.New(fmt.Sprintf("Invalid response value: %v", call.Reply))
			call.done()
			err = nil
			return
		}

		call.done()
	}
}
