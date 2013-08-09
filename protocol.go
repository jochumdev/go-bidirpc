// Copyright 2013 René Kistl. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bidirpc

import (
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

// ServerError represents an error that has been returned from
// the remote side of the RPC connection.
type ServerError string

func (e ServerError) Error() string {
	return string(e)
}

var ErrShutdown = errors.New("connection is shut down")

// Precompute the reflect type for error.  Can't use error directly
// because Typeof takes an empty interface value.  This is annoying.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

type methodType struct {
	method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

type service struct {
	name   string                 // name of service
	rcvr   reflect.Value          // receiver of methods for the service
	typ    reflect.Type           // type of the receiver
	method map[string]*methodType // registered methods
}

const (
	REQUEST  = 1
	RESPONSE = 2
)

type RepReq struct {
	Type          int `bson:",minsize"` // Either REQUEST OR RESPONSE
	ServiceMethod string
	Seq           uint64
	Error         string `bson:",omitempty"`
	next          *RepReq
}

// Call represents an active RPC.
type Call struct {
	ServiceMethod string      // The name of the service and method to call.
	Args          interface{} // The argument to the function (*struct).
	Reply         interface{} // The reply from the function (*struct).
	Error         error       // After completion, the error status.
	Done          chan *Call  // Strobes when call is complete.
}

func (call *Call) done() {
	select {
	case call.Done <- call:
		// ok
	default:
		// We don't want to block here.  It is the caller's responsibility to make
		// sure the channel has enough buffer space. See comment in Go().
		log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

// Protocol represents an RPC Protocol.
type Protocol struct {
	mu         sync.RWMutex // protects the serviceMap
	serviceMap map[string]*service
	reqLock    sync.Mutex // protects freeReq
	freeReq    *RepReq

	mutex    sync.Mutex // protects pending, seq, request
	sending  sync.Mutex
	request  RepReq
	codec    Codec
	seq      uint64
	pending  map[uint64]*Call
	closing  bool
	shutdown bool
}

// NewClientWithCodec uses the specified
// codec to encode requests and decode responses.
func NewClientWithCodec(codec Codec) *Protocol {
	s := NewProtocol(codec)
	go s.Serve()
	return s
}

// NewServer returns a new Server.
func NewProtocol(codec Codec) *Protocol {
	return &Protocol{
		codec:      codec,
		serviceMap: make(map[string]*service),
		pending:    make(map[uint64]*Call),
	}
}

// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method
//	- two arguments, both pointers to exported structs
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no methods or unsuitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (p *Protocol) Register(rcvr interface{}) error {
	return p.register(rcvr, "", false)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (p *Protocol) RegisterName(name string, rcvr interface{}) error {
	return p.register(rcvr, name, true)
}

func (p *Protocol) register(rcvr interface{}, name string, useName bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.serviceMap == nil {
		p.serviceMap = make(map[string]*service)
	}

	s := new(service)
	s.typ = reflect.TypeOf(rcvr)
	s.rcvr = reflect.ValueOf(rcvr)

	// Check if the given object implements "SetProtocol" and
	// set the Protocol if possible.
	if tmp, ok := rcvr.(interface {
		SetProtocol(*Protocol)
	}); ok {
		tmp.SetProtocol(p)
	}

	sname := reflect.Indirect(s.rcvr).Type().Name()
	if useName {
		sname = name
	}
	if sname == "" {
		s := "rpc.Register: no service name for type " + s.typ.String()
		log.Print(s)
		return errors.New(s)
	}
	if !isExported(sname) && !useName {
		s := "rpc.Register: type " + sname + " is not exported"
		log.Print(s)
		return errors.New(s)
	}
	if _, present := p.serviceMap[sname]; present {
		return errors.New("rpc: service already defined: " + sname)
	}
	s.name = sname

	// Install the methods
	s.method = suitableMethods(s.typ, true)

	if len(s.method) == 0 {
		str := ""
		// To help the user, see if a pointer receiver would work.
		method := suitableMethods(reflect.PtrTo(s.typ), false)
		if len(method) != 0 {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type (hint: pass a pointer to value of that type)"
		} else {
			str = "rpc.Register: type " + sname + " has no exported methods of suitable type"
		}
		log.Print(str)
		return errors.New(str)
	}
	p.serviceMap[s.name] = s

	return nil
}

// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if reportErr is true.
func suitableMethods(typ reflect.Type, reportErr bool) map[string]*methodType {
	methods := make(map[string]*methodType)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		if mname == "SetProtocol" {
			continue
		}
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			if reportErr {
				log.Println("method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println(mname, "argument type not exported:", argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Println("method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Println("method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Println("method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Println("method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}

// A value sent as a placeholder for the server's response value when the server
// receives an invalid request. It is never decoded by the client since the Response
// contains an error when it is used.
var invalidRequest = struct{}{}

func (s *service) call(server *Protocol, mtype *methodType, req *RepReq, argv, replyv reflect.Value) {
	function := mtype.method.Func
	// Invoke the method, providing a new value for the reply.
	returnValues := function.Call([]reflect.Value{s.rcvr, argv, replyv})
	// The return value for the method is an error.
	errInter := returnValues[0].Interface()
	errmsg := ""
	if errInter != nil {
		errmsg = errInter.(error).Error()
	}
	server.sendResponse(req, replyv.Interface(), errmsg)
	server.freeRepReq(req)
}

func (p *Protocol) Serve() {
	var (
		err         error
		repReq      *RepReq
		service     *service
		mtype       *methodType
		argv        reflect.Value
		replyv      reflect.Value
		dot         int
		serviceName string
		methodName  string
		argIsValue  bool
		seq         uint64
		call        *Call
	)

ENDFOR:
	for !p.closing {
		// Grab the request header.
		repReq = p.getRepReq()
		err = p.codec.ReadHeader(repReq)
		if err != nil {
			repReq = nil
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			err = errors.New("rpc: server cannot decode request: " + err.Error())
			break
		}

		switch repReq.Type {
		case REQUEST:
			dot = strings.LastIndex(repReq.ServiceMethod, ".")
			if dot < 0 {
				err = errors.New("rpc: service/method request ill-formed: " + repReq.ServiceMethod)
				p.sendResponse(repReq, invalidRequest, err.Error())
				p.freeRepReq(repReq)
				break ENDFOR
			}
			serviceName = repReq.ServiceMethod[:dot]
			methodName = repReq.ServiceMethod[dot+1:]

			// Look up the request.
			p.mu.RLock()
			service = p.serviceMap[serviceName]
			p.mu.RUnlock()

			if service == nil {
				err = errors.New("rpc: can't find service " + repReq.ServiceMethod)
				p.sendResponse(repReq, invalidRequest, err.Error())
				p.freeRepReq(repReq)
				break ENDFOR
			}
			mtype = service.method[methodName]
			if mtype == nil {
				err = errors.New("rpc: can't find method " + repReq.ServiceMethod)
				p.sendResponse(repReq, invalidRequest, err.Error())
				p.freeRepReq(repReq)
				break ENDFOR
			}

			// Decode the argument value.
			argIsValue = false // if true, need to indirect before calling.
			if mtype.ArgType.Kind() == reflect.Ptr {
				argv = reflect.New(mtype.ArgType.Elem())
			} else {
				argv = reflect.New(mtype.ArgType)
				argIsValue = true
			}
			// argv guaranteed to be a pointer now.
			if err = p.codec.ReadBody(argv.Interface()); err != nil {
				p.sendResponse(repReq, invalidRequest, err.Error())
				p.freeRepReq(repReq)
				break ENDFOR
			}
			if argIsValue {
				argv = argv.Elem()
			}

			replyv = reflect.New(mtype.ReplyType.Elem())

			go service.call(p, mtype, repReq, argv, replyv)

			break

		case RESPONSE:
			seq = repReq.Seq
			p.mutex.Lock()
			call = p.pending[seq]
			delete(p.pending, seq)
			p.mutex.Unlock()

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
				err = p.codec.ReadBody(call.Reply)
				if err != nil {
					call.Error = errors.New(fmt.Sprintf("Invalid response value: %v", call.Reply))
					call.done()
					err = nil
					break
				}

				call.done()
			}
			break
		}
	}

	// Terminate pending calls.
	p.sending.Lock()
	p.mutex.Lock()
	p.shutdown = true
	closing := p.closing

	var callError error
	if closing {
		callError = ErrShutdown
	} else {
		callError = io.ErrUnexpectedEOF
	}
	for _, call := range p.pending {
		call.Error = callError
		call.done()
	}
	p.mutex.Unlock()
	p.sending.Unlock()
	if err != nil && err != io.EOF && !closing {
		log.Println("rpc: server protocol error:", err)
	}

	p.Close()
}

func (p *Protocol) getRepReq() *RepReq {
	p.reqLock.Lock()
	req := p.freeReq
	if req == nil {
		req = new(RepReq)
	} else {
		p.freeReq = req.next
		*req = RepReq{}
	}
	p.reqLock.Unlock()
	return req
}

func (p *Protocol) freeRepReq(req *RepReq) {
	p.reqLock.Lock()
	req.next = p.freeReq
	p.freeReq = req
	p.reqLock.Unlock()
}

func (s *Protocol) Close() error {
	s.mutex.Lock()
	if s.shutdown || s.closing {
		s.mutex.Unlock()
		return ErrShutdown
	}
	s.closing = true
	s.mutex.Unlock()
	return s.codec.Close()
}

func (p *Protocol) sendResponse(req *RepReq, reply interface{}, errmsg string) {
	req.Type = RESPONSE
	if errmsg != "" {
		req.Error = errmsg
		reply = invalidRequest
	}

	p.sending.Lock()
	err := p.codec.Write(req, reply)
	p.sending.Unlock()

	if err != nil {
		log.Println("rpc: writing response:", err)
	}
}

func (p *Protocol) sendRequest(call *Call) {
	p.sending.Lock()
	defer p.sending.Unlock()

	// Register this call.
	p.mutex.Lock()
	if p.shutdown || p.closing {
		call.Error = ErrShutdown
		p.mutex.Unlock()
		call.done()
		return
	}
	seq := p.seq
	p.seq++
	p.pending[seq] = call
	p.mutex.Unlock()

	// Encode and send the request.
	p.request.Type = REQUEST
	p.request.Seq = seq
	p.request.ServiceMethod = call.ServiceMethod
	err := p.codec.Write(&p.request, call.Args)
	if err != nil {
		p.mutex.Lock()
		call = p.pending[seq]
		delete(p.pending, seq)
		p.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go invokes the function asynchronously.  It returns the Call structure representing
// the invocation.  The done channel will signal when the call is complete by returning
// the same Call object.  If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
func (s *Protocol) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
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
func (s *Protocol) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-s.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}

// A Codec implements reading of RPC requests and writing of
// RPC responses for the server side of an RPC session.
// The server calls ReadHeader and ReadBody in pairs
// to read requests from the connection, and it calls Write to
// write a response back.  The server calls Close when finished with the
// connection. ReadBody may be called with a nil
// argument to force the body of the request to be read and discarded.
type Codec interface {
	ReadHeader(*RepReq) error
	ReadBody(interface{}) error
	Write(*RepReq, interface{}) error

	Close() error
}

type ExtendedMethod interface {
	SetProtocol(*Protocol)
}
