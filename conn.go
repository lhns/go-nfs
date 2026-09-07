package nfs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

	xdr2 "github.com/rasky/go-xdr/xdr2"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// maxBufferedRequestBytes is the largest request read into memory before it is
// dispatched to a worker. See readRequestHeader.
const maxBufferedRequestBytes = 2 << 20

var (
	// ErrInputInvalid is returned when input cannot be parsed
	ErrInputInvalid = errors.New("invalid input")
	// ErrAlreadySent is returned when writing a header/status multiple times
	ErrAlreadySent = errors.New("response already started")
)

// ResponseCode is a combination of accept_stat and reject_stat.
type ResponseCode uint32

// ResponseCode Codes
const (
	ResponseCodeSuccess ResponseCode = iota
	ResponseCodeProgUnavailable
	ResponseCodeProcUnavailable
	ResponseCodeGarbageArgs
	ResponseCodeSystemErr
	ResponseCodeRPCMismatch
	ResponseCodeAuthError
)

type conn struct {
	*Server
	writeSerializer chan []byte
	net.Conn
}

// serve reads requests off one connection and hands each to a worker.
//
// A client multiplexes every outstanding RPC for a mount onto one connection,
// so handling them one at a time makes the slowest operation the latency of
// everything queued behind it. Replies therefore complete out of order, which
// the protocol allows: a reply is matched to its call by XID.
func (c *conn) serve(ctx context.Context) {
	connCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		// Cancel before waiting. A worker parked in finish() is released by the
		// context, so waiting first would deadlock against it once
		// serializeWrites has stopped draining.
		cancel()
		wg.Wait()
	}()
	c.writeSerializer = make(chan []byte, 1)
	go c.serializeWrites(connCtx)

	sem := make(chan struct{}, c.Server.maxConcurrentRequests())

	bio := bufio.NewReader(c.Conn)
	for {
		w, err := c.readRequestHeader(connCtx, bio)
		if err != nil {
			if err == io.EOF {
				// Clean close.
				c.Close()
				return
			}
			return
		}
		Log.Tracef("request: %v", w.req)

		select {
		case sem <- struct{}{}:
		case <-connCtx.Done():
			return
		}

		if !w.buffered {
			// Arguments too large to buffer are still a reader over the
			// connection, so this must run before anything else reads it.
			ok := c.dispatch(connCtx, w)
			<-sem
			if !ok {
				return
			}
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if !c.dispatch(connCtx, w) {
				// Release anything parked in finish(). The read at the top of
				// the loop then fails on the closed connection and serve ends.
				cancel()
			}
		}()
	}
}

// dispatch handles one request and queues its reply, reporting whether the
// connection may carry more.
//
// A failure ends the connection under whatever else is in flight, which is what
// a serial server did too: it dropped every request queued behind the failing
// one. Their replies are never written, so the client retries them on the next
// connection.
func (c *conn) dispatch(ctx context.Context, w *response) bool {
	// finish runs even when handle failed, so a partially written reply is
	// queued exactly as it was before requests were dispatched concurrently.
	handleErr := c.handle(ctx, w)
	finishErr := w.finish(ctx)
	if handleErr == nil && finishErr == nil {
		return true
	}
	if handleErr != nil {
		Log.Errorf("error handling req: %v", handleErr)
	} else {
		Log.Errorf("error sending response: %v", finishErr)
	}
	c.Close()
	return false
}

func (c *conn) serializeWrites(ctx context.Context) {
	// todo: maybe don't need the extra buffer
	writer := bufio.NewWriter(c.Conn)
	var fragmentBuf [4]byte
	var fragmentInt uint32
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.writeSerializer:
			if !ok {
				return
			}
			// prepend the fragmentation header
			fragmentInt = uint32(len(msg))
			fragmentInt |= (1 << 31)
			binary.BigEndian.PutUint32(fragmentBuf[:], fragmentInt)
			n, err := writer.Write(fragmentBuf[:])
			if n < 4 || err != nil {
				return
			}
			n, err = writer.Write(msg)
			if err != nil {
				return
			}
			if n < len(msg) {
				panic("todo: ensure writes complete fully.")
			}
			if err = writer.Flush(); err != nil {
				return
			}
		}
	}
}

// Handle a request. errors from this method indicate a failure to read or
// write on the network stream, and trigger a disconnection of the connection.
func (c *conn) handle(ctx context.Context, w *response) error {
	handler := c.Server.handlerFor(w.req.Header.Prog, w.req.Header.Proc)
	if handler == nil {
		Log.Errorf("No handler for %d.%d", w.req.Header.Prog, w.req.Header.Proc)
		if err := w.drain(ctx); err != nil {
			return err
		}
		return c.err(ctx, w, &ResponseCodeProcUnavailableError{})
	}
	appError := handler(ctx, w, c.Server.Handler)
	if drainErr := w.drain(ctx); drainErr != nil {
		return drainErr
	}
	if appError != nil && !w.responded {
		if err := c.err(ctx, w, appError); err != nil {
			return err
		}
	}
	if !w.responded {
		Log.Errorf("Handler did not indicate response status via writing or erroring")
		if err := c.err(ctx, w, &ResponseCodeSystemError{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) err(ctx context.Context, w *response, err error) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if w.err == nil {
		w.err = err
	}

	if w.responded {
		return nil
	}

	rpcErr := w.errorFmt(err)
	if writeErr := w.writeHeader(rpcErr.Code()); writeErr != nil {
		return writeErr
	}

	body, _ := rpcErr.MarshalBinary()
	return w.Write(body)
}

type request struct {
	xid uint32
	rpc.Header
	Body io.Reader
}

func (r *request) String() string {
	if r.Header.Prog == nfsServiceID {
		return fmt.Sprintf("RPC #%d (nfs.%s)", r.xid, NFSProcedure(r.Header.Proc))
	} else if r.Header.Prog == mountServiceID {
		return fmt.Sprintf("RPC #%d (mount.%s)", r.xid, MountProcedure(r.Header.Proc))
	}
	return fmt.Sprintf("RPC #%d (%d.%d)", r.xid, r.Header.Prog, r.Header.Proc)
}

type response struct {
	*conn
	writer    *bytes.Buffer
	responded bool
	err       error
	errorFmt  func(error) RPCError
	req       *request
	// buffered reports whether req.Body reads from memory rather than from the
	// connection, which is what makes the request safe to hand to a worker.
	buffered bool
}

func (w *response) writeXdrHeader() error {
	err := xdr.Write(w.writer, &w.req.xid)
	if err != nil {
		return err
	}
	respType := uint32(1)
	err = xdr.Write(w.writer, &respType)
	if err != nil {
		return err
	}
	return nil
}

func (w *response) writeHeader(code ResponseCode) error {
	if w.responded {
		return ErrAlreadySent
	}
	w.responded = true
	if err := w.writeXdrHeader(); err != nil {
		return err
	}

	status := rpc.MsgAccepted
	if code == ResponseCodeAuthError || code == ResponseCodeRPCMismatch {
		status = rpc.MsgDenied
	}

	err := xdr.Write(w.writer, &status)
	if err != nil {
		return err
	}

	if status == rpc.MsgAccepted {
		// Write opaque_auth header.
		err = xdr.Write(w.writer, &rpc.AuthNull)
		if err != nil {
			return err
		}
	}

	return xdr.Write(w.writer, &code)
}

// Write a response to an xdr message
func (w *response) Write(dat []byte) error {
	if !w.responded {
		if err := w.writeHeader(ResponseCodeSuccess); err != nil {
			return err
		}
	}

	acc := 0
	for acc < len(dat) {
		n, err := w.writer.Write(dat[acc:])
		if err != nil {
			return err
		}
		acc += n
	}
	return nil
}

// drain reads the rest of the request frame if not consumed by the handler.
func (w *response) drain(ctx context.Context) error {
	if reader, ok := w.req.Body.(*io.LimitedReader); ok {
		if reader.N == 0 {
			return nil
		}
		// todo: wrap body in a context reader.
		_, err := io.CopyN(io.Discard, w.req.Body, reader.N)
		if err == nil || err == io.EOF {
			return nil
		}
		return err
	}
	return io.ErrUnexpectedEOF
}

func (w *response) finish(ctx context.Context) error {
	select {
	case w.conn.writeSerializer <- w.writer.Bytes():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *conn) readRequestHeader(ctx context.Context, reader *bufio.Reader) (w *response, err error) {
	fragment, err := xdr.ReadUint32(reader)
	if err != nil {
		if xdrErr, ok := err.(*xdr2.UnmarshalError); ok {
			if xdrErr.Err == io.EOF {
				return nil, io.EOF
			}
		}
		return nil, err
	}
	if fragment&(1<<31) == 0 {
		Log.Warnf("Warning: haven't implemented fragment reconstruction.\n")
		return nil, ErrInputInvalid
	}
	reqLen := fragment - uint32(1<<31)
	if reqLen < 40 {
		return nil, ErrInputInvalid
	}

	// The body is read in full so it reads from memory rather than from the
	// connection. Handlers parse their arguments lazily out of req.Body, so a
	// body left on the connection can only be handled while nothing else reads
	// that connection, which is what made a connection serial.
	//
	// reqLen is the client's own number and FSINFO advertises a 1 GiB wtmax, so
	// buffering unconditionally would let one frame header allocate that much,
	// times the worker bound. Over the cap the body stays a reader over the
	// connection and is handled inline; Linux negotiates a wsize of at most
	// 1 MiB, so that is the exotic case.
	var body io.Reader = reader
	buffered := reqLen <= maxBufferedRequestBytes
	if buffered {
		buf := make([]byte, reqLen)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		body = bytes.NewReader(buf)
	}
	r := io.LimitedReader{R: body, N: int64(reqLen)}

	xid, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	reqType, err := xdr.ReadUint32(&r)
	if err != nil {
		return nil, err
	}
	if reqType != 0 { // 0 = request, 1 = response
		return nil, ErrInputInvalid
	}

	req := request{
		xid,
		rpc.Header{},
		&r,
	}
	if err = xdr.Read(&r, &req.Header); err != nil {
		return nil, err
	}

	w = &response{
		conn:     c,
		req:      &req,
		errorFmt: basicErrorFormatter,
		// TODO: use a pool for these.
		writer:   bytes.NewBuffer([]byte{}),
		buffered: buffered,
	}
	return w, nil
}
