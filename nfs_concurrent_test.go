package nfs_test

import (
	"fmt"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"github.com/willscott/go-nfs/helpers/memfs"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	rpc "github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// startMemNFSWithServer is startMemNFS with the Server built here, so a test
// can set a field on it before it serves.
func startMemNFSWithServer(t *testing.T, fs billy.Filesystem, configure func(*nfs.Server)) (*nfsc.Target, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &nfs.Server{Handler: helpers.NewCachingHandler(helpers.NewNullAuthHandler(fs), 1024)}
	if configure != nil {
		configure(srv)
	}
	go func() {
		_ = srv.Serve(listener)
	}()

	c, err := rpc.DialTCP(listener.Addr().Network(), listener.Addr().(*net.TCPAddr).String(), false)
	if err != nil {
		t.Fatal(err)
	}
	var mounter nfsc.Mount
	mounter.Client = c
	target, err := mounter.Mount("/", rpc.AuthNull)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	return target, func() {
		_ = mounter.Unmount()
		c.Close()
		_ = listener.Close()
	}
}

// blockingFS holds every Lstat of one name until the test releases it, which
// is how a slow filesystem operation is modelled without a slow filesystem.
type blockingFS struct {
	billy.Filesystem
	name    string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	blocked atomic.Int32
}

func (b *blockingFS) Lstat(p string) (os.FileInfo, error) {
	if path.Base(p) == b.name {
		b.blocked.Add(1)
		b.once.Do(func() { close(b.entered) })
		<-b.release
		b.blocked.Add(-1)
	}
	return b.Filesystem.Lstat(p)
}

// lookupRaw issues one LOOKUP and returns its NFS status word. One RPC, so a
// timing assertion measures the server rather than a client-side walk.
func lookupRaw(target *nfsc.Target, dirFH []byte, name string) (uint32, error) {
	type args struct {
		rpc.Header
		What nfsc.Diropargs3
	}
	res, err := target.Call(&args{
		Header: nfsHeader(uint32(nfs.NFSProcedureLookup)),
		What:   nfsc.Diropargs3{FH: dirFH, Filename: name},
	})
	if err != nil {
		return 0, err
	}
	return xdr.ReadUint32(res)
}

// A slow request must not hold up the requests behind it on the same
// connection. The Linux client multiplexes every outstanding RPC for a mount
// onto one TCP connection, so a server that handles a connection serially
// makes one slow operation the latency of everything queued behind it, and
// under load the queue passes the mount's timeout and the whole batch fails at
// once.
//
// Fails against a serial server: the second LOOKUP is not read off the socket
// at all until the first has returned.
func TestSlowRequestDoesNotBlockTheConnection(t *testing.T) {
	mem := memfs.New()
	for _, name := range []string{"slow", "quick"} {
		f, err := mem.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	fs := &blockingFS{
		Filesystem: mem,
		name:       "slow",
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}

	target, cleanup := startMemNFSWithServer(t, fs, nil)
	defer cleanup()

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatalf("looking up the root: %v", err)
	}

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fs.release) }) }
	defer release()

	slowDone := make(chan error, 1)
	go func() {
		_, err := lookupRaw(target, rootFH, "slow")
		slowDone <- err
	}()

	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the blocking LOOKUP never reached the filesystem")
	}

	quickDone := make(chan error, 1)
	go func() {
		status, err := lookupRaw(target, rootFH, "quick")
		if err == nil && status != nfsc.NFS3Ok {
			err = fmt.Errorf("LOOKUP quick returned status %d", status)
		}
		quickDone <- err
	}()

	const budget = 2 * time.Second
	start := time.Now()
	select {
	case err := <-quickDone:
		if err != nil {
			t.Fatalf("the second LOOKUP failed: %v", err)
		}
		t.Logf("the second LOOKUP completed in %s with the first still blocked", time.Since(start))
	case <-time.After(budget):
		release()
		t.Fatalf("the second LOOKUP on the same connection did not complete within %s while the first was blocked in the filesystem: the connection is handled serially", budget)
	}

	release()
	if err := <-slowDone; err != nil {
		t.Fatalf("the blocked LOOKUP failed after release: %v", err)
	}
}

// The worker bound is what stops a connection from spawning a goroutine, and
// up to wsize of buffered payload, per request in flight. With the bound at N,
// the N+1th request waits.
func TestConcurrencyIsBounded(t *testing.T) {
	mem := memfs.New()
	f, err := mem.Create("slow")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	fs := &blockingFS{
		Filesystem: mem,
		name:       "slow",
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}

	const bound = 2
	target, cleanup := startMemNFSWithServer(t, fs, func(s *nfs.Server) {
		s.MaxConcurrentRequests = bound
	})
	defer cleanup()

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatalf("looking up the root: %v", err)
	}

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fs.release) }) }
	defer release()

	for i := 0; i < bound+1; i++ {
		go func() { _, _ = lookupRaw(target, rootFH, "slow") }()
	}

	// The bound must hold for as long as the requests keep arriving.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := fs.blocked.Load(); got > bound {
			t.Fatalf("%d requests were inside the filesystem at once, want at most %d", got, bound)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := fs.blocked.Load(); got != bound {
		t.Fatalf("%d requests were inside the filesystem, want the bound %d to be saturated", got, bound)
	}
}
