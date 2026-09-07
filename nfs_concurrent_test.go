package nfs_test

import (
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers/memfs"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	rpc "github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

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

	target, cleanup := startMemNFS(t, fs)
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
	target, cleanup := startMemNFSBounded(t, fs, bound)
	defer cleanup()

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatalf("looking up the root: %v", err)
	}

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fs.release) }) }
	// The in-flight lookups must finish before cleanup closes the client.
	// go-nfs-client's Client.Close closes the channel its own receive
	// goroutine delivers replies on, so closing it with a call outstanding is
	// a close/send race inside that library. Deferred before release so the
	// order on the way out is release, wait, cleanup.
	var inflight sync.WaitGroup
	defer inflight.Wait()
	defer release()

	for i := 0; i < bound+1; i++ {
		inflight.Add(1)
		go func() {
			defer inflight.Done()
			_, _ = lookupRaw(target, rootFH, "slow")
		}()
	}

	// Wait for the bound to saturate, then hold: the extra request must still
	// not be inside the filesystem once it has had every chance to arrive.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := fs.blocked.Load()
		if got > bound {
			t.Fatalf("%d requests were inside the filesystem at once, want at most %d", got, bound)
		}
		if got == bound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests reached the filesystem, want the bound %d to be saturated", got, bound)
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if got := fs.blocked.Load(); got != bound {
		t.Fatalf("%d requests were inside the filesystem at once, want the bound %d to hold", got, bound)
	}
}
