package nfs_test

import (
	"net"
	"testing"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
	"github.com/willscott/go-nfs/helpers/memfs"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	rpc "github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// startMemNFS serves fs over a loopback NFS server through the caching handler
// and returns a mounted client target plus a cleanup function.
func startMemNFS(t *testing.T, fs billy.Filesystem) (*nfsc.Target, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	handler := helpers.NewNullAuthHandler(fs)
	cacheHelper := helpers.NewCachingHandler(handler, 1024)
	go func() {
		_ = nfs.Serve(listener, cacheHelper)
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
	cleanup := func() {
		_ = mounter.Unmount()
		c.Close()
		_ = listener.Close()
	}
	return target, cleanup
}

func nfsHeader(proc uint32) rpc.Header {
	return rpc.Header{
		Rpcvers: 2,
		Prog:    nfsc.Nfs3Prog,
		Vers:    nfsc.Nfs3Vers,
		Proc:    proc,
		Cred:    rpc.AuthNull,
		Verf:    rpc.AuthNull,
	}
}

// rmdirStatus issues a raw RMDIR and returns the NFS status word.
func rmdirStatus(t *nfsc.Target, dirFH []byte, name string) (uint32, error) {
	type args struct {
		rpc.Header
		Object nfsc.Diropargs3
	}
	res, err := t.Call(&args{
		Header: nfsHeader(uint32(nfs.NFSProcedureRmDir)),
		Object: nfsc.Diropargs3{FH: dirFH, Filename: name},
	})
	if err != nil {
		return 0, err
	}
	return xdr.ReadUint32(res)
}

// renameStatus issues a raw RENAME and returns the NFS status word.
func renameStatus(t *nfsc.Target, fromFH []byte, from string, toFH []byte, to string) (uint32, error) {
	type args struct {
		rpc.Header
		From nfsc.Diropargs3
		To   nfsc.Diropargs3
	}
	res, err := t.Call(&args{
		Header: nfsHeader(uint32(nfs.NFSProcedureRename)),
		From:   nfsc.Diropargs3{FH: fromFH, Filename: from},
		To:     nfsc.Diropargs3{FH: toFH, Filename: to},
	})
	if err != nil {
		return 0, err
	}
	return xdr.ReadUint32(res)
}

// linkStatus issues a raw LINK (RFC 1813: nfs_fh3 file + diropargs3 link) and
// returns the NFS status word. A well-formed status also proves the request
// parsed rather than dying in the XDR reader (the SYMLINK-shaped-parse bug).
func linkStatus(t *nfsc.Target, fileFH []byte, dirFH []byte, name string) (uint32, error) {
	type args struct {
		rpc.Header
		File []byte
		Link nfsc.Diropargs3
	}
	res, err := t.Call(&args{
		Header: nfsHeader(uint32(nfs.NFSProcedureLink)),
		File:   fileFH,
		Link:   nfsc.Diropargs3{FH: dirFH, Filename: name},
	})
	if err != nil {
		return 0, err
	}
	return xdr.ReadUint32(res)
}

// TestRmdirNotEmpty pins that removing a non-empty directory reports
// NFSStatusNotEmpty (what a native bind mount returns), not NFSStatusIO.
func TestRmdirNotEmpty(t *testing.T) {
	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	target, cleanup := startMemNFS(t, mem)
	defer cleanup()

	if _, err := target.Mkdir("/d", 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create("/d/f", 0666); err != nil {
		t.Fatal(err)
	}

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatal(err)
	}

	status, err := rmdirStatus(target, rootFH, "d")
	if err != nil {
		t.Fatal(err)
	}
	if nfs.NFSStatus(status) != nfs.NFSStatusNotEmpty {
		t.Fatalf("rmdir of non-empty dir: got status %d (%s), want NotEmpty (%d)",
			status, nfs.NFSStatus(status).String(), nfs.NFSStatusNotEmpty)
	}
}

// TestRenameOverNonEmptyDir pins that renaming onto a non-empty directory
// reports NFSStatusNotEmpty rather than NFSStatusIO.
func TestRenameOverNonEmptyDir(t *testing.T) {
	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	target, cleanup := startMemNFS(t, mem)
	defer cleanup()

	if _, err := target.Mkdir("/a", 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create("/a/x", 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Mkdir("/b", 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create("/b/y", 0666); err != nil {
		t.Fatal(err)
	}

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatal(err)
	}

	status, err := renameStatus(target, rootFH, "a", rootFH, "b")
	if err != nil {
		t.Fatal(err)
	}
	if nfs.NFSStatus(status) != nfs.NFSStatusNotEmpty {
		t.Fatalf("rename onto non-empty dir: got status %d (%s), want NotEmpty (%d)",
			status, nfs.NFSStatus(status).String(), nfs.NFSStatusNotEmpty)
	}
}

// TestRenameOverEmptyDir pins that renaming a directory onto an existing empty
// directory succeeds (native rename(2) replaces it) rather than failing.
func TestRenameOverEmptyDir(t *testing.T) {
	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	target, cleanup := startMemNFS(t, mem)
	defer cleanup()

	if _, err := target.Mkdir("/a", 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Create("/a/x", 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Mkdir("/b", 0755); err != nil {
		t.Fatal(err)
	}

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatal(err)
	}

	status, err := renameStatus(target, rootFH, "a", rootFH, "b")
	if err != nil {
		t.Fatal(err)
	}
	if nfs.NFSStatus(status) != nfs.NFSStatusOk {
		t.Fatalf("rename onto empty dir: got status %d (%s), want Ok",
			status, nfs.NFSStatus(status).String())
	}
	// The moved file must now live under the replaced target.
	if _, _, err := target.Lookup("/b/x"); err != nil {
		t.Fatalf("expected /b/x after rename: %v", err)
	}
	if _, _, err := target.Lookup("/a", false); err == nil {
		t.Fatal("expected /a to be gone after rename")
	}
}

// TestLinkNotSupported pins that a well-formed LINK against a filesystem that
// cannot hard-link parses and reports NFSStatusNotSupp (not EINVAL from a
// SYMLINK-shaped parse, and not ACCES).
func TestLinkNotSupported(t *testing.T) {
	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	target, cleanup := startMemNFS(t, mem)
	defer cleanup()

	if _, err := target.Create("/file", 0666); err != nil {
		t.Fatal(err)
	}
	_, fileFH, err := target.Lookup("/file")
	if err != nil {
		t.Fatal(err)
	}
	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatal(err)
	}

	status, err := linkStatus(target, fileFH, rootFH, "hardlink")
	if err != nil {
		t.Fatal(err)
	}
	if nfs.NFSStatus(status) != nfs.NFSStatusNotSupp {
		t.Fatalf("link on fs without hard-link support: got status %d (%s), want NotSupp (%d)",
			status, nfs.NFSStatus(status).String(), nfs.NFSStatusNotSupp)
	}
}
