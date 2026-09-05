package nfs_test

import (
	"io/fs"
	"syscall"
	"testing"

	"github.com/go-git/go-billy/v5"
	nfs "github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers/memfs"

	nfsc "github.com/willscott/go-nfs-client/nfs"
	rpc "github.com/willscott/go-nfs-client/nfs/rpc"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// einvalFS is a filesystem whose Create fails the way a Windows host does when
// it cannot spell a name: a *fs.PathError wrapping syscall.EINVAL.
type einvalFS struct {
	billy.Filesystem
}

func (e *einvalFS) Create(filename string) (billy.File, error) {
	return nil, &fs.PathError{Op: "open", Path: filename, Err: syscall.EINVAL}
}

// createStatus issues a raw CREATE (unchecked mode, no attributes) and returns
// the NFS status word.
func createStatus(t *nfsc.Target, dirFH []byte, name string) (uint32, error) {
	type args struct {
		rpc.Header
		Where nfsc.Diropargs3
		How   uint32
		Attrs nfsc.Sattr3
	}
	res, err := t.Call(&args{
		Header: nfsHeader(uint32(nfs.NFSProcedureCreate)),
		Where:  nfsc.Diropargs3{FH: dirFH, Filename: name},
		How:    0, // UNCHECKED
	})
	if err != nil {
		return 0, err
	}
	return xdr.ReadUint32(res)
}

// TestCreateEINVAL pins that a filesystem error of syscall.EINVAL is reported
// as NFSStatusInval rather than NFSStatusAccess.
func TestCreateEINVAL(t *testing.T) {
	mem := memfs.New()
	r, _ := mem.Create("/test")
	r.Close()
	target, cleanup := startMemNFS(t, &einvalFS{mem})
	defer cleanup()

	_, rootFH, err := target.Lookup("/")
	if err != nil {
		t.Fatal(err)
	}

	status, err := createStatus(target, rootFH, "unspellable")
	if err != nil {
		t.Fatal(err)
	}
	if nfs.NFSStatus(status) != nfs.NFSStatusInval {
		t.Fatalf("create with EINVAL from the filesystem: got status %d (%s), want Inval (%d)",
			status, nfs.NFSStatus(status).String(), nfs.NFSStatusInval)
	}
}
