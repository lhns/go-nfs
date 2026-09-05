package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// isInvalid reports whether a filesystem error is EINVAL, which onCreate,
// onMkdir, onSymlink and onRename map to NFSStatusInval. A backend that cannot
// spell a name (a Windows host given a reserved or unencodable filename)
// returns a *fs.PathError wrapping syscall.EINVAL; reporting that as ACCES or
// IO sends the client hunting for a permission or disk fault that is not there.
func isInvalid(err error) bool {
	return errors.Is(err, syscall.EINVAL)
}

// dirNotEmpty reports whether err (from a failed Remove or Rename of path)
// means the target is a non-empty directory. Native rename(2)/rmdir(2) and a
// Linux bind mount return ENOTEMPTY here; go-nfs used to map every such failure
// to NFSStatusIO. Some billy backends (memfs) return a plain error rather than
// syscall.ENOTEMPTY, so fall back to inspecting the target directly.
func dirNotEmpty(fs billy.Filesystem, path string, err error) bool {
	if errors.Is(err, syscall.ENOTEMPTY) {
		return true
	}
	info, serr := fs.Lstat(path)
	if serr != nil || !info.IsDir() {
		return false
	}
	children, rerr := fs.ReadDir(path)
	return rerr == nil && len(children) > 0
}

func onRemove(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = wccDataErrorFormatter
	obj := DirOpArg{}
	if err := xdr.Read(w.req.Body, &obj); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, path, err := userHandle.FromHandle(obj.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(obj.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, nil}
	}

	fullPath := fs.Join(path...)
	dirInfo, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !dirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(dirInfo, fullPath).AsCache()

	toDelete := fs.Join(append(path, string(obj.Filename))...)
	toDeleteHandle := userHandle.ToHandle(fs, append(path, string(obj.Filename)))

	err = fs.Remove(toDelete)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		if dirNotEmpty(fs, toDelete, err) {
			return &NFSStatusError{NFSStatusNotEmpty, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	if err := userHandle.InvalidateHandle(fs, toDeleteHandle); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, path)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
