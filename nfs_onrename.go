package nfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

var doubleWccErrorBody = [16]byte{}

// renameHandleMover is the optional interface a Handler implements to re-point a
// cached handle at its new path on rename rather than invalidating it, so a
// client that renames a file it holds open does not get ESTALE. Satisfied by
// helpers.CachingHandler.
type renameHandleMover interface {
	Rename(sourceFs billy.Filesystem, source []string, destFs billy.Filesystem, dest []string) error
}

func onRename(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = errFormatterWithBody(doubleWccErrorBody[:])
	from := DirOpArg{}
	err := xdr.Read(w.req.Body, &from)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs, fromPath, err := userHandle.FromHandle(from.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}

	to := DirOpArg{}
	if err = xdr.Read(w.req.Body, &to); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	fs2, toPath, err := userHandle.FromHandle(to.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// check the two fs are the same
	if !reflect.DeepEqual(fs, fs2) {
		return &NFSStatusError{NFSStatusNotSupp, os.ErrPermission}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(from.Filename)) > PathNameMax || len(string(to.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}

	fromDirPath := fs.Join(fromPath...)
	fromDirInfo, err := fs.Stat(fromDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !fromDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preCacheData := ToFileAttribute(fromDirInfo, fromDirPath).AsCache()

	toDirPath := fs.Join(toPath...)
	toDirInfo, err := fs.Stat(toDirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}
	if !toDirInfo.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}
	preDestData := ToFileAttribute(toDirInfo, toDirPath).AsCache()

	sourcePath := append(append([]string{}, fromPath...), string(from.Filename))
	destPath := append(append([]string{}, toPath...), string(to.Filename))
	oldHandle := userHandle.ToHandle(fs, sourcePath)

	fromLoc := fs.Join(sourcePath...)
	toLoc := fs.Join(destPath...)

	// rename(2) replaces an existing empty directory atomically, but some billy
	// backends (osfs.BoundOS) refuse a rename onto an existing target. Emulate
	// the native behaviour when both sides are directories: reject a non-empty
	// target with NFSStatusNotEmpty (what native and a Linux bind mount return,
	// not NFSStatusIO), and clear an empty one first so a backend that would
	// otherwise refuse still succeeds.
	if toInfo, terr := fs.Lstat(toLoc); terr == nil && toInfo.IsDir() {
		fromInfo, ferr := fs.Lstat(fromLoc)
		if children, rerr := fs.ReadDir(toLoc); rerr == nil && len(children) > 0 {
			return &NFSStatusError{NFSStatusNotEmpty, os.ErrExist}
		}
		if ferr == nil && fromInfo.IsDir() {
			if err := fs.Remove(toLoc); err != nil {
				return &NFSStatusError{NFSStatusIO, err}
			}
		}
	}

	err = fs.Rename(fromLoc, toLoc)
	if err != nil {
		if os.IsNotExist(err) {
			return &NFSStatusError{NFSStatusNoEnt, err}
		}
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		if isInvalid(err) {
			return &NFSStatusError{NFSStatusInval, err}
		}
		if dirNotEmpty(fs, toLoc, err) {
			return &NFSStatusError{NFSStatusNotEmpty, err}
		}
		// A backend that refuses to replace an existing target (rather than one
		// that could not, above) reports it here; surface EEXIST, not EIO.
		if errors.Is(err, os.ErrExist) {
			return &NFSStatusError{NFSStatusExist, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	// Move the cached handle to the new path instead of invalidating it, so a
	// file (or a file open inside a renamed directory) stays reachable through
	// the handle the client already holds.
	if mover, ok := userHandle.(renameHandleMover); ok {
		if err := mover.Rename(fs, sourcePath, fs, destPath); err != nil {
			return &NFSStatusError{NFSStatusServerFault, err}
		}
	} else if err := userHandle.InvalidateHandle(fs, oldHandle); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := WriteWcc(writer, preCacheData, tryStat(fs, fromPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, preDestData, tryStat(fs, toPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
