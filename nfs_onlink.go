package nfs

import (
	"bytes"
	"context"
	"os"
	"reflect"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs-client/nfs/xdr"
)

// onLink implements the NFSv3 LINK procedure (hard link).
//
// RFC 1813 LINK3args is
//
//	struct LINK3args {
//	    nfs_fh3     file;   // existing object to link to
//	    diropargs3  link;   // { directory handle, new name }
//	};
//
// The previous implementation parsed it as SYMLINK3args (diropargs3 + sattr3 +
// string), so a real hard-link request from the kernel died in the XDR parser
// with EINVAL before any filesystem call was made.
func onLink(ctx context.Context, w *response, userHandle Handler) error {
	w.errorFmt = linkErrorFormatter

	// file: nfs_fh3 of the existing object.
	fileHandle, err := xdr.ReadOpaque(w.req.Body)
	if err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}
	// link: diropargs3 naming where the new link is created.
	link := DirOpArg{}
	if err := xdr.Read(w.req.Body, &link); err != nil {
		return &NFSStatusError{NFSStatusInval, err}
	}

	fs, existingPath, err := userHandle.FromHandle(fileHandle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	dirFs, dirPath, err := userHandle.FromHandle(link.Handle)
	if err != nil {
		return &NFSStatusError{NFSStatusStale, err}
	}
	// A hard link cannot cross filesystems.
	if !reflect.DeepEqual(fs, dirFs) {
		return &NFSStatusError{NFSStatusXDev, os.ErrInvalid}
	}

	if !billy.CapabilityCheck(fs, billy.WriteCapability) {
		return &NFSStatusError{NFSStatusROFS, os.ErrPermission}
	}

	if len(string(link.Filename)) > PathNameMax {
		return &NFSStatusError{NFSStatusNameTooLong, os.ErrInvalid}
	}

	newFilePath := fs.Join(append(dirPath, string(link.Filename))...)
	if _, err := fs.Stat(newFilePath); err == nil {
		return &NFSStatusError{NFSStatusExist, os.ErrExist}
	}
	if s, err := fs.Stat(fs.Join(dirPath...)); err != nil {
		return &NFSStatusError{NFSStatusAccess, err}
	} else if !s.IsDir() {
		return &NFSStatusError{NFSStatusNotDir, nil}
	}

	// billy's base Filesystem has no hard-link operation; only a Change that
	// also implements UnixChange can create one. Report the honest "server does
	// not support this" rather than letting it fall through to ACCES.
	changer := userHandle.Change(fs)
	linker, ok := changer.(UnixChange)
	if !ok {
		return &NFSStatusError{NFSStatusNotSupp, os.ErrInvalid}
	}

	existingFilePath := fs.Join(existingPath...)
	if err := linker.Link(existingFilePath, newFilePath); err != nil {
		if os.IsPermission(err) {
			return &NFSStatusError{NFSStatusAccess, err}
		}
		return &NFSStatusError{NFSStatusIO, err}
	}

	writer := bytes.NewBuffer([]byte{})
	if err := xdr.Write(writer, uint32(NFSStatusOk)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	// LINK3resok { post_op_attr file_attributes; wcc_data linkdir_wcc; }
	if err := WritePostOpAttrs(writer, tryStat(fs, existingPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	if err := WriteWcc(writer, nil, tryStat(fs, dirPath)); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}

	if err := w.Write(writer.Bytes()); err != nil {
		return &NFSStatusError{NFSStatusServerFault, err}
	}
	return nil
}
