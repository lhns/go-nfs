package helpers

import (
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/willscott/go-nfs"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewCachingHandler wraps a handler to provide a basic to/from-file handle cache.
func NewCachingHandler(h nfs.Handler, limit int) nfs.Handler {
	return NewCachingHandlerWithVerifierLimit(h, limit, limit)
}

// NewCachingHandlerWithVerifierLimit provides a basic to/from-file handle cache that can be tuned with a smaller cache of active directory listings.
func NewCachingHandlerWithVerifierLimit(h nfs.Handler, limit int, verifierLimit int) nfs.Handler {
	if limit < 2 || verifierLimit < 2 {
		nfs.Log.Warnf("Caching handler created with insufficient cache to support directory listing", "size", limit, "verifiers", verifierLimit)
	}
	cache, _ := lru.New[uuid.UUID, entry](limit)
	verifiers, _ := lru.New[uint64, verifier](verifierLimit)
	return &CachingHandler{
		Handler:         h,
		activeHandles:   cache,
		reverseHandles:  make(map[string][]uuid.UUID),
		activeVerifiers: verifiers,
		cacheLimit:      limit,
	}
}

// CachingHandler implements to/from handle via an LRU cache.
type CachingHandler struct {
	nfs.Handler
	activeHandles  *lru.Cache[uuid.UUID, entry]
	reverseHandles map[string][]uuid.UUID
	// reverseHandlesMu guards reverseHandles. activeHandles has a lock of its
	// own and never reaches back here, so it may be taken under this one; the
	// reverse order is never safe.
	reverseHandlesMu sync.RWMutex
	activeVerifiers  *lru.Cache[uint64, verifier]
	cacheLimit       int
}

type entry struct {
	f billy.Filesystem
	p []string
}

// ToHandle takes a file and represents it with an opaque handle to reference it.
// In stateless nfs (when it's serving a unix fs) this can be the device + inode
// but we can generalize with a stateful local cache of handed out IDs.
//
// The search and the mint are one critical section. Requests on one connection
// are handled concurrently, so two LOOKUPs of one path that both missed would
// otherwise mint a handle each and hand the client two handles for one file,
// which a client is entitled to treat as two objects.
func (c *CachingHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	joinedPath := f.Join(path...)

	c.reverseHandlesMu.Lock()
	defer c.reverseHandlesMu.Unlock()

	if handle := c.searchReverseCacheLocked(f, joinedPath); handle != nil {
		return handle
	}

	id := uuid.New()

	newPath := make([]string, len(path))

	copy(newPath, path)
	evictedKey, evictedPath, ok := c.activeHandles.GetOldest()
	if evicted := c.activeHandles.Add(id, entry{f, newPath}); evicted && ok {
		rk := evictedPath.f.Join(evictedPath.p...)
		c.evictReverseCacheLocked(rk, evictedKey)
	}

	c.reverseHandles[joinedPath] = append(c.reverseHandles[joinedPath], id)
	b, _ := id.MarshalBinary()

	return b
}

// FromHandle converts from an opaque handle to the file it represents
func (c *CachingHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	id, err := uuid.FromBytes(fh)
	if err != nil {
		return nil, []string{}, err
	}

	if f, ok := c.activeHandles.Get(id); ok {
		c.refreshAncestors(f)

		newP := make([]string, len(f.p))
		copy(newP, f.p)
		return f.f, newP, nil
	}
	return nil, []string{}, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

// refreshAncestors marks every ancestor of a path as recently used, so a
// parent is not evicted while a handle below it is still live. An evicted
// ancestor surfaces as ESTALE on a path the client is still using.
//
// The ancestors are reached through the reverse index rather than by scanning
// the cache. This runs on EVERY request, and the cache is sized for a source
// tree (remote-docker allows a million handles), so a scan makes the cost of
// resolving one handle grow with the number of files the client has ever
// touched: measured there at 12us per resolution with 100 handles cached and
// 10.3ms with 50,000, linear in the cache size. That is 1.9 seconds of pure
// cache walking for one 256MB write, which is enough to push the RPCs queued
// behind it past a soft mount's timeout and turn the write into EIO.
//
// The index is read under its own lock rather than through a snapshot:
// evictReverseCacheLocked shifts a key's slice IN PLACE, so a range over one
// handed out earlier races it.
func (c *CachingHandler) refreshAncestors(f entry) {
	c.reverseHandlesMu.RLock()
	defer c.reverseHandlesMu.RUnlock()

	for i := len(f.p) - 1; i >= 0; i-- {
		for _, id := range c.reverseHandles[f.f.Join(f.p[:i]...)] {
			_, _ = c.activeHandles.Get(id)
		}
	}
}

// searchReverseCacheLocked requires reverseHandlesMu.
func (c *CachingHandler) searchReverseCacheLocked(f billy.Filesystem, path string) []byte {
	uuids := c.reverseHandles[path]

	for _, id := range uuids {
		if candidate, ok := c.activeHandles.Get(id); ok {
			if reflect.DeepEqual(candidate.f, f) {
				return id[:]
			}
		}
	}

	return nil
}

func (c *CachingHandler) evictReverseCache(path string, handle uuid.UUID) {
	c.reverseHandlesMu.Lock()
	defer c.reverseHandlesMu.Unlock()
	c.evictReverseCacheLocked(path, handle)
}

// evictReverseCacheLocked requires reverseHandlesMu.
func (c *CachingHandler) evictReverseCacheLocked(path string, handle uuid.UUID) {
	uuids, ok := c.reverseHandles[path]
	if !ok {
		return
	}
	for i, u := range uuids {
		if u == handle {
			c.reverseHandles[path] = append(uuids[:i], uuids[i+1:]...)
			return
		}
	}
}

// Rename re-points the cached handle for source (and any handle below it, so a
// file held open inside a renamed directory survives) at dest instead of
// invalidating it. Keying handles by path means a plain invalidation on rename
// hands the client ESTALE for a file it still has open, where a native mount
// keeps the descriptor valid; moving the entry keeps the existing handle live
// under its new path. See nfs_onrename.go.
func (c *CachingHandler) Rename(sourceFs billy.Filesystem, source []string, destFs billy.Filesystem, dest []string) error {
	sourceJoin := sourceFs.Join(source...)
	prefix := sourceJoin + string(filepath.Separator)

	c.reverseHandlesMu.Lock()
	defer c.reverseHandlesMu.Unlock()

	// Collect first: mutating reverseHandles while ranging it (and adding new
	// keys) would leave which entries are visited undefined.
	type move struct {
		oldKey  string
		newKey  string
		newPath []string
		ids     []uuid.UUID
	}
	var moves []move
	for path, ids := range c.reverseHandles {
		var rel []string
		switch {
		case path == sourceJoin:
			rel = nil
		case strings.HasPrefix(path, prefix):
			rel = strings.Split(strings.TrimPrefix(path, prefix), string(filepath.Separator))
		default:
			continue
		}
		newPath := append(append([]string{}, dest...), rel...)
		moves = append(moves, move{
			oldKey:  path,
			newKey:  destFs.Join(newPath...),
			newPath: newPath,
			ids:     ids,
		})
	}

	for _, m := range moves {
		delete(c.reverseHandles, m.oldKey)
		for _, id := range m.ids {
			e, ok := c.activeHandles.Peek(id)
			if !ok {
				continue
			}
			e.f = destFs
			e.p = append([]string{}, m.newPath...)
			c.activeHandles.Add(id, e)
			c.reverseHandles[m.newKey] = append(c.reverseHandles[m.newKey], id)
		}
	}
	return nil
}

func (c *CachingHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	//Remove from cache
	id, _ := uuid.FromBytes(handle)
	entry, ok := c.activeHandles.Get(id)
	if ok {
		rk := entry.f.Join(entry.p...)
		c.evictReverseCache(rk, id)
	}
	c.activeHandles.Remove(id)
	return nil
}

// HandleLimit exports how many file handles can be safely stored by this cache.
func (c *CachingHandler) HandleLimit() int {
	return c.cacheLimit
}

type verifier struct {
	path     string
	contents []fs.FileInfo
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	//calculate a cookie-verifier.
	vHash := sha256.New()

	// Add the path to avoid collisions of directories with the same content
	vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(len(path))))
	vHash.Write([]byte(path))

	for _, c := range contents {
		vHash.Write([]byte(c.Name())) // Never fails according to the docs
	}

	verify := vHash.Sum(nil)[0:8]
	return binary.BigEndian.Uint64(verify)
}

func (c *CachingHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	id := hashPathAndContents(path, contents)
	c.activeVerifiers.Add(id, verifier{path, contents})
	return id
}

func (c *CachingHandler) DataForVerifier(path string, id uint64) []fs.FileInfo {
	if cache, ok := c.activeVerifiers.Get(id); ok {
		return cache.contents
	}
	return nil
}
