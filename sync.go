package aferosync

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/gaboose/afero-guestfs/libguestfs.org/guestfs"
	"github.com/spf13/afero"
)

type Sync struct {
	fs        afero.Fs
	tarReader *tar.Reader
	opts      options

	symlinker  afero.Symlinker
	lchowner   Lchowner
	hardlinker Linker

	pathMap     map[string]struct{}
	deletePaths []string

	baseDirPath    string
	baseDirModTime time.Time

	upd PathUpdate
	err error
}

func (s *Sync) BaseDir() (string, time.Time) {
	return s.baseDirPath, s.baseDirModTime
}

func New(fs afero.Fs, tarReader *tar.Reader, opts ...Option) *Sync {
	ret := Sync{
		fs:        fs,
		tarReader: tarReader,
	}

	for _, o := range append(defaultOpts, opts...) {
		o(&ret.opts)
	}

	if ret.opts.withSymlinks {
		var ok bool
		if ret.symlinker, ok = fs.(afero.Symlinker); !ok {
			ret.err = fmt.Errorf("symlink syncing is enabled but fs doesn't implement afero.Symlinker")
			return &ret
		}

		if ret.lchowner, ok = fs.(Lchowner); !ok {
			ret.err = fmt.Errorf("symlink syncing is enabled but fs doesn't implement aferosync.Lchowner")
			return &ret
		}
	}

	var curFileInfo os.FileInfo
	if ret.opts.withHardLinks || ret.opts.withOwnership {
		var err error
		if curFileInfo, err = fs.Stat("."); err != nil {
			ret.err = fmt.Errorf("symlink syncing is enabled but fs doesn't implement aferosync.Lchowner")
			return &ret
		}
	}

	if ret.opts.withHardLinks {
		var ok bool
		if ret.hardlinker, ok = fs.(Linker); !ok {
			ret.err = fmt.Errorf("hard link syncing is enabled but fs doesn't implement aferosync.Linker")
			return &ret
		}

		if _, ok := curFileInfo.(FileInfoInoer); !ok {
			ret.err = fmt.Errorf("hard link syncing is enabled but fs returned a FileInfo that doesn't implement aferosync.FileInfoInoer")
			return &ret
		}
	}

	if ret.opts.withOwnership {
		if _, ok := curFileInfo.(FileInfoOwner); !ok {
			ret.err = fmt.Errorf("hard link syncing is enabled but fs returned a FileInfo that doesn't implement aferosync.FileInfoInoer")
			return &ret
		}
	}

	return &ret
}

func (s *Sync) Run() ([]PathUpdate, error) {
	updates := []PathUpdate{}
	for s.Next() {
		updates = append(updates, s.Update())
	}
	return updates, s.Err()
}

func (s *Sync) Next() bool {
	if s.err != nil {
		return false
	}

	if s.pathMap == nil {
		var err error
		s.pathMap, err = s.walkFs()
		if err != nil {
			s.err = fmt.Errorf("failed to walk disk: %w", err)
			return false
		}
	}

	// add and update files
	for {
		hdr, err := s.tarReader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			s.err = fmt.Errorf("failed to get next file in tar: %w", err)
			return false
		}

		path := normalizePath(hdr.Name)
		s.upd = PathUpdate{
			Path: path,
		}

		switch hdr.Typeflag {
		case tar.TypeReg:
			if err := s.syncRegularFile(hdr); err != nil {
				s.err = fmt.Errorf("failed to sync regular file: %s: %w", path, err)
				return false
			}
		case tar.TypeDir:
			if err := s.syncDir(hdr); err != nil {
				s.err = fmt.Errorf("failed to sync dir: %s: %w", path, err)
				return false
			}
		case tar.TypeSymlink:
			if !s.opts.withSymlinks {
				continue
			}

			if err := s.syncSymlink(hdr); err != nil {
				s.err = fmt.Errorf("failed to sync symlink: %s: %w", path, err)
				return false
			}
		case tar.TypeLink:
			if !s.opts.withHardLinks {
				continue
			}

			if err := s.syncLink(hdr); err != nil {
				s.err = fmt.Errorf("failed to sync hard link: %s: %w", path, err)
				return false
			}
		default:
			s.err = fmt.Errorf("unexpected file type in tar: %s: %d", path, hdr.Typeflag)
			return false
		}

		delete(s.pathMap, path)

		if !s.upd.IsEmpty() {
			return true
		}
	}

	// flatten s.pathMap
	if s.deletePaths == nil {
		s.deletePaths = make([]string, 0, len(s.pathMap))
		for entry := range s.pathMap {
			s.deletePaths = append(s.deletePaths, entry)
		}
		sort.Strings(s.deletePaths)
	}

	// delete files
	for len(s.deletePaths) > 0 {
		path := s.deletePaths[0]

		// ignore root path
		if path == "." {
			s.deletePaths = s.deletePaths[1:]
			continue
		}

		if _, _, err := LstatOrStat(s.fs, path); errors.Is(err, fs.ErrNotExist) {
			// ignore paths that don't exist
			// some paths appear in the guestfs.Guestfs.Filesystem_walk results but can't be
			// accessed via Lstat or removed, these for example include:
			// - /$OrphanFiles
			// - /lib/modules/5.15.78-v8/build/include/config/\xa
			// - /usr/share/mime/application/\x2
			// - /var/lib/opkg/info/\xf
			// - /var/lib/opkg/info/'
			// - /var/lib/opkg/info/^
			// - /var/lib/opkg/info/\x81
			s.deletePaths = s.deletePaths[1:]
			continue
		} else if err != nil {
			s.err = fmt.Errorf("failed to stat: %s: %w", path, err)
		}

		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			s.err = err
			return false
		}

		if err := s.fs.RemoveAll(path); err != nil {
			s.err = fmt.Errorf("failed to remove: %s: %w", path, err)
			return false
		}

		s.upd = PathUpdate{
			Path: path,
			Update: Update{
				Deleted: true,
			},
		}
		return true
	}

	s.err = s.preserveBaseDir("")
	return false
}

func (s *Sync) Update() PathUpdate {
	if s.err != nil {
		return PathUpdate{}
	}

	return s.upd
}

func (s *Sync) Err() error {
	return s.err
}

func (s *Sync) walkFs() (map[string]struct{}, error) {
	ret := map[string]struct{}{}
	walker, ok := s.fs.(interface {
		Walk(root string, walkFn filepath.WalkFunc) error
	})

	if ok {
		if err := walker.Walk(".", filepath.WalkFunc(func(path string, info fs.FileInfo, err error) error {
			ret[normalizePath(path)] = struct{}{}
			return nil
		})); err != nil {
			return nil, err
		}
	} else {
		if err := afero.Walk(s.fs, ".", filepath.WalkFunc(func(path string, info fs.FileInfo, err error) error {
			ret[normalizePath(path)] = struct{}{}
			return nil
		})); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func walkDiskTSK(g *guestfs.Guestfs) (map[string]struct{}, error) {
	mps, err := g.Mountpoints()
	if err != nil {
		return nil, fmt.Errorf("failed to get mount points: %w", err)
	}

	var partDevice string
	for d, mp := range mps {
		if mp == "/" {
			partDevice = d
			break
		}
	}

	if partDevice == "" {
		return nil, fmt.Errorf("nothing mounted at root")
	}

	entries, err := g.Filesystem_walk(partDevice)
	if err != nil {
		return nil, fmt.Errorf("failed to walk disk: %w", err)
	}

	diskPathMap := make(map[string]struct{}, len(*entries))
	for _, e := range *entries {
		diskPathMap[normalizePath(e.Tsk_name)] = struct{}{}
	}

	return diskPathMap, nil
}

func (s *Sync) syncRegularFile(hdr *tar.Header) error {
	path := normalizePath(hdr.Name)

	fi, _, err := LstatOrStat(s.fs, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to stat: %s: %w", path, err)
	}

	if fi != nil && !fi.Mode().IsRegular() {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		if err = s.fs.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove: %s: %w", path, err)
		}
		fi = nil
	}

	if fi == nil || !(hdr.Size == fi.Size() && hdr.ModTime.Equal(fi.ModTime())) {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		err := afero.WriteReader(s.fs, path, s.tarReader)
		if err != nil {
			return fmt.Errorf("failed to write file: %s: %w", path, err)
		}

		s.upd.Added = true
		fi = nil
	}

	if err := s.syncStat(hdr, fi); err != nil {
		return fmt.Errorf("failed to sync stat: %w", err)
	}

	return nil
}

func (s *Sync) syncDir(hdr *tar.Header) error {
	path := normalizePath(hdr.Name)
	tarFileInfo := hdr.FileInfo()

	fi, _, err := LstatOrStat(s.fs, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to stat: %s: %w", path, err)
	}

	if fi != nil && fi.Mode().Type() != fs.ModeDir {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		if err := s.fs.Remove(path); err != nil {
			return fmt.Errorf("failed to remove: %s: %w", path, err)
		}
		fi = nil
	}

	if fi == nil {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		err := s.fs.Mkdir(path, tarFileInfo.Mode().Perm())
		if err != nil {
			return fmt.Errorf("failed to make file: %s: %w", path, err)
		}

		s.upd.Added = true
		s.upd.Mode = ptr(tarFileInfo.Mode().Perm() | fs.ModeDir)
	}

	if err := s.syncStat(hdr, fi); err != nil {
		return fmt.Errorf("failed to sync stat: %w", err)
	}

	return nil
}

func (s *Sync) syncSymlink(hdr *tar.Header) error {
	path := normalizePath(hdr.Name)

	fi, _, err := s.symlinker.LstatIfPossible(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to lstat: %s: %w", path, err)
	}

	// remove if not symlink
	if fi != nil && fi.Mode().Type() != fs.ModeSymlink {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}
		if err := s.fs.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to remove: %s: %w", path, err)
		}
		fi = nil
	}

	// remove if symlink but target differs
	if fi != nil {
		target, err := s.symlinker.ReadlinkIfPossible(path)
		if err != nil {
			return fmt.Errorf("failed to read link: %s: %w", path, err)
		}

		if target != hdr.Linkname {
			if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
				return err
			}
			err = s.fs.Remove(path)
			if err != nil {
				return fmt.Errorf("failed to remove link: %s: %w", path, err)
			}
			fi = nil
		}
	}

	// add if doesn't exist
	if fi == nil {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		err := s.symlinker.SymlinkIfPossible(hdr.Linkname, path)
		if err != nil {
			return fmt.Errorf("failed to make link: %s: %w", path, err)
		}

		s.upd.Added = true
		s.upd.Link = ptr(hdr.Linkname)
		fi = nil
	}

	if err := s.syncStat(hdr, fi); err != nil {
		return fmt.Errorf("failed to sync stat: %w", err)
	}

	return nil
}

func (s *Sync) syncLink(hdr *tar.Header) error {
	path := normalizePath(hdr.Name)
	linkPath := normalizePath(hdr.Linkname)

	fi, _, err := LstatOrStat(s.fs, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("failed to stat: %s: %w", path, err)
	}

	fileInDisk := fi != nil

	if fileInDisk {
		targetFileInfo, _, err := LstatOrStat(s.fs, linkPath)
		if err != nil {
			return fmt.Errorf("failed to stat link target: %s: %w", path, err)
		}

		if fi.(FileInfoInoer).Ino() != targetFileInfo.(FileInfoInoer).Ino() {
			if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
				return err
			}

			if err = s.fs.RemoveAll(path); err != nil {
				return fmt.Errorf("failed to remove link: %s: %w", path, err)
			}

			fileInDisk = false
		}
	}

	if !fileInDisk {
		if err := s.preserveBaseDir(filepath.Dir(path)); err != nil {
			return err
		}

		if err := s.hardlinker.Link(linkPath, path); err != nil {
			return fmt.Errorf("failed to make link: %s: %w", path, err)
		}

		s.upd.Added = true
		fi = nil
	}

	if err := s.syncStat(hdr, fi); err != nil {
		return fmt.Errorf("failed to sync stat: %w", err)
	}

	return nil
}

func (s *Sync) syncStat(hdr *tar.Header, fi fs.FileInfo) error {
	path := normalizePath(hdr.Name)
	tarFileInfo := hdr.FileInfo()

	if fi == nil {
		var err error
		fi, _, err = LstatOrStat(s.fs, path)
		if err != nil {
			return fmt.Errorf("failed to stat: %s: %w", path, err)
		}
	}

	if s.opts.withOwnership {
		statOwner := fi.(FileInfoOwner)
		if hdr.Uid != statOwner.Uid() || hdr.Gid != statOwner.Gid() {
			var err error
			if hdr.Typeflag == tar.TypeSymlink {
				err = s.lchowner.Lchown(path, hdr.Uid, hdr.Gid)
			} else {
				err = s.fs.Chown(path, hdr.Uid, hdr.Gid)
			}
			if err != nil {
				return fmt.Errorf("failed to chown: %s: %w", path, err)
			}

			s.upd.Uid = ptr(hdr.Uid)
			s.upd.Gid = ptr(hdr.Gid)

			fi, _, err = LstatOrStat(s.fs, path)
			if err != nil {
				return fmt.Errorf("failed to stat: %s: %w", path, err)
			}
		}
	}

	// symlink mode permissions are not typically read, safest to ignore
	if hdr.Typeflag != tar.TypeSymlink && tarFileInfo.Mode() != fi.Mode() {
		err := s.fs.Chmod(path, tarFileInfo.Mode())
		if err != nil {
			return fmt.Errorf("failed to chmod: %s: %w", path, err)
		}

		s.upd.Mode = ptr(tarFileInfo.Mode())
	}

	if !hdr.ModTime.Equal(fi.ModTime()) {
		err := s.fs.Chtimes(path, hdr.ModTime, hdr.ModTime)
		if err != nil {
			return fmt.Errorf("failed to chtimes: %s: %w", path, err)
		}

		s.upd.ModTime = ptr(hdr.ModTime)
	}

	return nil
}

func (s *Sync) preserveBaseDir(baseDirPath string) error {
	// only execute when baseDirPath changes to save calls to guestfs
	if baseDirPath == s.baseDirPath {
		return nil
	}

	// restore last base's modtime
	if s.baseDirPath != "" {
		if err := s.fs.Chtimes(s.baseDirPath, s.baseDirModTime, s.baseDirModTime); err != nil {
			return fmt.Errorf("failed to preserve base dir modtime: %s: %w", s.baseDirPath, err)
		}
		s.baseDirPath = ""
	}

	// store this base's modtime
	if baseDirPath != "" {
		fi, _, err := LstatOrStat(s.fs, baseDirPath)
		if err != nil {
			return fmt.Errorf("failed to stat base dir: %s: %w", baseDirPath, err)
		}

		s.baseDirPath = baseDirPath
		s.baseDirModTime = fi.ModTime()
	}

	return nil
}

func normalizePath(path string) string {
	path = filepath.Clean(path)

	// turn absolute paths into local
	if path[0] == filepath.Separator {
		path = path[1:]
	}

	return path
}

func ptr[T any](t T) *T {
	return &t
}
