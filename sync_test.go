package aferosync_test

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	aferoguestfs "github.com/gaboose/afero-guestfs"
	"github.com/gaboose/afero-guestfs/libguestfs.org/guestfs"
	"github.com/gaboose/aferosync"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemMapFs(t *testing.T) {
	afs := afero.NewMemMapFs()
	opts := []aferosync.Option{
		aferosync.WithSymlinks(false),  // afero.Symlinker
		aferosync.WithHardLinks(false), // aferosync.Linker
		aferosync.WithOwnership(false), // aferosync.FileInfoOwner
	}

	testRegularFileAdd(t, afs, opts...)
	testRegularFileDelete(t, afs, opts...)
	testRegularFileChmod(t, afs, opts...)
	// testRegularFileChown(t, afs, opts...) // ownership
	// testRegularFileChownSetgid(t, afs, opts...) // ownership
	testRegularFileOverwriteModTime(t, afs, opts...)
	testRegularFileOverwriteSize(t, afs, opts...)
	testRegularFileOverwriteDir(t, afs, opts...)
	// testRegularFileOverwriteSymlink(t, afs, opts...) // symlinks
	testRegularFileNoop(t, afs, opts...)
	// testRegularFileSyncSetgidChown(t, afs, opts...) //ownership

	testDirAdd(t, afs, opts...)
	testDirDelete(t, afs, opts...)
	testDirChmod(t, afs, opts...)
	// testDirChown(t, afs, opts...) // ownership
	testDirOverwriteRegularFile(t, afs, opts...)
	// testDirOverwriteSymlink(t, afs, opts...) // symlinks
	testDirModTime(t, afs, opts...)
	testDirNoop(t, afs, opts...)
	testDirPreserveModTime(t, afs, opts...)
	// testDirPreserveModTimeSymlink(t, afs, opts...) // symlinks
	// testDirPreserveModTimeHardLink(t, afs, opts...) // hard links

	// testSymlink(t, afs, opts...) // symlinks
	// testLink(t, afs, opts...) // hard links

	testSummary(t, afs, opts...)
}

func TestGuestFs(t *testing.T) {
	var afsClose func() error
	var err error
	afs, afsClose, err := newTestGuestFS()
	require.Nil(t, err)
	defer afsClose()

	testRegularFileAdd(t, afs)
	testRegularFileDelete(t, afs)
	testRegularFileChmod(t, afs)
	testRegularFileChown(t, afs)
	testRegularFileChownSetgid(t, afs)
	testRegularFileOverwriteModTime(t, afs)
	testRegularFileOverwriteSize(t, afs)
	testRegularFileOverwriteDir(t, afs)
	testRegularFileOverwriteSymlink(t, afs)
	testRegularFileNoop(t, afs)

	testDirAdd(t, afs)
	testDirDelete(t, afs)
	testDirChmod(t, afs)
	testDirChown(t, afs)
	testDirOverwriteRegularFile(t, afs)
	testDirOverwriteSymlink(t, afs)
	testDirModTime(t, afs)
	testDirNoop(t, afs)
	testDirPreserveModTime(t, afs)
	testDirPreserveModTimeSymlink(t, afs)
	testDirPreserveModTimeHardLink(t, afs)

	testSymlink(t, afs)
	testLink(t, afs)

	testSummary(t, afs)
}

func testRegularFileAdd(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Add", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Added:   true,
				Mode:    ptr(fs.ModePerm),
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileDelete(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Delete", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar(nil)
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Deleted: true,
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileChmod(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Chmod", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Mode: ptr(fs.ModePerm),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileChown(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Chown", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				Uid:     1000,
				Gid:     1000,
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Uid: ptr(1000),
				Gid: ptr(1000),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileChownSetgid(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	// Assert that setting setgid bit and changing group owner at the same time works.
	// Chown may reset the setgid bit. It will if the file has the group execute bit
	// set too and chown changes the group. Test for that not happening.
	t.Run("RegularFile/Chown/Setgid", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(posixMode(fs.ModePerm | fs.ModeSetgid)),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				Uid:     1000,
				Gid:     1001,
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Uid:  ptr(1000),
				Gid:  ptr(1001),
				Mode: ptr(fs.ModePerm | fs.ModeSetgid),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileOverwriteModTime(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Overwrite/ModTime", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(0777),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text2",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text1"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Added:   true,
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
				Mode:    ptr(fs.ModePerm),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileOverwriteSize(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Overwrite/Size", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text2",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Added:   true,
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
				Mode:    ptr(fs.ModePerm),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileOverwriteSymlink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Overwrite/Symlink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "test.txt")
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Added:   true,
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
				Mode:    ptr(fs.ModePerm),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileOverwriteDir(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Overwrite/Dir", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("test.txt", fs.ModePerm)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "test.txt",
			Update: aferosync.Update{
				Added:   true,
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
				Mode:    ptr(fs.ModePerm),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testRegularFileNoop(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("RegularFile/Noop", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afero.WriteFile(afs, "test.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)
		err = afs.Chtimes("test.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate(nil), updates)
		assertEqualTars(t, bts, afs)
	})
}

func testDirAdd(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Add", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0755,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Added:   true,
				Mode:    ptr(0755 | fs.ModeDir),
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirDelete(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Delete", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar(nil)
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Deleted: true,
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirChmod(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Chmod", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Mode: ptr(fs.ModePerm | fs.ModeDir),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirOverwriteRegularFile(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Overwrite/RegularFile", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0755,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		require.Nil(t, afero.WriteFile(afs, "etc", []byte("some text"), fs.ModePerm))

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Added:   true,
				Mode:    ptr(0755 | fs.ModeDir),
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirOverwriteSymlink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Overwrite/Symlink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0755,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		require.Nil(t, afs.(afero.Symlinker).SymlinkIfPossible("/target", "etc"))

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Added:   true,
				Mode:    ptr(0755 | fs.ModeDir),
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirChown(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Chown", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				Uid:     1000,
				Gid:     1000,
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				Uid: ptr(1000),
				Gid: ptr(1000),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirModTime(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/ModTime", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate{{
			Path: "etc",
			Update: aferosync.Update{
				ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
			},
		}}, updates)

		assertEqualTars(t, bts, afs)
	})
}

func testDirNoop(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/Noop", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		var updates []aferosync.PathUpdate
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
			updates = append(updates, sync.Update())
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, []aferosync.PathUpdate(nil), updates)
		assertEqualTars(t, bts, afs)
	})
}

func testDirPreserveModTime(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/PreserveModTime", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/var/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afero.WriteFile(afs, "etc/todelete", []byte("some more text"), fs.ModePerm)
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime().Local())

		assertEqualTars(t, bts, afs)
	})
}

func testDirPreserveModTimeSymlink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/PreserveModTime/Symlink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     "./etc/symlink",
				Linkname: "/target",
				Mode:     int64(fs.ModePerm),
				ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/RegularFileOverwriteSymlink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/test.txt",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "etc/test.txt")
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/RegularFileOverwriteDir", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/test.txt",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afs.Mkdir("etc/test.txt", fs.ModePerm)
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/DirOverwriteRegularFile", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/dir/",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afero.WriteFile(afs, "etc/dir", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/DirOverwriteSymlink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/dir/",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afs.(afero.Symlinker).SymlinkIfPossible("./target", "etc/dir")
		require.Nil(t, err)
		err = afs.Chtimes("etc/dir", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/SymlinkOverwriteDir", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     "./etc/symlink",
				Linkname: "/target",
				Mode:     int64(fs.ModePerm),
				ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afs.Mkdir("etc/symlink", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc/symlink", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})

	t.Run("Dir/PreserveModTime/SymlinkOverwriteRegularFile", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Typeflag: tar.TypeSymlink,
				Name:     "./etc/symlink",
				Linkname: "/target",
				Mode:     int64(fs.ModePerm),
				ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		afero.WriteFile(afs, "etc/symlink", []byte("some text"), 0644)
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})
}

func testDirPreserveModTimeHardLink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Dir/PreserveModTime/HardLink", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./etc/",
				Mode:    0644,
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		}, {
			Header: tar.Header{
				Name:    "./etc/test.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}, {
			Header: tar.Header{
				Typeflag: tar.TypeLink,
				Name:     "./etc/hardlink",
				Linkname: "./etc/test.txt",
				Mode:     int64(fs.ModePerm),
				ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		})
		require.Nil(t, err)

		// build disk
		err = afs.Mkdir("etc", 0644)
		require.Nil(t, err)
		err = afs.Chtimes("etc", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		// sync
		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		_, err = sync.Run()
		require.Nil(t, err)

		// assert
		etcFileInfo, err := afs.Stat("etc")
		assert.Nil(t, err)
		assert.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local(), etcFileInfo.ModTime())

		assertEqualTars(t, bts, afs)
	})
}

func testSymlink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Symlink", func(t *testing.T) {
		t.Run("Add", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Typeflag: tar.TypeSymlink,
					Name:     "./link",
					Linkname: "/target",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Added:   true,
					ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
					Link:    ptr("/target"),
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Delete", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar(nil)
			require.Nil(t, err)

			// build disk
			err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "link")
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Deleted: true,
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Chown", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Typeflag: tar.TypeSymlink,
					Name:     "./link",
					Linkname: "/target",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					Uid:      1000,
					Gid:      1000,
				},
			}})
			require.Nil(t, err)

			// build disk
			err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "link")
			require.Nil(t, err)
			err = afs.Chtimes("link", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Uid: ptr(1000),
					Gid: ptr(1000),
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("ModTime", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Typeflag: tar.TypeSymlink,
					Name:     "./link",
					Linkname: "/target",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "link")
			require.Nil(t, err)
			err = afs.Chtimes("link", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Overwrite", func(t *testing.T) {
			t.Run("Symlink", func(t *testing.T) {
				err := clear(afs)
				require.Nil(t, err)

				// build tar
				bts, err := newTar([]struct {
					Header tar.Header
					Body   string
				}{{
					Header: tar.Header{
						Typeflag: tar.TypeSymlink,
						Name:     "./link",
						Linkname: "/target2",
						Mode:     int64(fs.ModePerm),
						ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				}})
				require.Nil(t, err)

				// build disk
				err = afs.(afero.Symlinker).SymlinkIfPossible("/target1", "link")
				require.Nil(t, err)
				err = afs.Chtimes("link", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
				require.Nil(t, err)

				// sync
				var updates []aferosync.PathUpdate
				sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
				for sync.Next() {
					updates = append(updates, sync.Update())
				}
				require.Nil(t, sync.Err())

				// assert
				assert.Equal(t, []aferosync.PathUpdate{{
					Path: "link",
					Update: aferosync.Update{
						Added:   true,
						ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
						Link:    ptr("/target2"),
					},
				}}, updates)

				assertEqualTars(t, bts, afs)
			})

			t.Run("RegularFile", func(t *testing.T) {
				err := clear(afs)
				require.Nil(t, err)

				// build tar
				bts, err := newTar([]struct {
					Header tar.Header
					Body   string
				}{{
					Header: tar.Header{
						Typeflag: tar.TypeSymlink,
						Name:     "./link",
						Linkname: "/target2",
						Mode:     int64(fs.ModePerm),
						ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				}})
				require.Nil(t, err)

				// build disk
				err = afero.WriteFile(afs, "link", []byte("some text"), fs.ModePerm)
				require.Nil(t, err)
				err = afs.Chtimes("link", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
				require.Nil(t, err)

				// sync
				var updates []aferosync.PathUpdate
				sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
				for sync.Next() {
					updates = append(updates, sync.Update())
				}
				require.Nil(t, sync.Err())

				// assert
				assert.Equal(t, []aferosync.PathUpdate{{
					Path: "link",
					Update: aferosync.Update{
						Added:   true,
						ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
						Link:    ptr("/target2"),
					},
				}}, updates)

				assertEqualTars(t, bts, afs)
			})

			t.Run("Dir", func(t *testing.T) {
				err := clear(afs)
				require.Nil(t, err)

				// build tar
				bts, err := newTar([]struct {
					Header tar.Header
					Body   string
				}{{
					Header: tar.Header{
						Typeflag: tar.TypeSymlink,
						Name:     "./link",
						Linkname: "/target2",
						Mode:     int64(fs.ModePerm),
						ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				}})
				require.Nil(t, err)

				// build disk
				err = afs.Mkdir("link", fs.ModePerm)
				require.Nil(t, err)
				err = afs.Chtimes("link", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
				require.Nil(t, err)

				// sync
				var updates []aferosync.PathUpdate
				sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
				for sync.Next() {
					updates = append(updates, sync.Update())
				}
				require.Nil(t, sync.Err())

				// assert
				assert.Equal(t, []aferosync.PathUpdate{{
					Path: "link",
					Update: aferosync.Update{
						Added:   true,
						ModTime: ptr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Local()),
						Link:    ptr("/target2"),
					},
				}}, updates)

				assertEqualTars(t, bts, afs)
			})
		})

		t.Run("Noop", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Typeflag: tar.TypeSymlink,
					Name:     "./link",
					Linkname: "/target",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			err = afs.(afero.Symlinker).SymlinkIfPossible("/target", "link")
			require.Nil(t, err)
			err = afs.Chtimes("link", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate(nil), updates)
			assertEqualTars(t, bts, afs)
		})
	})
}

func testLink(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Link", func(t *testing.T) {
		t.Run("Add", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}, {
				Header: tar.Header{
					Typeflag: tar.TypeLink,
					Name:     "./link",
					Linkname: "./atarget",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			err = afero.WriteFile(afs, "atarget", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Added: true,
				},
			}}, updates)

			linkfi, err := afs.Stat("link")
			assert.Nil(t, err)
			targetfi, err := afs.Stat("atarget")
			assert.Nil(t, err)
			assert.Equal(t, targetfi.(aferosync.FileInfoInoer).Ino(), linkfi.(aferosync.FileInfoInoer).Ino())

			assertEqualTars(t, bts, afs)
		})

		t.Run("Delete", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}})
			require.Nil(t, err)

			// build disk
			afero.WriteFile(afs, "atarget", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			afs.Chtimes("atarget", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			err = afs.(aferosync.Linker).Link("/atarget", "link")
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Deleted: true,
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Overwrite/Link", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget1",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}, {
				Header: tar.Header{
					Name:    "./atarget2",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some more text",
			}, {
				Header: tar.Header{
					Typeflag: tar.TypeLink,
					Name:     "./link",
					Linkname: "./atarget2",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			afero.WriteFile(afs, "atarget1", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget1", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)
			afero.WriteFile(afs, "atarget2", []byte("some more text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget2", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)
			err = afs.(aferosync.Linker).Link("/atarget1", "link")
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Added: true,
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Overwrite/Dir", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}, {
				Header: tar.Header{
					Typeflag: tar.TypeLink,
					Name:     "./link",
					Linkname: "./atarget",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			afero.WriteFile(afs, "atarget", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)
			err = afs.Mkdir("link", fs.ModePerm)
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Added: true,
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Overwrite/Symlink", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}, {
				Header: tar.Header{
					Typeflag: tar.TypeLink,
					Name:     "./link",
					Linkname: "./atarget",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			afero.WriteFile(afs, "atarget", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)
			err = afs.(afero.Symlinker).SymlinkIfPossible("/atarget2", "link")
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate{{
				Path: "link",
				Update: aferosync.Update{
					Added: true,
				},
			}}, updates)

			assertEqualTars(t, bts, afs)
		})

		t.Run("Noop", func(t *testing.T) {
			err := clear(afs)
			require.Nil(t, err)

			// build tar
			bts, err := newTar([]struct {
				Header tar.Header
				Body   string
			}{{
				Header: tar.Header{
					Name:    "./atarget",
					Mode:    int64(fs.ModePerm),
					ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Body: "some text",
			}, {
				Header: tar.Header{
					Typeflag: tar.TypeLink,
					Name:     "./link",
					Linkname: "./atarget",
					Mode:     int64(fs.ModePerm),
					ModTime:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			}})
			require.Nil(t, err)

			// build disk
			err = afero.WriteFile(afs, "atarget", []byte("some text"), fs.ModePerm)
			require.Nil(t, err)
			err = afs.Chtimes("atarget", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
			require.Nil(t, err)
			err = afs.(aferosync.Linker).Link("/atarget", "link")
			require.Nil(t, err)

			// sync
			var updates []aferosync.PathUpdate
			sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
			for sync.Next() {
				updates = append(updates, sync.Update())
			}
			require.Nil(t, sync.Err())

			// assert
			assert.Equal(t, []aferosync.PathUpdate(nil), updates)
			assertEqualTars(t, bts, afs)
		})
	})
}

func testSummary(t *testing.T, afs afero.Fs, opts ...aferosync.Option) {
	t.Run("Summary", func(t *testing.T) {
		err := clear(afs)
		require.Nil(t, err)

		// build tar
		bts, err := newTar([]struct {
			Header tar.Header
			Body   string
		}{{
			Header: tar.Header{
				Name:    "./test1.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}, {
			Header: tar.Header{
				Name:    "./test4.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}, {
			Header: tar.Header{
				Name:    "./test5.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}, {
			Header: tar.Header{
				Name:    "./test6.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}, {
			Header: tar.Header{
				Name:    "./test7.txt",
				Mode:    int64(fs.ModePerm),
				ModTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			Body: "some text",
		}})
		require.Nil(t, err)

		// sync
		err = afero.WriteFile(afs, "test2.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)
		err = afero.WriteFile(afs, "test3.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)
		err = afero.WriteFile(afs, "test4.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test4.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afero.WriteFile(afs, "test5.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test5.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afero.WriteFile(afs, "test6.txt", []byte("some text"), 0644)
		require.Nil(t, err)
		err = afs.Chtimes("test6.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)
		err = afero.WriteFile(afs, "test7.txt", []byte("some text"), fs.ModePerm)
		require.Nil(t, err)
		err = afs.Chtimes("test7.txt", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Nil(t, err)

		sync := aferosync.New(afs, tar.NewReader(bytes.NewBuffer(bts)), opts...)
		for sync.Next() {
		}
		require.Nil(t, sync.Err())

		// assert
		assert.Equal(t, aferosync.Summary{
			Added:   1,
			Deleted: 2,
			Updated: 3,
		}, sync.Summary())

		assertEqualTars(t, bts, afs)
	})
}

func newTestGuestFS() (afs *aferoguestfs.Fs, closeFn func() error, err error) {
	const size int64 = 4 * 1024 * 1024

	f, err := os.CreateTemp("", "guestfs-*.img")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp file failed: %w", err)
	}
	tmpPath := f.Name()
	closeFn = func() error {
		return os.Remove(tmpPath)
	}

	if cerr := f.Close(); cerr != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("close temp file failed: %w", cerr)
	}

	if err := os.Truncate(tmpPath, size); err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("truncate temp file failed: %w", err)
	}

	g, err := guestfs.Create()
	if err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("guestfs create failed: %w", err)
	}
	closeFn = func() error {
		if err := g.Close(); err != nil {
			return err
		}
		return os.Remove(tmpPath)
	}

	if err := g.Add_drive(tmpPath, nil); err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("add drive failed: %w", err)
	}

	if err := g.Launch(); err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("launch failed: %w", err)
	}

	devices, err := g.List_devices()
	if err != nil || len(devices) == 0 {
		_ = closeFn()
		return nil, nil, fmt.Errorf("no devices found: %w", err)
	}
	device := devices[0]

	if err := g.Mkfs("ext4", device, nil); err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("mkfs failed: %w", err)
	}

	if err := g.Mount(device, "/"); err != nil {
		_ = closeFn()
		return nil, nil, fmt.Errorf("mount failed: %w", err)
	}
	closeFn = func() error {
		if err := g.Umount_all(); err != nil {
			return err
		}
		if err := g.Close(); err != nil {
			return err
		}
		return os.Remove(tmpPath)
	}

	return aferoguestfs.New(g), closeFn, nil
}

func newTar(files []struct {
	Header tar.Header
	Body   string
}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	tarWriter := tar.NewWriter(buf)

	for _, tf := range files {
		tf.Header.Size = int64(len(tf.Body))
		if err := tarWriter.WriteHeader(&tf.Header); err != nil {
			return nil, fmt.Errorf("failed to write header: %s: %w", tf.Header.Name, err)
		}

		if len(tf.Body) > 0 {
			if _, err := io.Copy(tarWriter, bytes.NewBufferString(tf.Body)); err != nil {
				return nil, fmt.Errorf("failed to write file: %s: %w", tf.Header.Name, err)
			}
		}
	}

	if err := tarWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

func readFullTar(bts []byte) ([]struct {
	Header tar.Header
	Body   string
}, error) {
	ret := []struct {
		Header tar.Header
		Body   string
	}{}

	tarReader := tar.NewReader(bytes.NewBuffer(bts))
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read next: %w", err)
		}

		body := bytes.NewBuffer(nil)
		_, err = io.Copy(body, tarReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %s: %w", hdr.Name, err)
		}

		ret = append(ret, struct {
			Header tar.Header
			Body   string
		}{
			Header: *hdr,
			Body:   body.String(),
		})
	}

	return ret, nil
}

func assertEqualTars(t *testing.T, expectedTarBytes []byte, afs afero.Fs) {
	t.Helper()

	actualTarBuf := bytes.NewBuffer(nil)
	require.Nil(t, aferosync.TarOut(afs, ".", actualTarBuf))

	actualFiles, err := readFullTar(actualTarBuf.Bytes())
	require.Nil(t, err)

	expectedFiles, err := readFullTar(expectedTarBytes)
	require.Nil(t, err)

	normalize := func(files *[]struct {
		Header tar.Header
		Body   string
	}) {
		for i := 0; i < len(*files); i++ {
			// Remove leading dot-slahes to normalize.
			// GuestFs.TarOut produces them while MemMapFs with tar.Writer.AddFS
			// doesn't.
			(*files)[i].Header.Name = filepath.Clean((*files)[i].Header.Name)
			(*files)[i].Header.Linkname = filepath.Clean((*files)[i].Header.Linkname)

			// ignore the root folder
			if (*files)[i].Header.Name == "." {
				(*files) = append((*files)[:i], (*files)[i+1:]...)
				i--
				continue
			}

			// ignore user and group names
			(*files)[i].Header.Uname = ""
			(*files)[i].Header.Gname = ""

			// ignore format
			(*files)[i].Header.Format = 0

			// normalize hard links (link to the alphabetically first path)
			if (*files)[i].Header.Typeflag == tar.TypeLink {
				if (*files)[i].Header.Name < (*files)[i].Header.Linkname {
					for j, target := range (*files)[:i] {
						if (*files)[i].Header.Linkname == target.Header.Name {
							(*files)[j].Header.Name = (*files)[i].Header.Name
							(*files)[i].Header.Name = (*files)[i].Header.Linkname
							(*files)[i].Header.Linkname = (*files)[j].Header.Name
						}
					}
				}
			}
		}

		sort.Slice(*files, func(i, j int) bool {
			return (*files)[i].Header.Name < (*files)[j].Header.Name
		})
	}

	normalize(&expectedFiles)
	normalize(&actualFiles)
	assert.Equal(t, expectedFiles, actualFiles)
}

func clear(afs afero.Fs) error {
	root, err := afs.Open("/")
	if err != nil {
		return fmt.Errorf("failed to open root: %w", err)
	}

	dirnames, err := root.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("failed to read dir names: %w", err)
	}

	for _, dirname := range dirnames {
		err = afs.RemoveAll(dirname)
		if err != nil {
			return fmt.Errorf("failed to remove %s: %w", dirname, err)
		}
	}

	return nil
}

func ptr[T any](t T) *T {
	return &t
}

// reference: /usr/local/go/src/os/file_posix.go
func posixMode(i os.FileMode) (o uint32) {
	o |= uint32(i.Perm())
	if i&os.ModeSetuid != 0 {
		o |= syscall.S_ISUID
	}
	if i&os.ModeSetgid != 0 {
		o |= syscall.S_ISGID
	}
	if i&os.ModeSticky != 0 {
		o |= syscall.S_ISVTX
	}
	return
}
