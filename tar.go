package aferosync

import (
	"archive/tar"
	"io"

	"github.com/spf13/afero"
)

type TarOuter interface {
	TarOut(dir string, w io.Writer) error
}

// WriteTar calls fs.WriteTar if implemented or falls back to tar.Writer.AddFS.
func TarOut(fsys afero.Fs, dir string, w io.Writer) error {
	if tw, ok := fsys.(TarOuter); ok {
		return tw.TarOut(dir, w)
	}

	tw := tar.NewWriter(w)
	defer tw.Close()
	return tw.AddFS(afero.NewIOFS(fsys))
}
