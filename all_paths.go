package aferosync

import (
	"fmt"
	"io/fs"

	"github.com/spf13/afero"
)

type AllPathser interface {
	AllPaths() ([]string, error)
}

// AllPaths calls afs.AllPaths() if implemented or falls back to afero.Walk.
func AllPaths(afs afero.Fs) ([]string, error) {
	if allPathser, ok := afs.(AllPathser); ok {
		return allPathser.AllPaths()
	}

	var paths []string
	if err := afero.Walk(afs, ".", func(path string, info fs.FileInfo, err error) error {
		paths = append(paths, path)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to walk fs: %w", err)
	}

	return paths, nil
}
