package aferosync

import (
	"path/filepath"

	"github.com/spf13/afero"
)

type Walker interface {
	Walk(root string, walkFn filepath.WalkFunc) error
}

// Walk calls fs.Walk if implemented or falls back to afero.Walk.
func Walk(fs afero.Fs, root string, walkFn filepath.WalkFunc) error {
	if walker, ok := fs.(Walker); ok {
		return walker.Walk(root, walkFn)
	}

	return afero.Walk(fs, root, walkFn)
}
