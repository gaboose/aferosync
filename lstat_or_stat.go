package aferosync

import (
	"io/fs"

	"github.com/spf13/afero"
)

func LstatOrStat(fs afero.Fs, name string) (fs.FileInfo, bool, error) {
	if lstater, ok := fs.(afero.Lstater); ok {
		return lstater.LstatIfPossible(name)
	}

	fi, err := fs.Stat(name)
	return fi, false, err
}
