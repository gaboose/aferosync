package aferosync

import (
	"fmt"
	"io/fs"
	"strings"
	"time"
)

type PathUpdate struct {
	Path string
	Update
}

type Update struct {
	Added   bool
	Deleted bool
	Mode    *fs.FileMode
	Uid     *int
	Gid     *int
	ModTime *time.Time
	Link    *string
}

func (upd Update) IsEmpty() bool {
	return upd == Update{}
}

func (upd PathUpdate) String() string {
	if upd.Added {
		if upd.Link != nil {
			return "added " + upd.Path + " -> " + *upd.Link
		} else {
			return "added " + upd.Path
		}
	} else if upd.Deleted {
		return fmt.Sprintf("deleted %s", upd.Path)
	}

	parts := make([]string, 0, 6)
	parts = append(parts, "updated", upd.Path)

	if upd.Mode != nil {
		parts = append(parts, fmt.Sprintf("mode=%s", upd.Mode.String()))
	}
	if upd.Uid != nil {
		parts = append(parts, fmt.Sprintf("uid=%d", *upd.Uid))
	}
	if upd.Gid != nil {
		parts = append(parts, fmt.Sprintf("gid=%d", *upd.Gid))
	}
	if upd.ModTime != nil {
		parts = append(parts, fmt.Sprintf("modtime=%s", upd.ModTime.String()))
	}

	return strings.Join(parts, " ")
}
