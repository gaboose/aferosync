package aferosync

import "fmt"

type Summary struct {
	Added   int
	Updated int
	Deleted int
}

func (s *Summary) Add(upd Update) {
	if upd.Added {
		s.Added++
	} else if upd.Deleted {
		s.Deleted++
	} else {
		s.Updated++
	}
}

func (s Summary) String() string {
	return fmt.Sprintf("added: %d updated: %d deleted: %d", s.Added, s.Updated, s.Deleted)
}
