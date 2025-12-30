package aferosync

import "fmt"

type Summary struct {
	Added   int
	Updated int
	Deleted int
	Errors  int
}

func (s *Summary) Add(upd Update) {
	if upd.Added {
		s.Added++
	} else if upd.Deleted {
		s.Deleted++
	} else if upd.Error != nil {
		s.Errors++
	} else {
		s.Updated++
	}
}

func (s Summary) String() string {
	ret := fmt.Sprintf("added: %d updated: %d deleted: %d", s.Added, s.Updated, s.Deleted)
	if s.Errors == 0 {
		return ret
	}

	return fmt.Sprintf("%s errors: %d", ret, s.Errors)
}
