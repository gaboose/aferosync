package aferosync

type Lchowner interface {
	Lchown(name string, uid, gid int) error
}

type Linker interface {
	Link(oldname, newname string) error
}

type FileInfoOwner interface {
	Uid() int
	Gid() int
}

type FileInfoInoer interface {
	Ino() int
}
