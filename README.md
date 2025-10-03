# Sync Utility for Afero

# How to Use

```go
sync := aferosync.New(fsys, tarReader)
for sync.Next() {
    fmt.Println(sync.Update())
}
if sync.Err() != nil {
	// ...
}
```

### Sync Podman Image Example

```go
package main

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"os/exec"

	aferoguestfs "github.com/gaboose/afero-guestfs"
	"github.com/gaboose/aferosync"
)

func main() {
	fsys, err := aferoguestfs.OpenPartitionFs("disk.img", "/dev/sda2")
	if err != nil {
		panic(err)
	}

	cmd := exec.Command("podman", "unshare", "bash", "-c", "tar cC $(podman image mount alpine) .")
	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = os.Stderr

	go func() {
		pr.CloseWithError(cmd.Run())
	}()

	sync := aferosync.New(fsys, tar.NewReader(pr))
	for sync.Next() {
		fmt.Println(sync.Update())
	}
	if sync.Err() != nil {
		panic(sync.Err())
	}
}
```
