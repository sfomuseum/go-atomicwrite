# go-atomicwrite

Go package to implement an atomic io.WriteCloser instance backed by the gocloud.dev/blob package.

## Documentation

[![Go Reference](https://pkg.go.dev/badge/github.com/sfomuseum/go-atomicwrite.svg)](https://pkg.go.dev/github.com/sfomuseum/go-atomicwrite)

## Example

```
import (
	"context"
	"fmt"
	"github.com/sfomuseum/go-atomicwrite"
)

func main(){

	ctx := context.Background()
	
	fname := "atomicwrite.txt"
	uri := fmt.Sprintf("mem://%s", fname)

	wr, _ := atomicwrite.New(ctx, uri)
	wr.Write([]byte("Hello world"))
	wr.Close()
}
```

_Error handling omitted for the sake of brevity._


## See also

* https://pkg.go.dev/io#WriteCloser
* https://pkg.go.dev/gocloud.dev/blob