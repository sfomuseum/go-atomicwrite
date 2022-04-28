// package atomicwrite implements an atomic io.WriteCloser instance backed by the gocloud.dev/blob package.
package atomicwrite

import (
	"context"
	"fmt"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	"io"
	"log"
	"math/rand"
	"net/url"
	"path/filepath"
	"strings"
)

// type AtomicWriter implements an atomic io.WriteCloser instance backed by the gocloud.dev/blob package.
type AtomicWriter struct {
	io.WriteCloser
	// The underlying blob.Bucket instance where data is written
	bucket      *blob.Bucket
	// The underlying io.WriteCloser instance for writing data
	writer      io.WriteCloser
	// The final path (relative to bucket) that data will be written to
	final_path  string
	// The temporary path (relative to bucket) that data will be written to before writes are commited to final_path
	atomic_path string
}

// New returns a new AtomicWriter instance. 'uri' is expected to a valid gocloud.dev/blob URI however if 'uri' is passed
// in as a schema-less Unix-style path it will be converted to a gocloud.dev/blob `file://` URI. Under the hood this method
// will attempt to create a new temporary file for the "path" element of URI whose filename will be appended with a random
// string. This temporary file is where data will be written to until the `Close` method is invoked at which point the data
// in the temporary file will be copied to the final path (defined by 'uri') and the temporary file will be removed. This
// method will create a new `blob.Writer` instance (which implements `io.WriteCloser`) with the default nil `blob.WriterOptions`.
// If you need to specify custom writer options you should use the `NewWithOptions` method.
func New(ctx context.Context, uri string) (io.WriteCloser, error) {
	return NewWithOptions(ctx, uri, nil)
}

// NewWithOptions returns a new AtomicWriter instance, specifying 'writer_opts' as the custom options used to create the
// underlying `blob.Writer` instance.
func NewWithOptions(ctx context.Context, uri string, writer_opts *blob.WriterOptions) (io.WriteCloser, error) {	

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to parse URI, %w", err)
	}

	var bucket_uri string
	var final_path string
	var atomic_path string

	if u.Scheme == "" {

		abs_path, err := filepath.Abs(uri)

		if err != nil {
			return nil, fmt.Errorf("Failed to derive absolute path for URI, %w", err)
		}

		root := filepath.Dir(abs_path)
		fname := filepath.Base(abs_path)

		bucket_uri = fmt.Sprintf("file://%s", root)
		final_path = fname

	} else {

		root := filepath.Dir(uri)
		fname := filepath.Base(uri)

		bucket_uri = root
		final_path = fname
	}

	bucket, err := blob.OpenBucket(ctx, bucket_uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to open bucket %s, %w", bucket_uri, err)
	}

	ext := filepath.Ext(final_path)

	max_tries := 15

	for i := 0; i < max_tries; i++ {

		r := rand.Int()

		new_ext := fmt.Sprintf("-%d%s", r, ext)
		test_path := strings.Replace(final_path, ext, new_ext, 1)

		exists, err := bucket.Exists(ctx, test_path)

		if err != nil {
			return nil, fmt.Errorf("Failed to determine whether %s exists, %w", test_path, err)
		}

		if !exists {
			atomic_path = test_path
			break
		}
	}

	wr, err := bucket.NewWriter(ctx, atomic_path, writer_opts)

	if err != nil {
		return nil, fmt.Errorf("Failed to open %s, %w", atomic_path, err)
	}

	aw := &AtomicWriter{
		bucket:      bucket,
		writer:      wr,
		atomic_path: atomic_path,
		final_path:  final_path,
	}

	return aw, nil
}

// Write writes 'b' to the underlying writer instance.
func (aw *AtomicWriter) Write(b []byte) (int, error) {
	return aw.writer.Write(b)
}

// Close will copy data written to the intermediate temporary file to the final path defined in the
// `New` constructor. Upon successfully completing this operation the temporary file will be removed.
func (aw *AtomicWriter) Close() error {

	ctx := context.Background()

	err := aw.writer.Close()

	if err != nil {
		return fmt.Errorf("Failed to close atomic writer, %w", err)
	}

	r, err := aw.bucket.NewReader(ctx, aw.atomic_path, nil)

	if err != nil {
		return fmt.Errorf("Failed to open atomic reader, %w", err)
	}

	defer func() {

		r.Close()

		err := aw.bucket.Delete(ctx, aw.atomic_path)

		if err != nil {
			log.Printf("Failed to delete %s, %v", aw.atomic_path, err)
		}
	}()

	wr, err := aw.bucket.NewWriter(ctx, aw.final_path, nil)

	if err != nil {
		return fmt.Errorf("Failed to open %s for writing, %w", aw.final_path, err)
	}

	_, err = io.Copy(wr, r)

	if err != nil {
		return fmt.Errorf("Failed to copy atomic file %s, %w", aw.final_path, err)
	}

	err = wr.Close()

	if err != nil {
		return fmt.Errorf("Failed to close %s, %w", aw.final_path, err)
	}

	return nil
}
