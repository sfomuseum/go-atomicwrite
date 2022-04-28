package atomicwrite

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
)

const HELLO_WORLD string = "Hello world"

func TestAtomicWritePath(t *testing.T) {

	tmpdir := os.TempDir()
	fname := "atomicwrite.txt"

	path := filepath.Join(tmpdir, fname)

	defer func() {
		err := os.Remove(path)

		if err != nil {
			t.Fatalf("Failed to remove %s, %v", path, err)
		}
	}()

	err := testAtomicWrite(path)

	if err != nil {
		t.Fatalf("Failed to write atomic file, %v", err)
	}

	r, err := os.Open(path)

	if err != nil {
		t.Fatalf("Failed to open %s, %v", path, err)
	}

	body, err := io.ReadAll(r)

	if err != nil {
		t.Fatalf("Failed to read %s, %v", path, err)
	}

	if !bytes.Equal(body, []byte(HELLO_WORLD)) {
		t.Fatalf("Invalid data (%s) written to %s", string(body), err)
	}
}

func TestAtomicWriteFile(t *testing.T) {

	tmpdir := os.TempDir()
	fname := "atomicwrite.txt"

	path := filepath.Join(tmpdir, fname)

	defer func() {
		err := os.Remove(path)

		if err != nil {
			t.Fatalf("Failed to remove %s, %v", path, err)
		}
	}()

	uri := fmt.Sprintf("file://%s", path)
	
	err := testAtomicWrite(uri)

	if err != nil {
		t.Fatalf("Failed to write atomic file, %v", err)
	}

	r, err := os.Open(path)

	if err != nil {
		t.Fatalf("Failed to open %s, %v", path, err)
	}

	body, err := io.ReadAll(r)

	if err != nil {
		t.Fatalf("Failed to read %s, %v", path, err)
	}

	if !bytes.Equal(body, []byte(HELLO_WORLD)) {
		t.Fatalf("Invalid data (%s) written to %s", string(body), err)
	}
}

func TestAtomicWriteMem(t *testing.T) {

	fname := "atomicwrite.txt"
	uri := fmt.Sprintf("mem://%s", fname)
	
	err := testAtomicWrite(uri)

	if err != nil {
		t.Fatalf("Failed to write atomic file, %v", err)
	}
}

func testAtomicWrite(uri string) error {

	ctx := context.Background()

	wr, err := New(ctx, uri)

	if err != nil {
		return fmt.Errorf("Failed to create writer, %v", err)
	}

	_, err = wr.Write([]byte(HELLO_WORLD))

	if err != nil {
		return fmt.Errorf("Failed to write bytes, %v", err)
	}

	err = wr.Close()

	if err != nil {
		return fmt.Errorf("Failed to close writer, %v", err)
	}

	return nil
}
