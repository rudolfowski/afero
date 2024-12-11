package s3fs

import (
	"context"
	"errors"
	"fmt"
	"github.com/spf13/afero"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const (
	testBytes = 8
	dirSize   = 42
)

var bucketName = "test-bucket"

var files = []struct {
	name            string
	exists          bool
	isdir           bool
	size            int64
	content         string
	offset          int64
	contentAtOffset string
}{
	{"sub", true, true, dirSize, "", 0, ""},
	{"sub/testDir2", true, true, dirSize, "", 0, ""},
	{"sub/testDir2/testFile", true, false, 8 * 1024, "c", 4 * 1024, "d"},
	{"testFile", true, false, 12 * 1024, "a", 7 * 1024, "b"},
	{"testDir1/testFile", true, false, 3 * 512, "b", 512, "c"},

	{"", false, true, dirSize, "", 0, ""}, // special case

	{"nonExisting", false, false, dirSize, "", 0, ""},
}

var dirs = []struct {
	name     string
	children []string
}{
	{"/", []string{"sub", "testDir1", "testFile"}},
	{"/sub", []string{"testDir2"}},
	{"/sub/testDir2", []string{"testFile"}},
	{"/testDir1", []string{"testFile"}},
}
var s3Afs *afero.Afero

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error

	// in order to respect deferring
	var exitCode int
	defer os.Exit(exitCode)

	defer func() {
		err := recover()
		if err != nil {
			fmt.Print(err)
			exitCode = 2
		}
	}()

	if err = os.Setenv("AWS_CONFIG_FILE", "s3_fake_config.json"); err != nil {
		panic(err)
	}
	defer func() {
		if err = os.Unsetenv("AWS_CONFIG_FILE"); err != nil {
			fmt.Printf("failed to remove AWS_CONFIG_FILE, err: %v", err)
		}
	}()

	s3Fs, err := NewS3FS(ctx)
	if err != nil {
		panic(err)
	}

	s3Afs = &afero.Afero{Fs: s3Fs}

	exitCode = m.Run()

}

func TestS3Fs_Open(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)
}

func createFiles(t *testing.T) {
	t.Helper()

	var err error
	for _, f := range files {
		if !f.isdir && f.exists {
			name := filepath.Join(bucketName, f.name)

			var file afero.File
			file, err = s3Afs.Create(name)
			if err != nil {
				t.Fatalf("failed to create file %s: %v", name, err)
			}
			var written int
			var totalWritten int64
			for totalWritten < f.size {
				if totalWritten < f.offset {
					writeBuf := []byte(strings.Repeat(f.content, int(f.offset)))
					written, err = file.WriteAt(writeBuf, totalWritten)
				} else {
					writeBuf := []byte(strings.Repeat(f.contentAtOffset, int(f.size-f.offset)))
					written, err = file.WriteAt(writeBuf, totalWritten)
				}
				if err != nil {
					t.Fatalf("failed to write a file \"%s\": %s", f.name, err)
				}

				totalWritten += int64(written)
			}
			if err = file.Close(); err != nil {
				t.Fatalf("failed to close file \"%s\": %s", f.name, err)
			}
		}
	}

}

func removeFiles(t *testing.T) {
	t.Helper()
	var err error

	// the files have to be created first
	for _, f := range files {
		if !f.isdir && f.exists {
			name := filepath.Join(bucketName, f.name)

			err = s3Afs.Remove(name)
			if err != nil && errors.Is(err, syscall.ENOENT) {
				t.Errorf("failed to remove file \"%s\": %s", f.name, err)
			}
		}
	}
}
