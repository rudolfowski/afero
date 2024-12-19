package s3fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/spf13/afero"
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
	// {"sub", true, true, dirSize, "", 0, ""},
	// {"sub/testDir2", true, true, dirSize, "", 0, ""},
	{"sub/testDir2/testFile", true, false, 8 * 1024, "c", 4 * 1024, "d"},
	// {"testFile", true, false, 12 * 1024, "a", 7 * 1024, "b"},
	// {"testDir1/testFile", true, false, 3 * 512, "b", 512, "c"},
	//
	// {"", false, true, dirSize, "", 0, ""}, // special case
	//
	// {"nonExisting", false, false, dirSize, "", 0, ""},
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
var s3Fs *S3Fs

func TestMain(m *testing.M) {
	ctx := context.Background()

	var err error

	// in order to respect deferring
	var exitCode int
	defer func() {
		os.Exit(exitCode)
	}()

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

	s3Fs, err = NewS3FS(ctx)
	if err != nil {
		panic(err)
	}

	s3Afs = &afero.Afero{Fs: s3Fs}

	exitCode = m.Run()

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

func Test_Open(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, f := range files {
		nameBase := filepath.Join(bucketName, f.name)
		names := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}
		if f.name == "" {
			names = []string{f.name}
		}

		for _, name := range names {
			file, err := s3Afs.Open(name)
			if (err == nil) != f.exists {
				t.Errorf("%v exists = %v, but got err = %v", name, f.exists, err)
			}

			if !f.exists {
				continue
			}
			if err != nil {
				t.Fatalf("%v: %v", name, err)
			}

			if file.Name() != filepath.FromSlash(nameBase) {
				t.Errorf("Name(), got %v, expected %v", file.Name(), filepath.FromSlash(nameBase))
			}

			s, err := file.Stat()
			if err != nil {
				t.Fatalf("stat %v: got error '%v'", file.Name(), err)
			}

			if isdir := s.IsDir(); isdir != f.isdir {
				t.Errorf("%v directory, got: %v, expected: %v", file.Name(), isdir, f.isdir)
			}

			if size := s.Size(); size != f.size {
				t.Errorf("%v size, got: %v, expected: %v", file.Name(), size, f.size)
			}
		}
	}
}

func Test_Read(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, f := range files {
		if !f.exists {
			continue
		}

		nameBase := filepath.Join(bucketName, f.name)

		names := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}
		if f.name == "" {
			names = []string{f.name}
		}

		for _, name := range names {
			file, err := s3Afs.Open(name)
			if err != nil {
				t.Fatalf("opening %v: %v", name, err)
			}

			buf := make([]byte, 8)
			n, err := file.Read(buf)
			if err != nil {
				if f.isdir && (err != syscall.EISDIR) {
					t.Errorf("%v got error %v, expected EISDIR", name, err)
				} else if !f.isdir {
					t.Errorf("%v: %v", name, err)
				}
			} else if n != 8 {
				t.Errorf("%v: got %d read bytes, expected 8", name, n)
			} else if string(buf) != strings.Repeat(f.content, testBytes) {
				t.Errorf("%v: got <%s>, expected <%s>", f.name, f.content, string(buf))
			}
		}
	}
}

func Test_ReadAt(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, f := range files {
		if !f.exists {
			continue
		}

		nameBase := filepath.Join(bucketName, f.name)

		names := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}
		if f.name == "" {
			names = []string{f.name}
		}

		for _, name := range names {
			file, err := s3Afs.Open(name)
			if err != nil {
				t.Fatalf("opening %v: %v", name, err)
			}

			buf := make([]byte, testBytes)
			n, err := file.ReadAt(buf, f.offset-testBytes/2)
			if err != nil {
				if f.isdir && (err != syscall.EISDIR) {
					t.Errorf("%v got error %v, expected EISDIR", name, err)
				} else if !f.isdir {
					t.Errorf("%v: %v", name, err)
				}
			} else if n != 8 {
				t.Errorf("%v: got %d read bytes, expected 8", f.name, n)
			} else if string(buf) != strings.Repeat(f.content, testBytes/2)+strings.Repeat(f.contentAtOffset, testBytes/2) {
				t.Errorf("%v: got <%s>, expected <%s>", f.name, f.contentAtOffset, string(buf))
			}
		}
	}
}

func Test_Readdir(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, d := range dirs {
		nameBase := filepath.Join(bucketName, d.name)

		names := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}

		for _, name := range names {
			dir, err := s3Afs.Open(name)
			if err != nil {
				t.Fatal(err)
			}

			fi, err := dir.Readdir(0)
			if err != nil {
				t.Fatal(err)
			}
			var fileNames []string
			for _, f := range fi {
				fileNames = append(fileNames, f.Name())
			}

			if !reflect.DeepEqual(fileNames, d.children) {
				t.Errorf("%v: children, got '%v', expected '%v'", name, fileNames, d.children)
			}

			fi, err = dir.Readdir(1)
			if err != nil {
				t.Fatal(err)
			}

			fileNames = []string{}
			for _, f := range fi {
				fileNames = append(fileNames, f.Name())
			}

			if !reflect.DeepEqual(fileNames, d.children[0:1]) {
				t.Errorf("%v: children, got '%v', expected '%v'", name, fileNames, d.children[0:1])
			}
		}
	}

	nameBase := filepath.Join(bucketName, "testFile")

	names := []string{
		nameBase,
		string(os.PathSeparator) + nameBase,
	}

	for _, name := range names {
		dir, err := s3Fs.Open(name)
		if err != nil {
			t.Fatal(err)
		}

		_, err = dir.Readdir(-1)
		if err != syscall.ENOTDIR {
			t.Fatal("Expected error")
		}
	}
}
