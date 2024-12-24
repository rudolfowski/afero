// Description: This file contains the tests for the s3fs package.
// I did test almost same like the gcsfs package.

package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
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
			if err != nil && errors.Is(err, afero.ErrFileNotFound) {
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

func Test_OpenRoot(t *testing.T) {
	file, err := s3Afs.Open("/")
	if err != nil {
		t.Fatalf("open root: %v", err)
	}

	s, err := file.Stat()
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}

	if !s.IsDir() {
		t.Fatal("root is not a directory")
	}

	names, err := file.Readdirnames(-1)
	if err != nil {
		t.Fatalf("readdirnames root: %v", err)
	}

	if len(names) == 0 {
		t.Fatal("root has no children")
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

func Test_Seek(t *testing.T) {
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

			tests := []struct {
				offIn  int64
				whence int
				offOut int64
			}{
				{0, io.SeekStart, 0},
				{10, io.SeekStart, 10},
				{1, io.SeekCurrent, 11},
				{10, io.SeekCurrent, 21},
				{0, io.SeekEnd, f.size},
				{-1, io.SeekEnd, f.size - 1},
			}

			for _, s := range tests {
				n, err := file.Seek(s.offIn, s.whence)
				if err != nil {
					if f.isdir && err == syscall.EISDIR {
						continue
					}

					t.Errorf("%v: %v", name, err)
				}

				if n != s.offOut {
					t.Errorf("%v: (off: %v, whence: %v): got %v, expected %v", f.name, s.offIn, s.whence, n, s.offOut)
				}
			}
		}

	}
}

func Test_Name(t *testing.T) {
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

			n := file.Name()
			if n != filepath.FromSlash(nameBase) {
				t.Errorf("got: %v, expected: %v", n, filepath.FromSlash(nameBase))
			}
		}

	}
}

func Test_Close(t *testing.T) {
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

			err = file.Close()
			if err != nil {
				t.Errorf("%v: %v", name, err)
			}

			err = file.Close()
			if err == nil {
				t.Errorf("%v: closing twice should return an error", name)
			}

			buf := make([]byte, 8)
			n, err := file.Read(buf)
			if n != 0 || err == nil {
				t.Errorf("%v: could read from a closed file", name)
			}

			n, err = file.ReadAt(buf, 256)
			if n != 0 || err == nil {
				t.Errorf("%v: could readAt from a closed file", name)
			}

			off, err := file.Seek(0, io.SeekStart)
			if off != 0 || err == nil {
				t.Errorf("%v: could seek from a closed file", name)
			}
		}
	}
}

func Test_OpenFile(t *testing.T) {
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
			file, err := s3Afs.OpenFile(name, os.O_RDONLY, 0o400)
			if !f.exists {
				if (f.name != "" && !errors.Is(err, afero.ErrFileNotFound)) ||
					(f.name == "" && !errors.Is(err, ErrFilenameEmpty)) {
					t.Errorf("%v: got %v, expected%v", name, err, afero.ErrFileNotFound)
				}

				continue
			}

			if err != nil {
				t.Fatalf("%v: %v", name, err)
			}

			err = file.Close()
			if err != nil {
				t.Fatalf("failed to close a file \"%s\": %s", name, err)
			}

			_, err = s3Afs.OpenFile(name, os.O_CREATE, 0o600)
			if !errors.Is(err, syscall.EPERM) {
				t.Errorf("%v: open for write: got %v, expected %v", name, err, syscall.EPERM)
			}
		}
	}
}

func Test_FsStat(t *testing.T) {
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
			fi, err := s3Afs.Stat(name)
			if !f.exists {
				if (f.name != "" && !errors.Is(err, afero.ErrFileNotFound)) ||
					(f.name == "" && !errors.Is(err, ErrFilenameEmpty)) {
					t.Errorf("%v: got %v, expected%v", name, err, afero.ErrFileNotFound)
				}

				continue
			}

			if err != nil {
				t.Fatalf("stat %v: got error '%v'", name, err)
			}

			if isdir := fi.IsDir(); isdir != f.isdir {
				t.Errorf("%v directory, got: %v, expected: %v", name, isdir, f.isdir)
			}

			if size := fi.Size(); size != f.size {
				t.Errorf("%v size, got: %v, expected: %v", name, size, f.size)
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

func Test_Readdirnames(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, d := range dirs {
		nameBase := filepath.Join(bucketName, d.name)

		names := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}

		for _, name := range names {
			dir, err := s3Fs.Open(name)
			if err != nil {
				t.Fatal(err)
			}

			fileNames, err := dir.Readdirnames(0)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(fileNames, d.children) {
				t.Errorf("%v: children, got '%v', expected '%v'", name, fileNames, d.children)
			}

			fileNames, err = dir.Readdirnames(1)
			if err != nil {
				t.Fatal(err)
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

		_, err = dir.Readdirnames(-1)
		if err != syscall.ENOTDIR {
			t.Fatal("Expected error")
		}
	}
}

func Test_Glob(t *testing.T) {
	createFiles(t)
	defer removeFiles(t)

	for _, s := range []struct {
		glob    string
		entries []string
	}{
		{filepath.FromSlash("*"), []string{filepath.FromSlash("sub"), filepath.FromSlash("testDir1"), filepath.FromSlash("testFile")}},
		{filepath.FromSlash("sub/*"), []string{filepath.FromSlash("sub/testDir2")}},
		{filepath.FromSlash("sub/testDir2/*"), []string{filepath.FromSlash("sub/testDir2/testFile")}},
		{filepath.FromSlash("testDir1/*"), []string{filepath.FromSlash("testDir1/testFile")}},
	} {
		nameBase := filepath.Join(bucketName, s.glob)

		prefixedGlobs := []string{
			nameBase,
			string(os.PathSeparator) + nameBase,
		}

		prefixedEntries := [][]string{{}, {}}
		for _, entry := range s.entries {
			prefixedEntries[0] = append(prefixedEntries[0], filepath.Join(bucketName, entry))
			prefixedEntries[1] = append(prefixedEntries[1], string(os.PathSeparator)+filepath.Join(bucketName, entry))
		}

		for i, prefixedGlob := range prefixedGlobs {
			entries, err := afero.Glob(s3Afs.Fs, prefixedGlob)
			if err != nil {
				t.Error(err)
			}
			if reflect.DeepEqual(entries, prefixedEntries[i]) {
				t.Logf("glob: %s: glob ok", prefixedGlob)
			} else {
				t.Errorf("glob: %s: got %#v, expected %#v", prefixedGlob, entries, prefixedEntries)
			}
		}
	}
}

func Test_Mkdir(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		emptyDirName := bucketName

		err := s3Afs.Mkdir(emptyDirName, 0o755)
		if err == nil {
			t.Fatal("did not fail upon creation of an empty folder")
		}
	})
	t.Run("success", func(t *testing.T) {
		dirName := filepath.Join(bucketName, "a-test-dir")
		var err error

		err = s3Afs.Mkdir(dirName, 0o755)
		if err != nil {
			t.Fatal("failed to create a folder with error", err)
		}

		info, err := s3Afs.Stat(dirName)
		if err != nil {
			t.Fatal("failed to get info", err)
		}
		if !info.IsDir() {
			t.Fatalf("%s: not a dir", dirName)
		}
		if !info.Mode().IsDir() {
			t.Errorf("%s: mode is not directory", dirName)
		}

		if info.Mode() != os.ModeDir|0o755 {
			t.Errorf("%s: wrong permissions, expected drwxr-xr-x, got %s", dirName, info.Mode())
		}

		err = s3Afs.Remove(dirName)
		if err != nil {
			t.Fatalf("could not delete the folder %s after the test with error: %s", dirName, err)
		}
	})
}

func Test_MkdirAll(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		emptyDirName := bucketName

		err := s3Afs.MkdirAll(emptyDirName, 0o755)
		if err == nil {
			t.Fatal("did not fail upon creation of an empty folder")
		}
	})
	t.Run("success", func(t *testing.T) {
		dirName := filepath.Join(bucketName, "a/b/c")

		err := s3Afs.MkdirAll(dirName, 0o755)
		if err != nil {
			t.Fatal(err)
		}

		info, err := s3Afs.Stat(filepath.Join(bucketName, "a"))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsDir() {
			t.Errorf("%s: mode is not directory", filepath.Join(bucketName, "a"))
		}
		if info.Mode() != os.ModeDir|0o755 {
			t.Errorf("%s: wrong permissions, expected drwxr-xr-x, got %s", filepath.Join(bucketName, "a"), info.Mode())
		}
		info, err = s3Afs.Stat(filepath.Join(bucketName, "a/b"))
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsDir() {
			t.Errorf("%s: mode is not directory", filepath.Join(bucketName, "a/b"))
		}
		if info.Mode() != os.ModeDir|0o755 {
			t.Errorf("%s: wrong permissions, expected drwxr-xr-x, got %s", filepath.Join(bucketName, "a/b"), info.Mode())
		}
		info, err = s3Afs.Stat(dirName)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsDir() {
			t.Errorf("%s: mode is not directory", dirName)
		}
		if info.Mode() != os.ModeDir|0o755 {
			t.Errorf("%s: wrong permissions, expected drwxr-xr-x, got %s", dirName, info.Mode())
		}

		err = s3Afs.RemoveAll(filepath.Join(bucketName, "a"))
		if err != nil {
			t.Fatalf("failed to remove the folder %s with error: %s", filepath.Join(bucketName, "a"), err)
		}
	})
}

func Test_RemoveAll(t *testing.T) {
	t.Run("non-existent", func(t *testing.T) {
		err := s3Afs.RemoveAll(filepath.Join(bucketName, "a"))
		if err != nil {
			t.Fatal("error should be nil when removing non-existent file")
		}
	})
	t.Run("success", func(t *testing.T) {
		aDir := filepath.Join(bucketName, "a")
		bDir := filepath.Join(aDir, "b")

		err := s3Afs.MkdirAll(bDir, 0o755)
		if err != nil {
			t.Fatal(err)
		}
		_, err = s3Afs.Stat(bDir)
		if err != nil {
			t.Fatal(err)
		}

		err = s3Afs.RemoveAll(aDir)
		if err != nil {
			t.Fatalf("failed to remove the folder %s with error: %s", aDir, err)
		}

		_, err = s3Afs.Stat(aDir)
		if err == nil {
			t.Fatalf("folder %s wasn't removed", aDir)
		}
	})
}

func Test_Rename(t *testing.T) {
	fileA := filepath.Join(bucketName, "a")
	fileB := filepath.Join(bucketName, "b")
	f, err := s3Afs.OpenFile(fileA, os.O_CREATE, 0o755)
	if err != nil {
		t.Fatalf("failed to create a file %s: %v", fileA, err)
	}
	_, err = f.Write([]byte("test"))
	if err != nil {
		t.Fatalf("failed to write to a file %s: %v", fileA, err)
	}
	f.Close()
	defer s3Afs.RemoveAll(bucketName)

	err = s3Afs.Rename(fileA, fileB)
	if err != nil {
		t.Fatalf("failed to rename a file %s to %s: %v", fileA, fileB, err)
	}
}

func Test_Truncate(t *testing.T) {
	file := filepath.Join(bucketName, "testFile")
	f, err := s3Afs.OpenFile(file, os.O_CREATE, 0o755)
	if err != nil {
		t.Fatalf("failed to create a file %s: %v", file, err)
	}
	defer f.Close()
	defer s3Afs.RemoveAll(bucketName)

	_, err = f.Write([]byte("test"))
	if err != nil {
		t.Fatalf("failed to write to a file %s: %v", file, err)
	}
	err = f.Truncate(2)
	if err != nil {
		t.Fatalf("failed to truncate a file %s: %v", file, err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to get file info %s: %v", file, err)
	}
	if fi.Size() != 2 {
		t.Fatalf("file %s size is not 2: %d", file, fi.Size())
	}

	err = f.Truncate(20)
	if err != nil {
		t.Fatalf("failed to truncate a file %s: %v", file, err)
	}
	fi, err = f.Stat()
	if err != nil {
		t.Fatalf("failed to get file info %s: %v", file, err)
	}
	if fi.Size() != 20 {
		t.Fatalf("file %s size is not 20: %d", file, fi.Size())
	}
}
