package s3fs

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/afero"
	"io"
	"os"
	"sync"
)

var (
	ErrFileReadonly = errors.New("file is readonly")
)

var _ io.WriteCloser = (*writer)(nil)

type writer struct {
	fs   *fs
	name string
	w    *io.PipeWriter
	r    *io.PipeReader
	wg   sync.WaitGroup

	err error
}

func newWriter(fs *fs, name string) *writer {
	return &writer{
		fs:   fs,
		wg:   sync.WaitGroup{},
		name: name,
	}
}

func (w *writer) open() error {
	w.r, w.w = io.Pipe()
	w.wg.Add(1)

	go func() {
		defer w.wg.Done()
		if _, err := w.fs.uploadObject(w.name, w.r); err != nil {
			w.err = err
		}

		if err := w.r.Close(); err != nil {
			fmt.Printf("error closing pipe reader: %v", err)
		}

	}()

	return nil

}

func (w *writer) Write(p []byte) (int, error) {
	if w.w == nil && w.r == nil {
		if err := w.open(); err != nil {
			return 0, err
		}
	}

	n, err := w.w.Write(p)
	if w.err != nil {
		return 0, w.err
	}

	return n, err

}

func (w *writer) Close() error {
	if w.err != nil {
		return w.err
	}

	if w.w != nil && w.r != nil {
		err := w.w.Close()
		w.wg.Wait()

		if w.err != nil {
			return w.err
		}
		return err
	}
	return nil
}

var _ afero.File = (*S3File)(nil)

type S3File struct {
	fs     *fs
	name   string
	closed bool

	flags int

	offset int64
	size   int64

	writer io.WriteCloser
	reader io.ReadCloser
}

func newS3File(fs *fs, name string) *S3File {
	return &S3File{
		fs:     fs,
		name:   name,
		closed: false,
	}
}

func (f *S3File) Close() error {
	if f.closed {
		return afero.ErrFileClosed
	}
	f.closed = true

	return f.closeIo()
}

func (f *S3File) Write(p []byte) (int, error) {
	return f.WriteAt(p, f.offset)
}

func (f *S3File) WriteAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, afero.ErrFileClosed
	}

	if f.flags&os.O_RDONLY != 0 {
		return 0, ErrFileReadonly
	}

	ok, err := f.fs.isObjectExist(f.name)
	if err != nil {
		return 0, err
	}

	if !ok {
		return 0, afero.ErrFileNotFound
	}

	// if the offset is the same as the current offset and the writer is not nil
	// we can write directly to the writer
	if off == f.offset && f.writer != nil {
		n, err := f.writer.Write(p)
		f.offset += int64(n)
		return n, err

	}

	// if readers are open, close them
	// same for writers if they are open on different offsets
	if err = f.closeIo(); err != nil {
		return 0, err
	}

	// open a new writer
	// it downloads again the object from the bucket
	// and can't be avoided because we can't seek on remote objects

	w := newWriter(f.fs, f.name)

	// get the object attributes
	// we need to know the size of the object
	currentSize := int64(0)
	h, err := f.fs.getObj(f.name)
	if err != nil {
		if off > 0 {
			return 0, err
		}
	}

	currentSize = aws.ToInt64(h.ContentLength)

	if off > currentSize {
		return 0, afero.ErrOutOfRange
	}

	// now we can write the data to the pipe
	// It will write the data to the object
	if off > 0 {
		// if the offset is greater than 0
		// we need to read the object and write it to the pipe
		// until the offset
		// then we can write the new data
		r, err := f.fs.downloadObject(f.name)
		if err != nil {
			return 0, err
		}

		_, err = io.CopyN(w, r.Body, off)
		if err != nil {
			return 0, err
		}
		if err := r.Body.Close(); err != nil {
			return 0, err
		}
	}

	f.writer = w
	f.offset = off

	n, err := w.Write(p)
	f.offset += int64(n)

	return n, err

}
func (f *S3File) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Name() string {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Readdir(count int) ([]os.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Readdirnames(n int) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Stat() (os.FileInfo, error) {
	// we sync file before getting the file info
	// to make sure that the file is up to date
	if err := f.Sync(); err != nil {
		return nil, err
	}
	return newFileInfo(f.name, f.fs, DefaultFileMode)
}

func (f *S3File) Sync() error {
	return f.closeIo()
}

func (f *S3File) Truncate(size int64) error {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) WriteString(s string) (ret int, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) closeIo() error {
	if f.reader != nil {
		if err := f.reader.Close(); err != nil {
			return err
		}
	}

	if f.writer != nil {
		if err := f.writer.Close(); err != nil {
			return err
		}
	}

	return nil
}
