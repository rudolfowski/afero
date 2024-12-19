package s3fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/spf13/afero"
)

var (
	ErrFileReadonly = errors.New("file is readonly")
)

// DummyWriterAt is a dummy implementation of io.WriterAt
// It writes data do a buffer and then to the writer
// It can be used only with s3 downloader with 1 concurrency
type DummyWriterAt struct {
	w      io.Writer
	offset int64
	buf    []byte
	l      int64
}

func (dw *DummyWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	result := p
	pLen := len(result)

	return pLen, nil
}

func (dw *DummyWriterAt) Bytes() []byte {
	return dw.buf
}

func newDummyWriterAt(buf []byte, offset int64) *DummyWriterAt {
	return &DummyWriterAt{
		offset: offset,
		buf:    buf,
	}
}

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

var _ io.ReadCloser = (*reader)(nil)

type reader struct {
	fs *fs

	name        string
	offset      int64
	buf         []byte
	currentSize int64
	pos         int64

	wg sync.WaitGroup

	r io.ReadCloser
}

func newReader(fs *fs, name string, offset int64, size int64) *reader {
	return &reader{
		fs:     fs,
		name:   name,
		offset: offset,
		buf:    make([]byte, size-offset),
	}
}

func (r *reader) open() error {
	_, err := r.fs.DownloadObject(r.name, r)
	if err != nil {
		return err
	}
	return nil
}

func (r *reader) WriteAt(p []byte, _ int64) (int, error) {

	l := len(p)
	size := r.currentSize
	pLen := int64(l)
	expLen := size + pLen
	var pos int64
	if expLen > r.offset {
		diff := expLen - r.offset
		var buff []byte
		if diff >= pLen {
			buff = p
			pos = pLen
		} else {
			pos = pLen - diff
			buff = p[pos:]
		}

		copy(r.buf, buff)
		r.pos += pos
	}

	r.currentSize += pLen
	return l, nil

}

func (r *reader) Read(p []byte) (n int, err error) {
	if r.r == nil {
		if err := r.open(); err != nil {
			return 0, err
		}
	}

	return r.r.Read(p)
}

func (r *reader) Close() error {

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
		buffer := manager.NewWriteAtBuffer([]byte{})
		_, err := f.fs.DownloadObject(f.name, buffer)
		if err != nil {
			return 0, err
		}

		_, err = io.CopyN(w, bytes.NewBuffer(buffer.Bytes()), off)
		if err != nil {
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
	return f.ReadAt(p, f.offset)
}

// ReadAt reads len(p) bytes into p starting at the offset
// Reading when offset is greater than 0 its not supported in the s3
// so we have to download the object
// its very costly operation on large files if we have to read not from the beginning
func (f *S3File) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, afero.ErrFileClosed
	}

	if cap(p) == 0 {
		return 0, nil
	}

	// if the offset is the same as the current offset and the reader is not nil
	// we can read directly from the reader
	if off == f.offset && f.reader != nil {
		return f.reader.Read(p)
	}

	// we have to check if its not a directory
	if f.reader == nil && f.writer == nil {
		fi, err := f.Stat()
		if err != nil {
			return 0, err
		}

		if fi.IsDir() {
			return 0, syscall.EISDIR
		}
	}

	// if writers are open, close them
	// same for readers if they are open on different offsets
	if err = f.closeIo(); err != nil {
		return 0, err
	}

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

	r := newReader(f.fs, f.name, off, currentSize)

	f.reader = r
	f.offset = off

	read, err := r.Read(p)
	f.offset += int64(read)
	return read, err

}

func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (f *S3File) Name() string {
	return f.name
}

func (f *S3File) Readdir(count int) ([]os.FileInfo, error) {
	// we sync file before getting the file info
	err := f.Sync()
	if err != nil {
		return nil, err
	}

	// we get the file info
	// to check if the file is a directory
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		return nil, syscall.ENOTDIR
	}

	name := EnsureTrailingSeparator(f.name, f.fs.separator)
	attrs, err := f.fs.getObjectsAttrs(name)
	if err != nil {
		return nil, err
	}

	var res []os.FileInfo
	var fileInfos []*FileInfo

	// if there are no objects in the directory
	if len(attrs) == 0 {
		return res, io.EOF
	}

	for _, attr := range attrs {
		fileInfos = append(fileInfos, newFileInfoFromAttrs(attr, f.fs, DefaultFileMode))
	}

	// sort the file infos by name
	sort.Sort(ByName(fileInfos))

	if count > 0 {
		fileInfos = fileInfos[:count]
	}

	for _, f := range fileInfos {
		res = append(res, f)
	}

	return res, nil
}

func (f *S3File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil && err != io.EOF {
		return nil, err
	}

	names := make([]string, len(fi))
	for i, f := range fi {
		names[i] = f.Name()
	}

	return names, err
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
	return f.Write([]byte(s))
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
