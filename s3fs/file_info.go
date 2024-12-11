package s3fs

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"os"
	"path/filepath"
	"time"
)

var _ os.FileInfo = (*FileInfo)(nil)

type FileInfo struct {
	name     string
	size     int64
	isDir    bool
	updated  time.Time
	fileMode os.FileMode
}

func newFileInfo(name string, fs *fs, fileMode os.FileMode) (*FileInfo, error) {
	fi := &FileInfo{
		name:     name,
		fileMode: fileMode,
		size:     0,
		isDir:    false,
		updated:  time.Time{},
	}

	obj, err := fs.getObj(name)
	if err != nil {
		return nil, err
	}

	fi.size = aws.ToInt64(obj.ContentLength)
	fi.updated = aws.ToTime(obj.LastModified)

	return fi, nil

}

func (f FileInfo) Name() string {
	return filepath.Base(filepath.FromSlash(f.name))
}

func (f FileInfo) Size() int64 {
	return f.size
}

func (f FileInfo) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | f.fileMode
	}
	return f.fileMode
}

func (f FileInfo) ModTime() time.Time {
	return f.updated
}

func (f FileInfo) IsDir() bool {
	return f.isDir
}

func (f FileInfo) Sys() any {
	return nil
}
