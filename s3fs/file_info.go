package s3fs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spf13/afero"
)

const folderSize = 42

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
		// If the object key is empty, it's a root folder
		// because getObj first check bucket and when find the bucket check key
		if errors.Is(err, ErrEmptyObjectKey) {
			fi.isDir = true
			fi.size = folderSize
			fi.name = EnsureTrailingSeparator(fi.name, fs.separator)
			return fi, nil
		}

		if errors.Is(err, afero.ErrFileNotFound) {

			// S3 doesn't have a concept of directories
			// so we check if the object is empty and has a trailing separator
			// to determine if it's a directory
			ok, err := fs.checkIfDirExists(name)
			if err != nil {
				return nil, err
			}

			if ok {
				fi.isDir = true
				fi.size = folderSize
				fi.name = EnsureTrailingSeparator(fi.name, fs.separator)
				return fi, nil
			}
			return nil, afero.ErrFileNotFound

		}

		return nil, err
	}

	fi.size = aws.ToInt64(obj.ContentLength)
	fi.updated = aws.ToTime(obj.LastModified)

	return fi, nil

}

func newFileInfoFromAttrs(attrs ObjectAttrs, fs *fs, fileMode os.FileMode) *FileInfo {
	fi := &FileInfo{
		name:     attrs.Key,
		size:     attrs.Size,
		updated:  attrs.LastModified,
		fileMode: fileMode,
		isDir:    false,
	}

	if attrs.IsDir {
		fi.isDir = true
		fi.size = folderSize
		fi.name = EnsureTrailingSeparator(fi.name, fs.separator)
	}
	return fi
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

type ByName []*FileInfo

func (a ByName) Len() int { return len(a) }
func (a ByName) Swap(i, j int) {
	a[i].name, a[j].name = a[j].name, a[i].name
	a[i].size, a[j].size = a[j].size, a[i].size
	a[i].updated, a[j].updated = a[j].updated, a[i].updated
	a[i].isDir, a[j].isDir = a[j].isDir, a[i].isDir
}
func (a ByName) Less(i, j int) bool { return strings.Compare(a[i].Name(), a[j].Name()) == -1 }
