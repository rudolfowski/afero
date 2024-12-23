// Description: S3 file system implementation
// The S3 file system implementation is a wrapper around the S3 client from the AWS SDK for Go V2.
// It implements the afero.Fs interface.
// I inspired from the GCS file system implementation in the afero library.

package s3fs

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
)

var _ afero.Fs = (*S3Fs)(nil)

type S3Fs struct {
	source *fs
}

func NewS3FS(ctx context.Context, opts ...func(o *s3.Options)) (*S3Fs, error) {

	var cfg aws.Config
	var err error
	if jsonFile := os.Getenv("AWS_CONFIG_FILE"); jsonFile != "" {
		cfg, err = loadS3Config(ctx, jsonFile)
		if err != nil {
			return nil, err
		}
	} else {
		cfg, err = config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
	}

	return NewS3FSWithConfig(ctx, cfg, opts...), nil
}

func NewS3FSWithConfig(ctx context.Context, cfg aws.Config, opts ...func(o *s3.Options)) *S3Fs {
	client := s3.NewFromConfig(cfg, opts...)

	return &S3Fs{source: newFs(ctx, client)}
}

func NewS3FSWithClient(ctx context.Context, client *s3.Client) *S3Fs {
	return &S3Fs{
		source: newFs(ctx, client),
	}
}

func (s S3Fs) Create(name string) (afero.File, error) {
	return s.source.Create(name)
}

func (s S3Fs) Mkdir(name string, perm os.FileMode) error {
	return s.source.Mkdir(name, perm)
}

func (s S3Fs) MkdirAll(path string, perm os.FileMode) error {
	return s.source.MkdirAll(path, perm)
}

func (s S3Fs) Open(name string) (afero.File, error) {
	return s.source.Open(name)
}

func (s S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return s.source.OpenFile(name, flag, perm)
}

func (s S3Fs) Remove(name string) error {
	return s.source.Remove(name)
}

func (s S3Fs) RemoveAll(path string) error {
	return s.source.RemoveAll(path)
}

func (s S3Fs) Rename(oldname, newname string) error {
	return s.source.Rename(oldname, newname)
}

func (s S3Fs) Stat(name string) (os.FileInfo, error) {
	return s.source.Stat(name)
}

func (s S3Fs) Name() string {
	return "s3Fs"
}

func (s S3Fs) Chmod(name string, mode os.FileMode) error {
	panic("not implemented")
}

func (s S3Fs) Chown(name string, uid, gid int) error {
	panic("not implemented")
}

func (s S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	panic("not implemented")
}
