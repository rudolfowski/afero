package s3fs

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	ErrFilenameEmpty = errors.New("filename is empty")
)

type fs struct {
	ctx      context.Context
	client   *s3.Client
	uploader *manager.Uploader
	log      *slog.Logger

	separator string
}

func newFs(ctx context.Context, client *s3.Client) *fs {

	return &fs{
		ctx:       ctx,
		client:    client,
		uploader:  manager.NewUploader(client),
		separator: "/",
	}
}

func (f *fs) Create(name string) (*S3File, error) {
	name, err := f.parseName(name)
	if err != nil {
		return nil, err
	}

	ok, err := f.isObjectExist(name)
	if err != nil {
		return nil, err
	}

	if ok {
		return nil, afero.ErrFileExists
	}

	_, err = f.uploadObject(name, strings.NewReader(""))
	if err != nil {
		return nil, err
	}

	return newS3File(f, name), nil

}

func (f *fs) Open(name string) (*S3File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *fs) OpenFile(name string, flag int, perm os.FileMode) (*S3File, error) {
	name, err := f.parseName(name)
	if err != nil {
		return nil, err
	}

	file := newS3File(f, name)
	if flag == os.O_RDONLY {
		// we just want to read the file
		// so we get file info to check if the file exists
		_, err = file.Stat()
		if err != nil {
			return nil, err
		}
	}

	if flag&os.O_TRUNC != 0 {
		// we want to truncate the file
		// so we delete the file
		err = f.deleteObject(name)
		if err != nil {
			return nil, err
		}
		return f.Create(name)
	}

	if flag&os.O_APPEND != 0 {
		// we want to append to the file
		// so we seek to the end of the file
		_, err = file.Seek(0, 2)
		if err != nil {
			return nil, err
		}
	}

	if flag&os.O_CREATE != 0 {
		// we want to create the file
		// so we check if the file exists
		_, err = file.Stat()
		if err == nil {
			// the file actually exists
			return nil, afero.ErrFileExists
		}

		if _, err = file.WriteString(""); err != nil {
			return nil, err
		}
	}
	return file, nil
}

func (f *fs) Remove(name string) error {
	name, err := f.parseName(name)
	if err != nil {
		return err

	}

	fi, err := f.Stat(name)
	if err != nil {
		return err
	}

	if fi.IsDir() {

	}

	return f.deleteObject(name)
}

func (f *fs) Rename(oldname, newname string) error {
	return nil
}

func (f *fs) Stat(name string) (os.FileInfo, error) {
	return newFileInfo(name, f, DefaultFileMode)
}

func (f *fs) parseName(name string) (string, error) {
	if name == "" {
		return "", ErrFilenameEmpty
	}
	s := f.separator
	return NoLeadingSeparator(NormalizeSeparators(name, s), s), nil
}

func (f *fs) getObj(name string) (*s3.HeadObjectOutput, error) {
	bucketName, key := SplitName(name, f.separator)
	_, err := f.getBucket(bucketName)
	if err != nil {
		return nil, err
	}

	out, err := f.client.HeadObject(f.ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fs) getBucket(name string) (*s3.HeadBucketOutput, error) {
	out, err := f.client.HeadBucket(f.ctx, &s3.HeadBucketInput{
		Bucket: &name,
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fs) uploadObject(name string, body io.Reader) (*manager.UploadOutput, error) {
	bucketName, key := SplitName(name, f.separator)
	out, err := f.uploader.Upload(f.ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   body,
	})
	if err != nil {
		return nil, err
	} else {
		err := s3.NewObjectExistsWaiter(f.client).Wait(f.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}, time.Minute)

		if err != nil {
			f.log.Error("Failed attempt to wait for object %s to exist in %s.\n", key, bucketName)
		}
	}

	return out, nil
}
func (f *fs) downloadObject(name string) (*s3.GetObjectOutput, error) {
	bucketName, key := SplitName(name, f.separator)
	out, err := f.client.GetObject(f.ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *fs) deleteObject(name string) error {
	bucketName, key := SplitName(name, f.separator)
	_, err := f.client.DeleteObject(f.ctx, &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	return nil
}

func (f *fs) isObjectExist(name string) (bool, error) {
	if _, err := f.getObj(name); err != nil {
		var responseError *awshttp.ResponseError
		if errors.As(err, &responseError) && responseError.ResponseError.HTTPStatusCode() == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
