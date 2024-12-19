package s3fs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/afero"
)

var (
	ErrorNoBucketName = errors.New("no bucket name")
	ErrFilenameEmpty  = errors.New("filename is empty")
	ErrEmptyObjectKey = errors.New("input member Key must not be empty")
	ErrDirExists      = errors.New("directory already exists")
)

type ObjectAttrs struct {
	Key          string
	Size         int64
	LastModified time.Time
	Prefix       string
}

type fs struct {
	ctx        context.Context
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	log        *slog.Logger

	separator string
	limit     int32
}

func newFs(ctx context.Context, client *s3.Client) *fs {

	return &fs{
		ctx:    ctx,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = 10 * 1024 * 1024
		}),
		downloader: manager.NewDownloader(client, func(d *manager.Downloader) {
			d.Concurrency = 1
			d.PartSize = 10 * 1024 * 1024
		}),
		separator: "/",
		limit:     1000,
	}
}

// S3 don't have a concept of directories
// so we have to create a virtual directory
// by creating an empty object with a trailing separator
func (f *fs) Mkdir(name string, perm os.FileMode) error {
	name, err := f.parseName(name)
	if err != nil {
		return err
	}

	bucketName, key := SplitName(name, f.separator)
	if bucketName == "" {
		return ErrorNoBucketName
	}

	if key == "" {
		return ErrEmptyObjectKey
	}

	// we have to ensure dir name ends with separator
	// so we can create virtual directory
	key = EnsureTrailingSeparator(key, f.separator)

	_, err = f.client.PutObject(f.ctx, &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   strings.NewReader(""),
	})
	if err != nil {
		return err
	}

	return nil
}

func (f *fs) MkdirAll(path string, perm os.FileMode) error {
	path, err := f.parseName(path)
	if err != nil {
		return err
	}

	bucketName, key := SplitName(path, f.separator)
	if bucketName == "" {
		return ErrorNoBucketName
	}

	if key == "" {
		return ErrEmptyObjectKey
	}

	folderName := ""
	folders := strings.Split(path, f.separator)

	for i, folder := range folders {
		// If first element we it as the bucket name and continue
		if i == 0 {
			folderName = folder
			continue
		} else if folder == "" {
			continue
		} else {
			folderName = folderName + f.separator + folder
		}

		if err := f.Mkdir(folderName, perm); err != nil {
			return err
		}
	}

	return nil
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
		// if it's a directory we have to check if it's empty
		dir, err := f.Open(name)
		if err != nil {
			return err
		}

		infos, err := dir.Readdir(-1)
		if err != nil {
			return err
		}

		if len(infos) > 0 {
			return syscall.ENOTEMPTY
		}

		name = EnsureTrailingSeparator(name, f.separator)
	}

	return f.deleteObject(name)
}

func (f *fs) Rename(oldname, newname string) error {
	return nil
}

func (f *fs) Stat(name string) (os.FileInfo, error) {
	name, err := f.parseName(name)
	if err != nil {
		return nil, err
	}

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

	if key == "" {
		return nil, ErrEmptyObjectKey
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
func (f *fs) DownloadObject(name string, w io.WriterAt, opts ...func(d *manager.Downloader)) (int64, error) {
	bucketName, key := SplitName(name, f.separator)

	params := &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	}
	n, err := f.downloader.Download(f.ctx, w, params, opts...)
	if err != nil {
		return 0, err
	}

	return n, nil
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

func (f *fs) checkIfDirExists(name string) (bool, error) {
	bucketName, key := SplitName(name, f.separator)
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(key),
		Delimiter: aws.String(f.separator),
	}

	p := s3.NewListObjectsV2Paginator(f.client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 2
	})

	for p.HasMorePages() {
		page, err := p.NextPage(f.ctx)
		if err != nil {
			return false, err
		}

		// Check if common prefixes exist
		// if yes we have dictionary
		if len(page.CommonPrefixes) > 0 {
			return true, nil
		}

	}

	return false, nil
}

func (f *fs) getObjectsAttrs(name string) ([]ObjectAttrs, error) {
	bucketName, key := SplitName(name, f.separator)
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(key),
		Delimiter: aws.String(f.separator),
	}

	p := s3.NewListObjectsV2Paginator(f.client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = f.limit
	})

	var objects []ObjectAttrs
	for p.HasMorePages() {
		page, err := p.NextPage(f.ctx)
		if err != nil {
			return nil, err
		}

		dirs := page.CommonPrefixes
		for _, dir := range dirs {
			objects = append(objects, ObjectAttrs{
				Key:    *dir.Prefix,
				Prefix: *dir.Prefix,
			})
		}

		for _, obj := range page.Contents {
			objects = append(objects, ObjectAttrs{
				Key:          *obj.Key,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
			})
		}
	}

	return objects, nil
}
