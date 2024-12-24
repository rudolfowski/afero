package s3fs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/spf13/afero"
)

type FSConfig struct {
	AwsConfig aws.Config

	DownloadPartSize *int64
	UploadPartSize   *int64

	// The maximum number of objects to return
	Limit *int32
}

func DefaultFSConfig() FSConfig {
	return FSConfig{
		DownloadPartSize: aws.Int64(0 * 1024 * 1024),
		UploadPartSize:   aws.Int64(0 * 1024 * 1024),
		Limit:            aws.Int32(1000),
	}
}

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
	IsDir        bool
}

type Fs struct {
	ctx        context.Context
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	log        *slog.Logger

	separator string
	limit     int32
}

func newFs(ctx context.Context, client *s3.Client, conf FSConfig) *Fs {
	c := DefaultFSConfig()
	if conf.DownloadPartSize != nil {
		c.DownloadPartSize = conf.DownloadPartSize
	}

	if conf.UploadPartSize != nil {
		c.UploadPartSize = conf.UploadPartSize
	}

	if conf.Limit != nil {
		c.Limit = conf.Limit
	}

	return &Fs{
		ctx:    ctx,
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = aws.ToInt64(c.UploadPartSize)
		}),
		downloader: manager.NewDownloader(client, func(d *manager.Downloader) {
			d.Concurrency = 1
			d.PartSize = aws.ToInt64(c.DownloadPartSize)
		}),
		separator: "/",
		limit:     aws.ToInt32(c.Limit),
	}
}

// S3 don't have a concept of directories
// so we have to create a virtual directory
// by creating an empty object with a trailing separator
func (f *Fs) Mkdir(name string, perm os.FileMode) error {
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

func (f *Fs) MkdirAll(path string, perm os.FileMode) error {
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

func (f *Fs) Create(name string) (*S3File, error) {
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

	return newS3File(f, name, os.O_CREATE), nil

}

func (f *Fs) Open(name string) (*S3File, error) {
	return f.OpenFile(name, os.O_RDONLY, 0)
}

func (f *Fs) OpenFile(name string, flag int, perm os.FileMode) (*S3File, error) {
	name, err := f.parseName(name)
	if err != nil {
		return nil, err
	}

	file := newS3File(f, name, flag)
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
			return nil, syscall.EPERM
		}

		if _, err = file.WriteString(""); err != nil {
			return nil, err
		}
	}
	return file, nil
}

func (f *Fs) Remove(name string) error {
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
		if err != nil && err != io.EOF {
			return err
		}

		if len(infos) > 0 {
			return syscall.ENOTEMPTY
		}

		name = EnsureTrailingSeparator(name, f.separator)
	}

	return f.deleteObject(name)
}

func (f *Fs) RemoveAll(path string) error {
	path, err := f.parseName(path)
	if err != nil {
		return err
	}

	fi, err := f.Stat(path)
	if err != nil {
		// if the path doesn't exist we just return nil
		if errors.Is(err, afero.ErrFileNotFound) {
			return nil
		}
		return err
	}

	// if it's a file we just remove it
	if !fi.IsDir() {
		return f.Remove(path)
	}

	dir, err := f.Open(path)
	if err != nil {
		return err
	}

	infos, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		return err
	}

	for _, info := range infos {
		err = f.RemoveAll(path + f.separator + info.Name())
		if err != nil {
			return err
		}
	}

	return f.Remove(path)
}

func (f *Fs) Rename(oldname, newname string) error {
	oldname, err := f.parseName(oldname)
	if err != nil {
		return err
	}

	newname, err = f.parseName(newname)
	if err != nil {
		return err
	}

	_, err = f.getObj(oldname)
	if err != nil {
		return err
	}

	_, err = f.getObj(newname)
	if err != nil {
		if !errors.Is(err, afero.ErrFileNotFound) {
			return err
		}
	}

	err = f.copyObject(oldname, newname)
	if err != nil {
		return err
	}

	return f.deleteObject(oldname)
}

func (f *Fs) Stat(name string) (os.FileInfo, error) {
	name, err := f.parseName(name)
	if err != nil {
		return nil, err
	}

	return newFileInfo(name, f, DefaultFileMode)
}

func (f *Fs) parseName(name string) (string, error) {
	if name == "" {
		return "", ErrFilenameEmpty
	}
	s := f.separator
	return NoLeadingSeparator(NormalizeSeparators(name, s), s), nil
}

func (f *Fs) getObj(name string) (*s3.HeadObjectOutput, error) {
	bucketName, key := SplitName(name, f.separator)

	// if bucket name is empty we return error
	if bucketName == "" {
		return nil, ErrorNoBucketName
	}

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
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				err = afero.ErrFileNotFound
			}
		}
		return nil, err
	}
	return out, nil
}

func (f *Fs) getBucket(name string) (*s3.HeadBucketOutput, error) {
	out, err := f.client.HeadBucket(f.ctx, &s3.HeadBucketInput{
		Bucket: &name,
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *Fs) getBuckets() ([]string, error) {
	out, err := f.client.ListBuckets(f.ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []string
	for _, bucket := range out.Buckets {
		buckets = append(buckets, *bucket.Name)
	}

	return buckets, nil
}

func (f *Fs) uploadObject(name string, body io.Reader) (*manager.UploadOutput, error) {
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

func (f *Fs) copyObject(source string, dest string) error {
	destBucket, destKey := SplitName(dest, f.separator)
	_, err := f.client.CopyObject(f.ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		CopySource: aws.String(source),
		Key:        aws.String(destKey),
	})
	if err != nil {
		return err
	} else {
		err := s3.NewObjectExistsWaiter(f.client).Wait(f.ctx, &s3.HeadObjectInput{
			Bucket: aws.String(destBucket),
			Key:    aws.String(destKey),
		}, time.Minute)

		if err != nil {
			f.log.Error("Failed attempt to wait for object %s to exist in %s.\n", destKey, destBucket)
		}
	}

	return nil
}

func (f *Fs) DownloadObject(name string, w io.WriterAt, opts ...func(d *manager.Downloader)) (int64, error) {
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

func (f *Fs) deleteObject(name string) error {
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

func (f *Fs) isObjectExist(name string) (bool, error) {
	if _, err := f.getObj(name); err != nil {
		if errors.Is(err, afero.ErrFileNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *Fs) checkIfDirExists(name string) (bool, error) {
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

func (f *Fs) getObjectsAttrs(name string) ([]ObjectAttrs, error) {
	bucketName, key := SplitName(name, f.separator)

	// if bucket name is empty we read all buckets
	if bucketName == "" {

		buckets, err := f.getBuckets()
		if err != nil {
			return nil, err
		}

		var objects []ObjectAttrs
		for _, bucket := range buckets {
			objects = append(objects, ObjectAttrs{
				Key:   bucket,
				IsDir: true,
			})
		}

		return objects, nil
	}

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
			dirKey := strings.TrimPrefix(aws.ToString(dir.Prefix), key)
			objects = append(objects, ObjectAttrs{
				Key:   dirKey,
				IsDir: true,
			})
		}

		for _, obj := range page.Contents {
			if *obj.Key == key {
				continue
			}
			objects = append(objects, ObjectAttrs{
				Key:          *obj.Key,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
			})
		}
	}

	return objects, nil
}
