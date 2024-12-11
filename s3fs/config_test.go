package s3fs

import (
	"testing"
)

func Test_LoadJsonConfig(t *testing.T) {

	cfg, err := loadJsonConfig("s3-fake-config.json")
	if err != nil {
		t.Fatalf("Error loading config: %v", err)
	}

	if cfg.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("Expected endpoint http://127.0.0.1:9000, got %s", cfg.Endpoint)
	}

	if cfg.Region != "eu" {
		t.Fatalf("Expected region eu, got %s", cfg.Region)
	}

	if cfg.AccessKeyID != "minio" {
		t.Fatalf("Expected access key minio, got %s", cfg.AccessKeyID)
	}

	if cfg.SecretAccessKey != "minio123" {
		t.Fatalf("Expected secret access key minio123, got %s", cfg.SecretAccessKey)
	}

}

func Test_LoadS3Config(t *testing.T) {

	// check if aws config file exist

}
