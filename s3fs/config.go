package s3fs

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"os"
)

type Config struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	Session         string `json:"session"`
}

func loadJsonConfig(jsonFile string) (Config, error) {

	var err error
	var cfg Config

	b, err := os.ReadFile(jsonFile)
	if err != nil {
		return cfg, err
	}

	err = json.Unmarshal(b, &cfg)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

func loadS3Config(ctx context.Context, jsonFile string) (aws.Config, error) {
	var cfg aws.Config

	jsonCfg, err := loadJsonConfig(jsonFile)
	if err != nil {
		return cfg, err
	}
	var opts []func(*config.LoadOptions) error
	if jsonCfg.Endpoint != "" {
		opts = append(opts, config.WithBaseEndpoint(jsonCfg.Endpoint))
	}
	if jsonCfg.Region != "" {
		opts = append(opts, config.WithRegion(jsonCfg.Region))
	}

	if jsonCfg.AccessKeyID != "" && jsonCfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			jsonCfg.AccessKeyID,
			jsonCfg.SecretAccessKey,
			jsonCfg.Session,
		)))
	}

	cfg, err = config.LoadDefaultConfig(ctx, opts...)

	if err != nil {
		panic(err)
	}

	return cfg, nil

}
