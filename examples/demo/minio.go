package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	objs3 "github.com/ankur-anand/objlog/s3"
)

// openMinIO wires objlog to a local MinIO, which speaks the same S3 API as AWS.
// Point OBJLOG_MINIO_ENDPOINT at real S3 and the rest of the demo is unchanged.
func openMinIO(ctx context.Context, prefix string) (*provider, error) {
	endpoint := env("OBJLOG_MINIO_ENDPOINT", "http://127.0.0.1:9000")
	bucket := env("OBJLOG_MINIO_BUCKET", "objlog-demo")
	accessKey := env("OBJLOG_MINIO_ACCESS_KEY", "minioadmin")
	secretKey := env("OBJLOG_MINIO_SECRET_KEY", "minioadmin")

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(env("OBJLOG_MINIO_REGION", "us-east-1")),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // MinIO serves path-style buckets
	})

	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil && !bucketExists(err) {
		return nil, fmt.Errorf("create bucket %q at %s: %w (is MinIO running?)", bucket, endpoint, err)
	}

	st, err := objs3.New(objs3.Options{
		Client:   client,
		Bucket:   bucket,
		Prefix:   prefix,
		StreamID: streamID,
	})
	if err != nil {
		return nil, fmt.Errorf("s3.New: %w", err)
	}

	list := func(ctx context.Context) ([]object, error) {
		var objects []object
		pages := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, item := range page.Contents {
				objects = append(objects, object{key: aws.ToString(item.Key), size: aws.ToInt64(item.Size)})
			}
		}
		return objects, nil
	}

	return &provider{
		name:        "minio",
		where:       endpoint,
		container:   "bucket " + bucket,
		store:       st,
		listObjects: list,
		close:       func() {},
	}, nil
}

func bucketExists(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists"
}
