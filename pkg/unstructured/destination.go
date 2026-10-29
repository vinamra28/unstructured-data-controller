package unstructured

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Destination interface {
	// SyncFilesToDestination uploads filePaths and returns how many were
	// actually written. Files already present at the destination with a
	// matching checksum are skipped, so the count is a true delta rather than
	// len(filePaths). On error the count covers the uploads that completed
	// before the failure.
	SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore, filePaths []string) (int, error)
}

// S3Destination syncs files to an S3 bucket at prefix/stages/<stageName>/.
type S3Destination struct {
	S3Client  *s3.Client
	Bucket    string
	Prefix    string
	StageName string
}

// s3Key returns the destination key, preserving the path structure
// relative to the stage directory.
// e.g. "pipelines/p/stages/crawl/permissions/1ABC.json"
//
//	→ "<prefix>/stages/crawl/permissions/1ABC.json"
func (d *S3Destination) s3Key(filePath string) string {
	rel := filePath
	if idx := strings.Index(filePath, "stages/"+d.StageName+"/"); idx >= 0 {
		rel = filePath[idx+len("stages/"+d.StageName+"/"):]
	}
	key := filepath.Join(d.Prefix, "stages", d.StageName, rel)
	if filepath.Separator != '/' {
		key = filepath.ToSlash(key)
	}
	return key
}

func (d *S3Destination) SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore,
	chunksFilePaths []string) (int, error) {
	logger := log.FromContext(ctx)
	// uploaded counts files actually written, not files considered: unchanged
	// files are skipped below, so len(chunksFilePaths) would report the whole
	// corpus on every reconcile even when nothing moved.
	uploaded := 0
	destPrefix := filepath.Join(d.Prefix, "stages", d.StageName) + "/"
	if filepath.Separator != '/' {
		destPrefix = filepath.ToSlash(destPrefix)
	}
	logger.Info("syncing data to S3 destination",
		"bucket", d.Bucket, "prefix", destPrefix, "filePaths", chunksFilePaths)

	s3Client := d.S3Client

	keysInDestination := make(map[string]bool)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.Bucket),
		Prefix: aws.String(destPrefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return uploaded, fmt.Errorf("list objects s3://%s prefix %q: %w", d.Bucket, destPrefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keysInDestination[*obj.Key] = true
			}
		}
	}

	for _, chunksFilePath := range chunksFilePaths {
		data, err := fs.Retrieve(ctx, chunksFilePath)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", chunksFilePath)
			return uploaded, fmt.Errorf("retrieve %s: %w", chunksFilePath, err)
		}

		key := d.s3Key(chunksFilePath)
		delete(keysInDestination, key)

		// Calculate SHA256 of local file
		hash := sha256.Sum256(data)
		localSHA256 := base64.StdEncoding.EncodeToString(hash[:])

		// Check if file exists and compare SHA256 from metadata
		headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:       aws.String(d.Bucket),
			Key:          aws.String(key),
			ChecksumMode: types.ChecksumModeEnabled,
		})

		var notFoundErr *types.NotFound
		if err != nil && errors.As(err, &notFoundErr) {
			if notFoundErr.ErrorCode() != "NotFound" {
				logger.Info("error while fetching the object",
					"key", key, "error", err.Error())
				return uploaded, err
			}
		}

		if err == nil && headResp.ChecksumSHA256 != nil && *headResp.ChecksumSHA256 == localSHA256 {
			logger.Info("file unchanged, skipping upload",
				"file", chunksFilePath, "key", key)
			continue
		}

		// File is new or changed, upload it with SHA256
		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:            aws.String(d.Bucket),
			Key:               aws.String(key),
			Body:              bytes.NewReader(data),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		})
		if err != nil {
			logger.Error(err, "failed to upload file to S3", "bucket", d.Bucket, "key", key)
			return uploaded, fmt.Errorf("put object s3://%s/%s: %w", d.Bucket, key, err)
		}
		uploaded++
		logger.Info("uploaded file to S3 destination", "key", key)
	}

	// Remaining keys = files in destination but no longer in ingestion; delete
	for key := range keysInDestination {
		logger.Info("found file in destination no longer in ingestion, deleting", "key", key)
		_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			logger.Error(err, "failed to delete file from S3 destination", "key", key)
			return uploaded, fmt.Errorf("delete object s3://%s/%s: %w", d.Bucket, key, err)
		}
	}

	return uploaded, nil
}
