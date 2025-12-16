package reg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/mattn/go-sqlite3"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
)

type Registry struct {
	s3Client *s3.Client
	bucket   string
	db       *RegistryDB
}

func NewRegistry(ctx context.Context, bucket string) (*Registry, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}
	cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	s3Client := s3.NewFromConfig(cfg)

	db, err := initSQLite("registry.db")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return &Registry{
		s3Client: s3Client,
		bucket:   bucket,
		db:       db,
	}, nil
}

func (r *Registry) getBlobRedirect(ctx context.Context, name string, digest string, method string) (string, error) {
	algo, hex, found := strings.Cut(digest, ":")
	if !found {
		return "", fmt.Errorf("invalid digest format")
	}

	blobKey := fmt.Sprintf("docker/registry/v2/blobs/%s/%s/%s/data", algo, hex[0:2], hex)
	slog.Debug("getBlob", "name", name, "blobKey", blobKey, "method", method)

	// TODO: small blob cache and direct retrieval for small blobs
	expires := 15 * time.Minute

	var err error
	var presignedReq *v4.PresignedHTTPRequest
	presignClient := s3.NewPresignClient(r.s3Client)
	switch method {
	case http.MethodGet:
		presignedReq, err = presignClient.PresignGetObject(ctx,
			&s3.GetObjectInput{
				Bucket: &r.bucket,
				Key:    &blobKey,
			},
			s3.WithPresignExpires(expires),
		)
	case http.MethodHead:
		presignedReq, err = presignClient.PresignHeadObject(ctx,
			&s3.HeadObjectInput{
				Bucket: &r.bucket,
				Key:    &blobKey,
			},
			s3.WithPresignExpires(expires),
		)
	default:
		return "", fmt.Errorf("Method not allowed: %s", method)
	}
	if err != nil {
		return "", fmt.Errorf("failed to create presigned URL: %w", err)
	}
	return presignedReq.URL, nil
}

func (r *Registry) getManifestSHA(ctx context.Context, repo string, tag string) (digest.Digest, error) {
	metaKey := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", repo, tag)
	slog.Debug("getting manifest SHA", "repo", repo, "tag", tag, "metaKey", metaKey)

	obj, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &metaKey,
	})
	if err != nil {
		return "", fmt.Errorf("error getting sha: %w", err)
	}
	defer obj.Body.Close()
	sha, err := io.ReadAll(obj.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}
	return digest.Parse(string(sha))
}

func (r *Registry) getManifest(ctx context.Context, name string, reference string) (*v1.Manifest, []byte, error) {
	readyManifestBytes, err := r.db.GetManifest(name, reference)
	if err == nil {
		var manifest v1.Manifest
		if err := json.Unmarshal([]byte(readyManifestBytes), &manifest); err != nil {
			return nil, nil, err
		}
		return &manifest, []byte(readyManifestBytes), nil
	}

	sha, err := r.getManifestSHA(ctx, name, reference)
	if err != nil {
		return nil, nil, errors.Join(err, fs.ErrNotExist)
	}
	hex := sha.Hex()
	blobKey := fmt.Sprintf("docker/registry/v2/blobs/sha256/%s/%s/data", hex[0:2], hex)
	slog.Debug("getting manifest blob", "blobKey", blobKey)
	obj, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &r.bucket,
		Key:    &blobKey,
	})
	if err != nil {
		return nil, nil, err
	}
	defer obj.Body.Close()
	blobData, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, nil, err
	}
	var manifest v1.Manifest
	if err := json.Unmarshal(blobData, &manifest); err != nil {
		return nil, nil, err
	}

	if err := r.db.PutManifest(name, reference, string(blobData), &manifest); err != nil {
		slog.Error("error storing manifest in database", "error", err)
	}

	return &manifest, blobData, nil
}

func (r *Registry) putManifest(ctx context.Context, name string, reference string, manifestBytes []byte) error {
	sha := digest.FromBytes(manifestBytes)
	hex := sha.Hex()
	blobKey := fmt.Sprintf("docker/registry/v2/blobs/sha256/%s/%s/data", hex[0:2], hex)
	slog.Debug("putting manifest blob", "blobKey", blobKey)

	var manifest v1.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("error unmarshalling manifest: %w", err)
	}

	_, err := r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &blobKey,
		Body:   strings.NewReader(string(manifestBytes)),
	})
	if err != nil {
		return err
	}

	// TODO: check why on earth we need to put the same thing in at least 3 places... come on OCI
	metaKey := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", name, reference)
	slog.Debug("putting manifest meta", "metaKey", metaKey)

	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &metaKey,
		Body:   strings.NewReader(sha.String()),
	})
	if err != nil {
		return err
	}

	metaIndexKey := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/%s/index/%s/%s/link", name, reference, sha.Algorithm(), sha.Hex())
	slog.Debug("putting manifest index meta", "metaIndexKey", metaIndexKey)
	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &metaIndexKey,
		Body:   strings.NewReader(sha.String()),
	})
	if err != nil {
		return err
	}

	revisionsKey := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/revisions/%s/%s/link", name, sha.Algorithm(), sha.Hex())
	slog.Debug("putting manifest revisions meta", "revisionsKey", revisionsKey)
	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &revisionsKey,
		Body:   strings.NewReader(sha.String()),
	})
	if err != nil {
		return err
	}

	err = r.db.PutManifest(name, reference, string(manifestBytes), &manifest)
	if err != nil {
		slog.Error("error storing manifest in database", "error", err)
	}
	return nil
}

func (r *Registry) startUpload(ctx context.Context, name string, reference string) error {
	tempKey := fmt.Sprintf("uploads/%s.uploading", reference)

	multipartInput := &s3.CreateMultipartUploadInput{
		Bucket: &r.bucket,
		Key:    &tempKey,
	}

	_, err := r.s3Client.CreateMultipartUpload(ctx, multipartInput)
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %w", err)
	}

	return r.db.CreateUploadSession(reference, name, tempKey)
}

func (r *Registry) uploadChunk(ctx context.Context, reference string, offset int64, body io.ReadCloser) (int64, error) {
	defer body.Close()

	s3UploadID, s3Key, uploadedSize, err := r.db.GetUploadSession(reference)
	if err != nil {
		return 0, fmt.Errorf("upload session not found: %w", err)
	}

	if s3UploadID == "" {
		tempKey := fmt.Sprintf("uploads/%s.uploading", reference)
		multipartInput := &s3.CreateMultipartUploadInput{
			Bucket: &r.bucket,
			Key:    &tempKey,
		}

		multipartOutput, err := r.s3Client.CreateMultipartUpload(ctx, multipartInput)
		if err != nil {
			return 0, fmt.Errorf("failed to create multipart upload: %w", err)
		}
		s3UploadID = *multipartOutput.UploadId
	}

	if offset != uploadedSize {
		return 0, fmt.Errorf("invalid offset: expected %d, got %d", uploadedSize, offset)
	}

	partNumber := int32((offset / (5 * 1024 * 1024)) + 1)

	buf := &bytes.Buffer{}
	n, err := io.Copy(buf, body)
	if err != nil {
		return 0, fmt.Errorf("failed to read request body: %w", err)
	}

	uploadPartInput := &s3.UploadPartInput{
		Bucket:     &r.bucket,
		Key:        &s3Key,
		PartNumber: &partNumber,
		UploadId:   &s3UploadID,
		Body:       bytes.NewReader(buf.Bytes()),
	}

	_, err = r.s3Client.UploadPart(ctx, uploadPartInput)
	if err != nil {
		return 0, fmt.Errorf("failed to upload part: %w", err)
	}

	newUploadedSize := uploadedSize + n
	err = r.db.UpdateUploadSession(reference, s3UploadID, newUploadedSize)
	if err != nil {
		return 0, fmt.Errorf("failed to update upload session: %w", err)
	}

	return n, nil
}

func (r *Registry) completeUpload(ctx context.Context, reference string, dig string) error {
	s3UploadID, s3Key, _, err := r.db.GetUploadSession(reference)
	if err != nil {
		return fmt.Errorf("upload session not found: %w", err)
	}

	if s3UploadID == "" {
		return fmt.Errorf("no active multipart upload found")
	}

	listPartsInput := &s3.ListPartsInput{
		Bucket:   &r.bucket,
		Key:      &s3Key,
		UploadId: &s3UploadID,
	}

	listPartsOutput, err := r.s3Client.ListParts(ctx, listPartsInput)
	if err != nil {
		return fmt.Errorf("failed to list parts: %w", err)
	}

	var completedParts []types.CompletedPart
	for _, part := range listPartsOutput.Parts {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &r.bucket,
		Key:      &s3Key,
		UploadId: &s3UploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}

	_, err = r.s3Client.CompleteMultipartUpload(ctx, completeInput)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	sha, err := digest.Parse(dig)
	if err != nil {
		return fmt.Errorf("failed to parse digest: %w", err)
	}

	hex := sha.Hex()
	finalBlobKey := fmt.Sprintf("docker/registry/v2/blobs/sha256/%s/%s/data", hex[0:2], hex)

	copyInput := &s3.CopyObjectInput{
		Bucket:     &r.bucket,
		Key:        &finalBlobKey,
		CopySource: aws.String(fmt.Sprintf("%s/%s", r.bucket, s3Key)),
	}

	_, err = r.s3Client.CopyObject(ctx, copyInput)
	if err != nil {
		return fmt.Errorf("failed to copy blob to final location: %w", err)
	}

	deleteInput := &s3.DeleteObjectInput{
		Bucket: &r.bucket,
		Key:    &s3Key,
	}

	_, err = r.s3Client.DeleteObject(ctx, deleteInput)
	if err != nil {
		slog.Warn("failed to delete temporary upload file", "key", s3Key, "error", err)
	}

	err = r.db.DeleteUploadSession(reference)
	if err != nil {
		slog.Warn("failed to delete upload session", "reference", reference, "error", err)
	}

	slog.Debug("completed upload", "tempKey", s3Key, "finalKey", finalBlobKey)
	return nil
}

func (r *Registry) getUploadSession(uploadID string) (string, string, int64, error) {
	return r.db.GetUploadSession(uploadID)
}

func (r *Registry) abortUpload(ctx context.Context, uploadID string) error {
	s3UploadID, s3Key, _, err := r.db.GetUploadSession(uploadID)
	if err != nil {
		return fmt.Errorf("upload session not found: %w", err)
	}

	if s3UploadID != "" {
		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   &r.bucket,
			Key:      &s3Key,
			UploadId: &s3UploadID,
		}

		_, err = r.s3Client.AbortMultipartUpload(ctx, abortInput)
		if err != nil {
			slog.Warn("failed to abort multipart upload", "uploadID", s3UploadID, "error", err)
		}
	}

	err = r.db.DeleteUploadSession(uploadID)
	if err != nil {
		slog.Warn("failed to delete upload session", "uploadID", uploadID, "error", err)
	}

	return nil
}

func (r *Registry) cleanupStaleUploads(ctx context.Context) error {
	uploadIDs, err := r.db.GetStaleUploadSessions("-24 hours")
	if err != nil {
		return fmt.Errorf("failed to get stale upload sessions: %w", err)
	}

	for _, uploadID := range uploadIDs {
		err := r.abortUpload(ctx, uploadID)
		if err != nil {
			slog.Warn("failed to cleanup stale upload", "uploadID", uploadID, "error", err)
		}
	}

	slog.Info("cleaned up stale uploads", "count", len(uploadIDs))
	return nil
}

func (r *Registry) listTags(ctx context.Context, name string) ([]string, error) {
	readyTags, err := r.db.ListTags(name)
	if err == nil && len(readyTags) > 0 {
		return readyTags, nil
	}

	var repoTags []string
	var continuationToken *string
	prefix := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/", name)
	for {
		req, err := r.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range req.Contents {
			if strings.HasSuffix(*obj.Key, "current/link") {
				tag := strings.TrimSuffix(
					strings.TrimPrefix(
						*obj.Key,
						fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/", name),
					),
					"/current/link",
				)
				repoTags = append(repoTags, tag)
			}
		}
		if req.IsTruncated == nil || !*req.IsTruncated {
			break
		}
		continuationToken = req.NextContinuationToken
	}

	err = r.db.PutTags(name, repoTags)
	if err != nil {
		slog.Error("error storing tags in database", "error", err)
	}

	return repoTags, nil
}

func (r *Registry) listRepositories(_ context.Context, continuationToken *string, n int) ([]string, *string, error) {
	return r.db.ListRepositories(continuationToken, n)
}

func (r *Registry) Bootstrap(ctx context.Context) error {
	prefix := "docker/registry/v2/repositories/"
	var continuationToken *string

	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(runtime.NumCPU() * 4)

	found := uint64(0)
	skipped := uint64(0)
	processed := uint64(0)
	processing := int64(0)
	for {
		req, err := r.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucket,
			Prefix:            &prefix,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return err
		}
		for _, obj := range req.Contents {
			if strings.HasSuffix(*obj.Key, "current/link") {
				found++
				noPrefix := strings.TrimPrefix(*obj.Key, "docker/registry/v2/repositories/")
				repo, tag, ok := strings.Cut(noPrefix, "/_manifests/tags/")
				if !ok {
					continue
				}
				tag = strings.TrimSuffix(tag, "/current/link")
				if r.db.Exists(repo, tag) {
					skipped++
					if skipped%10000 == 5000 {
						slog.Info("Bootstrap progress", "skipped", skipped)
					}
					continue
				}
				group.Go(func() error {
					atomic.AddInt64(&processing, 1)
					defer atomic.AddInt64(&processing, -1)
					_, _, err := r.getManifest(ctx, repo, tag)
					atomic.AddUint64(&processed, 1)
					if err != nil {
						slog.Warn("error getting manifest", "repo", repo, "tag", tag, "error", err)
					}
					return nil
				})
				if found%1000 == 500 {
					slog.Info("Bootstrap progress", "found", found, "processed", processed, "processing", processing)
				}
			}
		}
		if req.IsTruncated == nil || !*req.IsTruncated {
			break
		}
		continuationToken = req.NextContinuationToken
	}
	return group.Wait()
}

func (r *Registry) Close() error {
	if err := r.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}
	return nil
}
