package reg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	// TODO: customize the uploads directory
	if err := os.Mkdir("uploads", 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create uploads directory: %w", err)
	}

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

func (r *Registry) uploadChunk(ctx context.Context, name string, reference string, offset int64, len int64, body io.ReadCloser) (int64, error) {
	f, err := os.OpenFile(filepath.Join("uploads", reference), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	_, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, body)
	if err != nil {
		return 0, err
	}
	if n < len {
		return 0, err
	}

	return n, nil
}

func (r *Registry) completeUpload(ctx context.Context, name string, reference string, dig string) error {
	f, err := os.OpenFile(filepath.Join("uploads", reference), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	sha, err := digest.Parse(dig)
	if err != nil {
		return err
	}
	hex := sha.Hex()
	blobKey := fmt.Sprintf("docker/registry/v2/blobs/sha256/%s/%s/data", hex[0:2], hex)
	slog.Debug("completing upload", "blobKey", blobKey)
	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &blobKey,
		Body:   f,
	})
	// TODO: read what they need this _uploads thing for, atomicity?
	return err
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
				tag := strings.TrimSuffix(strings.TrimPrefix(*obj.Key, fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/", name)), "/current/link")
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

func (r *Registry) listRepositories(ctx context.Context, continuationToken *string, n int) ([]string, *string, error) {
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
