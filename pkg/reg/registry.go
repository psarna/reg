package reg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
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
	slog.Debug("getting blob", "name", name, "digest", digest, "blobKey", blobKey)

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
		return "", fmt.Errorf("Method not allowed: %s", http.StatusMethodNotAllowed)
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
		return nil, nil, err
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
