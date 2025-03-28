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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gorilla/mux"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Handler struct {
	s3Client *s3.Client
	bucket   string
}

func NewRouter(ctx context.Context, bucket string) (*mux.Router, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	h := &Handler{
		s3Client: s3Client,
		bucket:   bucket,
	}

	r := mux.NewRouter()
	apiRouter := r.PathPrefix("/v2").Subrouter()

	// end-1: Check API support
	apiRouter.Handle("/", http.HandlerFunc(h.checkAPISupport)).Methods("GET")

	// end-2: Get blob
	apiRouter.Handle("/{name:.*}/blobs/{digest}", http.HandlerFunc(h.getBlob)).Methods("GET", "HEAD")

	// end-3: Get manifest
	apiRouter.Handle("/{name:.*}/manifests/{reference}", http.HandlerFunc(h.getManifest)).Methods("GET", "HEAD")

	// end-4a: Start upload
	apiRouter.Handle("/{name:.*}/blobs/uploads/", http.HandlerFunc(h.startUpload)).Methods("POST")

	// end-4b: Start upload with digest
	apiRouter.Handle("/{name:.*}/blobs/uploads/", http.HandlerFunc(h.startUploadWithDigest)).
		Methods("POST").
		Queries("digest", "{digest}")

	// end-5: Upload chunk
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.uploadChunk)).Methods("PATCH")

	// end-6: Complete upload
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.completeUpload)).
		Methods("PUT").
		Queries("digest", "{digest}")

	// end-7: Put manifest
	apiRouter.Handle("/{name:.*}/manifests/{reference}", http.HandlerFunc(h.putManifest)).Methods("PUT")

	// end-8a: List tags
	apiRouter.Handle("/{name:.*}/tags/list", http.HandlerFunc(h.listTags)).Methods("GET")

	// end-8b: List tags with pagination
	apiRouter.Handle("/{name:.*}/tags/list", http.HandlerFunc(h.listTagsPaginated)).
		Methods("GET").
		Queries("n", "{n:[0-9]+}", "last", "{last}")

	// end-9: Delete manifest
	apiRouter.Handle("/{name:.*}/manifests/{reference}", http.HandlerFunc(h.deleteManifest)).Methods("DELETE")

	// end-10: Delete blob
	apiRouter.Handle("/{name:.*}/blobs/{digest}", http.HandlerFunc(h.deleteBlob)).Methods("DELETE")

	// end-11: Mount blob from another repository
	apiRouter.Handle("/{name:.*}/blobs/uploads/", http.HandlerFunc(h.mountBlob)).
		Methods("POST").
		Queries("mount", "{digest}", "from", "{other_name}")

	// end-12a: Get referrers
	apiRouter.Handle("/{name:.*}/referrers/{digest}", http.HandlerFunc(h.getReferrers)).Methods("GET")

	// end-12b: Get referrers filtered by artifact type
	apiRouter.Handle("/{name:.*}/referrers/{digest}", http.HandlerFunc(h.getReferrersFiltered)).
		Methods("GET").
		Queries("artifactType", "{artifactType}")

	// end-13: Get upload status
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.getUploadStatus)).Methods("GET")

	return r, nil
}

func (h *Handler) checkAPISupport(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]
	algo, hex, found := strings.Cut(digest, ":")
	if !found {
		http.Error(w, "Invalid digest format", http.StatusBadRequest)
		return
	}

	blobKey := fmt.Sprintf("docker/registry/v2/blobs/%s/%s/%s/data", algo, hex[0:2], hex)
	slog.Debug("getting blob", "name", name, "digest", digest, "blobKey", blobKey)

	// TODO: small blob cache and direct retrieval for small blobs

	expires := 15 * time.Minute

	presignClient := s3.NewPresignClient(h.s3Client)
	switch r.Method {
	case http.MethodGet:
		presignedReq, err := presignClient.PresignGetObject(r.Context(),
			&s3.GetObjectInput{
				Bucket: &h.bucket,
				Key:    &blobKey,
			},
			s3.WithPresignExpires(expires),
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to create presigned URL: %v", err), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, presignedReq.URL, http.StatusFound)

	case http.MethodHead:
		presignedReq, err := presignClient.PresignHeadObject(r.Context(),
			&s3.HeadObjectInput{
				Bucket: &h.bucket,
				Key:    &blobKey,
			},
			s3.WithPresignExpires(expires),
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to create presigned URL: %v", err), http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, presignedReq.URL, http.StatusFound)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) getManifestSHA(ctx context.Context, repo string, tag string) (digest.Digest, error) {
	metaKey := fmt.Sprintf("docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", repo, tag)
	slog.Debug("getting manifest SHA", "repo", repo, "tag", tag, "metaKey", metaKey)

	obj, err := h.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &h.bucket,
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

func (h *Handler) getManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	sha, err := h.getManifestSHA(r.Context(), name, reference)
	if err != nil {
		slog.Error("error getting manifest SHA", "error", err)
		http.Error(w, fmt.Sprintf("error getting manifest: %v", err), http.StatusInternalServerError)
		return
	}
	hex := sha.Hex()
	blobKey := fmt.Sprintf("docker/registry/v2/blobs/sha256/%s/%s/data", hex[0:2], hex)
	slog.Debug("getting manifest blob", "blobKey", blobKey)
	obj, err := h.s3Client.GetObject(r.Context(), &s3.GetObjectInput{
		Bucket: &h.bucket,
		Key:    &blobKey,
	})
	if err != nil {
		slog.Error("error getting manifest blob", "error", err)
		http.Error(w, fmt.Sprintf("error getting manifest blob: %v", err), http.StatusInternalServerError)
		return
	}
	defer obj.Body.Close()
	blobData, err := io.ReadAll(obj.Body)
	if err != nil {
		slog.Error("error reading manifest blob", "error", err)
		http.Error(w, fmt.Sprintf("error reading manifest blob: %v", err), http.StatusInternalServerError)
		return
	}
	var manifest v1.Manifest
	if err := json.Unmarshal(blobData, &manifest); err != nil {
		slog.Error("error unmarshalling manifest", "error", err)
		http.Error(w, fmt.Sprintf("error unmarshalling manifest: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", manifest.MediaType)
	w.Header().Set("Docker-Content-Digest", string(sha))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(blobData)))
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(blobData)
	if err != nil {
		slog.Error("error writing manifest response", "error", err)
		http.Error(w, fmt.Sprintf("error writing manifest response: %v", err), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) startUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/123456", name))
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) startUploadWithDigest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/123456", name))
	w.WriteHeader(http.StatusAccepted)
	fmt.Printf("Started upload for %s with digest %s", name, digest)
}

func (h *Handler) uploadChunk(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	w.Header().Set("Range", "0-100")
	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference))
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) completeUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]
	digest := vars["digest"]

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.WriteHeader(http.StatusCreated)
	fmt.Printf("Completed upload for %s reference %s with digest %s", name, reference, digest)
}

func (h *Handler) putManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/manifests/%s", name, reference))
	w.WriteHeader(http.StatusCreated)
	fmt.Printf("Uploaded manifest for %s with reference %s", name, reference)
}

func (h *Handler) listTags(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	w.WriteHeader(http.StatusOK)
	slog.Warn("list tags not implemented")
	fmt.Fprintf(w, `{"name":"%s", "tags":["tag1", "tag2", "tag3"]}`, name)
}

func (h *Handler) listTagsPaginated(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	n := vars["n"]
	last := vars["last"]

	w.WriteHeader(http.StatusOK)
	fmt.Printf(`{"name":"%s", "tags":["tag4", "tag5"], "n":%s, "last":"%s"}`, name, n, last)
}

func (h *Handler) deleteManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	w.WriteHeader(http.StatusAccepted)
	fmt.Printf("Deleting manifest for %s with reference %s", name, reference)
}

func (h *Handler) deleteBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]

	w.WriteHeader(http.StatusAccepted)
	fmt.Printf("Deleting blob for %s with digest %s", name, digest)
}

func (h *Handler) mountBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]
	otherName := vars["other_name"]

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.WriteHeader(http.StatusCreated)
	fmt.Printf("Mounted blob from %s to %s with digest %s", otherName, name, digest)
}

func (h *Handler) getReferrers(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]

	w.WriteHeader(http.StatusOK)
	fmt.Printf(`{"name":"%s", "digest":"%s", "referrers":[{"mediaType":"application/example", "digest":"sha256:abc"}]}`, name, digest)
}

func (h *Handler) getReferrersFiltered(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]
	artifactType := vars["artifactType"]

	w.WriteHeader(http.StatusOK)
	fmt.Printf(`{"name":"%s", "digest":"%s", "artifactType":"%s", "referrers":[{"mediaType":"application/example", "digest":"sha256:def"}]}`, name, digest, artifactType)
}

func (h *Handler) getUploadStatus(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
