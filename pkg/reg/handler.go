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
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type Handler struct {
	registry *Registry
}

func NewRouter(ctx context.Context, registry *Registry) (*mux.Router, error) {
	h := &Handler{
		registry: registry,
	}

	r := mux.NewRouter()
	apiRouter := r.PathPrefix("/v2").Subrouter()

	// end-1: Check API support
	apiRouter.Handle("/", http.HandlerFunc(h.checkAPISupport)).Methods("GET")

	// end-2: Get blob
	apiRouter.Handle("/{name:.*}/blobs/{digest}", http.HandlerFunc(h.getBlob)).Methods("GET", "HEAD")

	// end-3: Get manifest
	apiRouter.Handle("/{name:.*}/manifests/{reference}", http.HandlerFunc(h.getManifest)).Methods("GET", "HEAD")

	// end-4b: Start upload with digest
	apiRouter.Handle("/{name:.*}/blobs/uploads/", http.HandlerFunc(h.startUploadWithDigest)).
		Methods("POST").
		Queries("digest", "{digest}")

	// end-4a: Start upload
	apiRouter.Handle("/{name:.*}/blobs/uploads/", http.HandlerFunc(h.startUpload)).Methods("POST")

	// end-6: Complete upload
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.completeUpload)).
		Methods("PUT", "PATCH").
		Queries("digest", "{digest}")

	// end-5: Upload chunk
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.uploadChunk)).Methods("PUT", "PATCH")

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

	// end-14: Cancel upload
	apiRouter.Handle("/{name:.*}/blobs/uploads/{reference}", http.HandlerFunc(h.cancelUpload)).Methods("DELETE")

	// custom endpoint 1: list all repositories
	apiRouter.Handle("/repositories", http.HandlerFunc(http.HandlerFunc(h.listRepositories))).
		Methods("GET")

	return r, nil
}

func (h *Handler) checkAPISupport(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) getBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]
	presignedURL, err := h.registry.getBlobRedirect(r.Context(), name, digest, r.Method)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, fmt.Sprintf("blob not found: %v", err), http.StatusNotFound)
			return
		}
		slog.Error("error getting blob redirect", "error", err)
		http.Error(w, fmt.Sprintf("error getting blob redirect: %v", err), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, presignedURL, http.StatusFound)
}

func (h *Handler) getManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	manifest, manifestBytes, err := h.registry.getManifest(r.Context(), name, reference)
	if err != nil {
		slog.Error("error getting manifest", "error", err)
		if errors.Is(err, fs.ErrNotExist) {
			http.Error(w, fmt.Sprintf("manifest not found: %v", err), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("error getting manifest: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", manifest.MediaType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(manifestBytes)))
	_, err = w.Write(manifestBytes)
	if err != nil {
		slog.Error("error writing manifest response", "error", err)
		http.Error(w, fmt.Sprintf("error writing manifest response: %v", err), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) startUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	uploadId := uuid.New().String()

	err := h.registry.startUpload(r.Context(), name, uploadId)
	if err != nil {
		slog.Error("error starting upload", "error", err)
		http.Error(w, fmt.Sprintf("error starting upload: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("v2/%s/blobs/uploads/%s", name, uploadId))
	w.WriteHeader(http.StatusAccepted)
}

func parseContentRange(fRange string) (int64, int64, error) {
	var startOffset, endOffset int64
	_, err := fmt.Sscanf(fRange, "bytes=%d-%d", &startOffset, &endOffset)
	if err != nil {
		return 0, int64(1<<63 - 1), nil
	}
	// NOTICE: The endOffset is inclusive in the Content-Range header, but we need to
	// convert it to exclusive for our internal representation.
	endOffset++
	return startOffset, endOffset, nil
}

func (h *Handler) startUploadWithDigest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	digest := vars["digest"]
	uploadId := uuid.New().String()

	err := h.registry.startUpload(r.Context(), name, uploadId)
	if err != nil {
		slog.Error("error starting upload", "error", err)
		http.Error(w, fmt.Sprintf("error starting upload: %v", err), http.StatusInternalServerError)
		return
	}

	if len(r.Header.Get("Content-Length")) > 0 && r.Header.Get("Content-Length") != "0" {
		_, err := h.registry.uploadChunk(r.Context(), uploadId, 0, r.Body)
		if err != nil {
			slog.Error("error uploading chunk", "error", err)
			http.Error(w, fmt.Sprintf("error uploading chunk: %v", err), http.StatusInternalServerError)
			return
		}

		err = h.registry.completeUpload(r.Context(), uploadId, digest)
		if err != nil {
			slog.Error("error completing upload", "error", err)
			http.Error(w, fmt.Sprintf("error completing upload: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
		w.WriteHeader(http.StatusCreated)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("v2/%s/blobs/uploads/%s", name, uploadId))
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) uploadChunk(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	fRange := r.Header.Get("Content-Range")
	startOffset, endOffset, err := parseContentRange(fRange)
	if err != nil {
		slog.Error("error parsing content range", "error", err)
		http.Error(w, fmt.Sprintf("error parsing content range: %v", err), http.StatusBadRequest)
		return
	}
	slog.Debug("uploadChunk", "ref", reference, "range", fRange, "start", startOffset, "end", endOffset)

	n, err := h.registry.uploadChunk(r.Context(), reference, startOffset, r.Body)
	if err != nil {
		slog.Error("error uploading chunk", "error", err)
		http.Error(w, fmt.Sprintf("error uploading chunk: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference))
	w.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", startOffset, startOffset+n-1))
	w.WriteHeader(http.StatusAccepted)
}

func (h *Handler) completeUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]
	digest := vars["digest"]

	err := h.registry.completeUpload(r.Context(), reference, digest)
	if err != nil {
		slog.Error("error completing upload", "error", err)
		http.Error(w, fmt.Sprintf("error completing upload: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/%s", name, digest))
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) putManifest(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]
	slog.Warn("putManifest", "name", name, "reference", reference)

	manifestBytes, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("error reading manifest body", "error", err)
		http.Error(w, fmt.Sprintf("error reading manifest body: %v", err), http.StatusInternalServerError)
		return
	}
	err = h.registry.putManifest(r.Context(), name, reference, manifestBytes)
	if err != nil {
		slog.Error("error putting manifest", "error", err)
		http.Error(w, fmt.Sprintf("error putting manifest: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/v2/%s/manifests/%s", name, reference))
	w.WriteHeader(http.StatusCreated)
	fmt.Printf("Put manifest for %s with reference %s\n", name, reference)
}

type tags struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func (h *Handler) listTags(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	repoTags, err := h.registry.listTags(r.Context(), name)
	if err != nil {
		slog.Error("error listing tags", "error", err)
		http.Error(w, fmt.Sprintf("error listing tags: %v", err), http.StatusInternalServerError)
		return
	}

	marshaledTags, err := json.Marshal(tags{
		Name: name,
		Tags: repoTags,
	})
	if err != nil {
		slog.Error("error marshalling tags", "error", err)
		http.Error(w, fmt.Sprintf("error marshalling tags: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(marshaledTags)
	if err != nil {
		slog.Error("error writing tags response", "error", err)
		http.Error(w, fmt.Sprintf("error writing tags response: %v", err), http.StatusInternalServerError)
		return
	}
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
	vars := mux.Vars(r)
	name := vars["name"]
	reference := vars["reference"]

	_, _, uploadedSize, err := h.registry.getUploadSession(reference)
	if err != nil {
		slog.Error("error getting upload status", "error", err)
		http.Error(w, fmt.Sprintf("error getting upload status: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", name, reference))
	if uploadedSize > 0 {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", uploadedSize-1))
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) cancelUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	reference := vars["reference"]

	err := h.registry.abortUpload(r.Context(), reference)
	if err != nil {
		slog.Error("error canceling upload", "error", err)
		http.Error(w, fmt.Sprintf("error canceling upload: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) listRepositories(w http.ResponseWriter, r *http.Request) {
	var continuationToken *string
	if token := r.URL.Query().Get("continuationToken"); token != "" {
		continuationToken = &token
	}
	nStr := r.URL.Query().Get("n")
	n, err := strconv.Atoi(nStr)
	if err != nil {
		n = 64
	}
	repositories, continuationToken, err := h.registry.listRepositories(r.Context(), continuationToken, n)
	if err != nil {
		slog.Error("error listing repositories", "error", err)
		http.Error(w, fmt.Sprintf("error listing repositories: %v", err), http.StatusInternalServerError)
		return
	}

	marshaledRepos, err := json.Marshal(repositories)
	if err != nil {
		slog.Error("error marshalling repositories", "error", err)
		http.Error(w, fmt.Sprintf("error marshalling repositories: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if continuationToken != nil {
		w.Header().Set(
			"Link",
			fmt.Sprintf(
				"<%s/v2/repositories?continuationToken=%s&n=%d>; rel=\"next\"",
				r.URL.Scheme+"://"+r.URL.Host,
				url.QueryEscape(*continuationToken),
				n,
			),
		)
	}
	_, err = w.Write(marshaledRepos)
	if err != nil {
		slog.Error("error writing repositories response", "error", err)
		http.Error(w, fmt.Sprintf("error writing repositories response: %v", err), http.StatusInternalServerError)
		return
	}
}
