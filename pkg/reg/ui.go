package reg

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
)

type uiData struct {
	Title          string
	Stats          map[string]interface{}
	Repositories   []string
	Tags           []map[string]string
	Layers         []map[string]interface{}
	Uploads        []map[string]interface{}
	Repository     string
	RepositoryTags []string
	Manifest       string
}

var uiTemplate = template.Must(template.New("ui").Funcs(template.FuncMap{
	"bytes": func(value interface{}) string {
		size, ok := value.(int64)
		if !ok {
			return "0 B"
		}
		units := []string{"B", "KB", "MB", "GB", "TB"}
		amount := float64(size)
		unit := 0
		for amount >= 1024 && unit < len(units)-1 {
			amount /= 1024
			unit++
		}
		return fmt.Sprintf("%.1f %s", amount, units[unit])
	},
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} · reg</title><style>
:root{font:16px system-ui,sans-serif;color:#17202a;background:#f4f6f8}body{max-width:1100px;margin:0 auto;padding:2rem}a{color:#1261a0;text-decoration:none}a:hover{text-decoration:underline}nav{display:flex;gap:1rem;margin:1rem 0 2rem;flex-wrap:wrap}.brand{font-size:1.8rem;font-weight:700;color:#17202a}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1rem}.card,table{background:white;border:1px solid #dce2e8;border-radius:8px}.card{padding:1rem}.number{display:block;font-size:1.7rem;font-weight:700;margin-top:.25rem}table{border-collapse:separate;border-spacing:0;width:100%;overflow:hidden}th,td{text-align:left;padding:.7rem;border-bottom:1px solid #e9edf0}th{background:#eef2f5}tr:last-child td{border-bottom:0}.muted{color:#66737f}code{font-size:.9em;word-break:break-all}
</style></head><body><a class="brand" href="/">reg</a><nav><a href="/">Dashboard</a><a href="/repositories">Repositories</a><a href="/tags">Tags</a><a href="/layers">Layers</a><a href="/uploads">Uploads</a></nav>
{{if eq .Title "Dashboard"}}<h1>Registry dashboard</h1><div class="cards">
<div class="card">Repositories<span class="number">{{index .Stats "repositories"}}</span></div><div class="card">Tags<span class="number">{{index .Stats "tags"}}</span></div><div class="card">Manifests<span class="number">{{index .Stats "manifests"}}</span></div><div class="card">Layers<span class="number">{{index .Stats "layers"}}</span></div><div class="card">Storage<span class="number">{{bytes (index .Stats "total_size_bytes")}}</span></div></div>
<h2>Repositories</h2>{{template "repositories" .}}<h2>Recent tags</h2>{{template "tags" .}}
{{else if eq .Title "Repositories"}}<h1>Repositories</h1>{{template "repositories" .}}
{{else if eq .Title "Tags"}}<h1>Tags</h1>{{template "tags" .}}
{{else if eq .Title "Layers"}}<h1>Layers</h1>{{template "layers" .}}
{{else if eq .Title "Uploads"}}<h1>Upload sessions</h1>{{template "uploads" .}}
{{else if eq .Title "Repository"}}<h1>{{.Repository}}</h1><p><a href="/repositories">← all repositories</a></p><h2>Tags</h2><table><tr><th>Tag</th><th>Manifest</th></tr>{{range .RepositoryTags}}<tr><td><code>{{.}}</code></td><td><a href="/manifest?repository={{urlquery $.Repository}}&amp;tag={{urlquery .}}">view manifest</a></td></tr>{{else}}<tr><td colspan="2" class="muted">No tags found</td></tr>{{end}}</table>
{{else}}<h1>Manifest</h1><p><a href="/repository?name={{urlquery .Repository}}">← {{.Repository}}</a></p><pre>{{.Manifest}}</pre>{{end}}
{{define "repositories"}}<table><tr><th>Repository</th></tr>{{range .Repositories}}<tr><td><a href="/repository?name={{urlquery .}}"><code>{{.}}</code></a></td></tr>{{else}}<tr><td class="muted">No repositories found</td></tr>{{end}}</table>{{end}}
{{define "tags"}}<table><tr><th>Repository</th><th>Tag</th></tr>{{range .Tags}}<tr><td><a href="/repository?name={{urlquery .repository}}"><code>{{.repository}}</code></a></td><td><code>{{.tag}}</code></td></tr>{{else}}<tr><td colspan="2" class="muted">No tags found</td></tr>{{end}}</table>{{end}}
{{define "layers"}}<table><tr><th>Digest</th><th>Media type</th><th>Size</th></tr>{{range .Layers}}<tr><td><code>{{.digest}}</code></td><td>{{.media_type}}</td><td>{{bytes .size}}</td></tr>{{else}}<tr><td colspan="3" class="muted">No layers found</td></tr>{{end}}</table>{{end}}
{{define "uploads"}}<table><tr><th>Repository</th><th>Upload</th><th>Last activity</th><th>Uploaded</th></tr>{{range .Uploads}}<tr><td><code>{{.repository}}</code></td><td><code>{{.upload_id}}</code></td><td>{{.last_activity}}</td><td>{{bytes .uploaded_size}}</td></tr>{{else}}<tr><td colspan="4" class="muted">No active uploads</td></tr>{{end}}</table>{{end}}
</body></html>`))

func NewUIHandler(registry *Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		renderUI(w, uiDataFor(registry, "Dashboard", ""))
	})
	mux.HandleFunc("/repositories", func(w http.ResponseWriter, r *http.Request) {
		renderUI(w, uiDataFor(registry, "Repositories", ""))
	})
	mux.HandleFunc("/repository", func(w http.ResponseWriter, r *http.Request) {
		repository := r.URL.Query().Get("name")
		if repository == "" {
			http.Error(w, "repository is required", http.StatusBadRequest)
			return
		}
		data := uiDataFor(registry, "Repository", repository)
		var err error
		data.RepositoryTags, err = registry.db.ListTags(repository)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		renderUI(w, data)
	})
	mux.HandleFunc("/manifest", func(w http.ResponseWriter, r *http.Request) {
		repository, tag := r.URL.Query().Get("repository"), r.URL.Query().Get("tag")
		if repository == "" || tag == "" {
			http.Error(w, "repository and tag are required", http.StatusBadRequest)
			return
		}
		manifest, err := registry.db.GetManifest(repository, tag)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		renderUI(w, uiData{Title: "Manifest", Repository: repository, Manifest: manifest})
	})
	mux.HandleFunc("/tags", func(w http.ResponseWriter, r *http.Request) { renderUI(w, uiDataFor(registry, "Tags", "")) })
	mux.HandleFunc("/layers", func(w http.ResponseWriter, r *http.Request) { renderUI(w, uiDataFor(registry, "Layers", "")) })
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) { renderUI(w, uiDataFor(registry, "Uploads", "")) })
	return mux
}

func uiDataFor(registry *Registry, title, repository string) uiData {
	data := uiData{Title: title, Repository: repository}
	data.Stats, _ = registry.getRegistryStats(nil)
	ctx := context.Background()
	data.Repositories, _, _ = registry.listRepositories(ctx, nil, 500)
	data.Tags, _, _ = registry.listAllTags(ctx, nil, 500)
	data.Layers, _, _ = registry.listLayers(ctx, nil, 500)
	data.Uploads, _ = registry.listUploadSessions(ctx)
	return data
}

func renderUI(w http.ResponseWriter, data uiData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
