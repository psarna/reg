package reg

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const uiPageSize = 25

type uiData struct {
	Title, Repository, Manifest                                             string
	Stats                                                                   map[string]interface{}
	Repositories                                                            []string
	Tags                                                                    []map[string]string
	Layers, Uploads                                                         []map[string]interface{}
	RepositoryTags                                                          []string
	RepositoriesNext, TagsNext, LayersNext, UploadsNext, RepositoryTagsNext string
}

var uiTemplate = template.Must(template.New("ui").Funcs(template.FuncMap{
	"bytes": func(value interface{}) string {
		size, ok := value.(int64)
		if !ok {
			return "0 B"
		}
		units := []string{"B", "KB", "MB", "GB", "TB"}
		amount, unit := float64(size), 0
		for amount >= 1024 && unit < len(units)-1 {
			amount /= 1024
			unit++
		}
		return fmt.Sprintf("%.1f %s", amount, units[unit])
	},
	"bar": func(value, maximum interface{}) int {
		current, currentOK := value.(int)
		max, maxOK := maximum.(int)
		if !currentOK || !maxOK || max == 0 {
			return 0
		}
		return current * 100 / max
	},
	"maxStats": func(stats map[string]interface{}) int {
		maximum := 0
		for _, key := range []string{"repositories", "tags", "manifests", "layers"} {
			if value, ok := stats[key].(int); ok && value > maximum {
				maximum = value
			}
		}
		return maximum
	},
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}} · reg</title><style>
:root{font:16px system-ui,sans-serif;color-scheme:light;color:#17202a;background:#f4f6f8;--panel:#fff;--line:#dce2e8;--head:#eef2f5;--link:#1261a0;--bar:#1261a0}body{max-width:1100px;margin:0 auto;padding:2rem}a{color:var(--link);text-decoration:none}a:hover{text-decoration:underline}nav{display:flex;gap:1rem;margin:1rem 0 2rem;flex-wrap:wrap;align-items:center}.brand{font-size:1.8rem;font-weight:700;color:inherit}.theme{margin-left:auto;border:1px solid var(--line);border-radius:6px;background:var(--panel);color:inherit;padding:.35rem .6rem;cursor:pointer}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1rem}.card,table,.chart{background:var(--panel);border:1px solid var(--line);border-radius:8px}.card{padding:1rem}.number{display:block;font-size:1.7rem;font-weight:700;margin-top:.25rem}table{border-collapse:separate;border-spacing:0;width:100%;overflow:hidden}th,td{text-align:left;padding:.7rem;border-bottom:1px solid var(--line)}th{background:var(--head)}tr:last-child td{border-bottom:0}.muted{color:#66737f}code{font-size:.9em;word-break:break-all}.pager{display:flex;justify-content:flex-end;margin:1rem 0}.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1rem}.chart{padding:1rem}.bar{height:12px;background:var(--head);border-radius:10px;margin:.35rem 0 1rem;overflow:hidden}.bar i{display:block;height:100%;background:var(--bar);border-radius:10px}pre{background:var(--panel);border:1px solid var(--line);padding:1rem;overflow:auto}
:root[data-theme=dark]{color-scheme:dark;color:#e8edf2;background:#11161b;--panel:#1b2229;--line:#35414c;--head:#26313b;--link:#7fc4ff;--bar:#4ea8de}@media (prefers-color-scheme:dark){:root:not([data-theme=light]){color-scheme:dark;color:#e8edf2;background:#11161b;--panel:#1b2229;--line:#35414c;--head:#26313b;--link:#7fc4ff;--bar:#4ea8de}}
</style></head><body><a class="brand" href="/">reg</a><nav><a href="/">Dashboard</a><a href="/repositories">Repositories</a><a href="/tags">Tags</a><a href="/layers">Layers</a><a href="/uploads">Uploads</a><a href="/insights">Insights</a><button class="theme" type="button" onclick="toggleTheme()">Toggle theme</button></nav>
{{if eq .Title "Dashboard"}}<h1>Registry dashboard</h1><div class="cards"><div class="card">Repositories<span class="number">{{index .Stats "repositories"}}</span></div><div class="card">Tags<span class="number">{{index .Stats "tags"}}</span></div><div class="card">Manifests<span class="number">{{index .Stats "manifests"}}</span></div><div class="card">Layers<span class="number">{{index .Stats "layers"}}</span></div><div class="card">Storage<span class="number">{{bytes (index .Stats "total_size_bytes")}}</span></div></div><h2>Repositories</h2>{{template "repositories" .}}<h2>Recent tags</h2>{{template "tags" .}}
{{else if eq .Title "Repositories"}}<h1>Repositories</h1>{{template "repositories" .}}
{{else if eq .Title "Tags"}}<h1>Tags</h1>{{template "tags" .}}
{{else if eq .Title "Layers"}}<h1>Layers</h1>{{template "layers" .}}
{{else if eq .Title "Uploads"}}<h1>Upload sessions</h1>{{template "uploads" .}}
{{else if eq .Title "Insights"}}<h1>Insights</h1><p class="muted">A live summary of the current SQLite registry metadata.</p><div class="charts"><div class="chart"><h2>Object counts</h2>{{ $max := maxStats .Stats }}<div>Repositories <strong>{{index .Stats "repositories"}}</strong></div><div class="bar"><i style="width:{{bar (index .Stats "repositories") $max}}%"></i></div><div>Tags <strong>{{index .Stats "tags"}}</strong></div><div class="bar"><i style="width:{{bar (index .Stats "tags") $max}}%"></i></div><div>Manifests <strong>{{index .Stats "manifests"}}</strong></div><div class="bar"><i style="width:{{bar (index .Stats "manifests") $max}}%"></i></div><div>Layers <strong>{{index .Stats "layers"}}</strong></div><div class="bar"><i style="width:{{bar (index .Stats "layers") $max}}%"></i></div></div><div class="chart"><h2>Storage</h2><div class="cards"><div class="card">Total<span class="number">{{bytes (index .Stats "total_size_bytes")}}</span></div><div class="card">Active uploads<span class="number">{{index .Stats "active_uploads"}}</span></div></div><p class="muted">Layer storage reported by the SQLite metadata index.</p></div></div>
{{else if eq .Title "Repository"}}<h1>{{.Repository}}</h1><p><a href="/repositories">← all repositories</a></p><h2>Tags</h2><table><tr><th>Tag</th><th>Manifest</th></tr>{{range .RepositoryTags}}<tr><td><code>{{.}}</code></td><td><a href="/manifest?repository={{urlquery $.Repository}}&amp;tag={{urlquery .}}">view manifest</a></td></tr>{{else}}<tr><td colspan="2" class="muted">No tags found</td></tr>{{end}}</table>{{if .RepositoryTagsNext}}<p class="pager"><a href="{{.RepositoryTagsNext}}">Next 25 →</a></p>{{end}}
{{else}}<h1>Manifest</h1><p><a href="/repository?name={{urlquery .Repository}}">← {{.Repository}}</a></p><pre>{{.Manifest}}</pre>{{end}}
{{define "repositories"}}<table><tr><th>Repository</th></tr>{{range .Repositories}}<tr><td><a href="/repository?name={{urlquery .}}"><code>{{.}}</code></a></td></tr>{{else}}<tr><td class="muted">No repositories found</td></tr>{{end}}</table>{{if .RepositoriesNext}}<p class="pager"><a href="{{.RepositoriesNext}}">Next 25 →</a></p>{{end}}{{end}}
{{define "tags"}}<table><tr><th>Repository</th><th>Tag</th></tr>{{range .Tags}}<tr><td><a href="/repository?name={{urlquery .repository}}"><code>{{.repository}}</code></a></td><td><code>{{.tag}}</code></td></tr>{{else}}<tr><td colspan="2" class="muted">No tags found</td></tr>{{end}}</table>{{if .TagsNext}}<p class="pager"><a href="{{.TagsNext}}">Next 25 →</a></p>{{end}}{{end}}
{{define "layers"}}<table><tr><th>Digest</th><th>Media type</th><th>Size</th></tr>{{range .Layers}}<tr><td><code>{{.digest}}</code></td><td>{{.media_type}}</td><td>{{bytes .size}}</td></tr>{{else}}<tr><td colspan="3" class="muted">No layers found</td></tr>{{end}}</table>{{if .LayersNext}}<p class="pager"><a href="{{.LayersNext}}">Next 25 →</a></p>{{end}}{{end}}
{{define "uploads"}}<table><tr><th>Repository</th><th>Upload</th><th>Last activity</th><th>Uploaded</th></tr>{{range .Uploads}}<tr><td><code>{{.repository}}</code></td><td><code>{{.upload_id}}</code></td><td>{{.last_activity}}</td><td>{{bytes .uploaded_size}}</td></tr>{{else}}<tr><td colspan="4" class="muted">No active uploads</td></tr>{{end}}</table>{{if .UploadsNext}}<p class="pager"><a href="{{.UploadsNext}}">Next 25 →</a></p>{{end}}{{end}}
<script>const root=document.documentElement;const saved=localStorage.getItem('reg-theme');if(saved)root.dataset.theme=saved;function toggleTheme(){const next=root.dataset.theme==='dark'?'light':'dark';root.dataset.theme=next;localStorage.setItem('reg-theme',next)}</script></body></html>`))

func NewUIHandler(registry *Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		renderUI(w, uiDataFor(registry, "Dashboard", "", r.URL.Query().Get("continuationToken")))
	})
	mux.HandleFunc("/repositories", func(w http.ResponseWriter, r *http.Request) {
		renderUI(w, uiDataFor(registry, "Repositories", "", r.URL.Query().Get("continuationToken")))
	})
	mux.HandleFunc("/repository", func(w http.ResponseWriter, r *http.Request) {
		repository := r.URL.Query().Get("name")
		if repository == "" {
			http.Error(w, "repository is required", http.StatusBadRequest)
			return
		}
		tags, _, err := registry.db.ListTagsPage(repository, r.URL.Query().Get("continuationToken"), uiPageSize+1)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data := uiData{Title: "Repository", Repository: repository, RepositoryTags: tags}
		if len(tags) > uiPageSize {
			data.RepositoryTags = tags[:uiPageSize]
			data.RepositoryTagsNext = pageURL("/repository?name="+url.QueryEscape(repository), tags[uiPageSize-1])
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
	for _, page := range []string{"tags", "layers"} {
		page, title := page, pageTitle(page)
		mux.HandleFunc("/"+page, func(w http.ResponseWriter, r *http.Request) {
			renderUI(w, uiDataFor(registry, title, "", r.URL.Query().Get("continuationToken")))
		})
	}
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page < 0 {
			page = 0
		}
		uploads, err := registry.db.ListUploadSessionsPage(page*uiPageSize, uiPageSize+1)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data := uiData{Title: "Uploads", Uploads: uploads}
		if len(uploads) > uiPageSize {
			data.Uploads = uploads[:uiPageSize]
			data.UploadsNext = fmt.Sprintf("/uploads?page=%d", page+1)
		}
		renderUI(w, data)
	})
	mux.HandleFunc("/insights", func(w http.ResponseWriter, r *http.Request) { renderUI(w, uiDataFor(registry, "Insights", "", "")) })
	return mux
}

func pageTitle(page string) string {
	if page == "tags" {
		return "Tags"
	}
	return "Layers"
}
func pageURL(path, token string) string {
	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}
	return path + separator + "continuationToken=" + url.QueryEscape(token)
}

func uiDataFor(registry *Registry, title, repository, continuationToken string) uiData {
	data := uiData{Title: title, Repository: repository}
	data.Stats, _ = registry.getRegistryStats(nil)
	ctx := context.Background()
	token := stringPointer(continuationToken)
	repositories, repositoryToken, _ := registry.listRepositories(ctx, token, uiPageSize+1)
	data.Repositories, data.RepositoriesNext = trimRepositories(repositories, repositoryToken)
	tags, tagsToken, _ := registry.listAllTags(ctx, token, uiPageSize+1)
	data.Tags, data.TagsNext = trimTags(tags, tagsToken)
	layers, layersToken, _ := registry.listLayers(ctx, token, uiPageSize+1)
	data.Layers, data.LayersNext = trimLayers(layers, layersToken)
	return data
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func trimRepositories(rows []string, token *string) ([]string, string) {
	if len(rows) > uiPageSize {
		rows = rows[:uiPageSize]
		return rows, pageURL("/repositories", rows[len(rows)-1])
	}
	return rows, nextPage("/repositories", token)
}
func trimTags(rows []map[string]string, token *string) ([]map[string]string, string) {
	if len(rows) > uiPageSize {
		rows = rows[:uiPageSize]
		row := rows[len(rows)-1]
		return rows, pageURL("/tags", row["repository"]+":"+row["tag"])
	}
	return rows, nextPage("/tags", token)
}
func trimLayers(rows []map[string]interface{}, token *string) ([]map[string]interface{}, string) {
	if len(rows) > uiPageSize {
		rows = rows[:uiPageSize]
		return rows, pageURL("/layers", rows[len(rows)-1]["digest"].(string))
	}
	return rows, nextPage("/layers", token)
}
func nextPage(path string, token *string) string {
	if token == nil {
		return ""
	}
	return pageURL(path, *token)
}

func renderUI(w http.ResponseWriter, data uiData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
