// Package ui renders embedded HTML templates and serves static assets.
package ui

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"path/filepath"
	"sync"
)

//go:embed assets/templates/*.html
var templateFS embed.FS

//go:embed assets/static/*
var staticFS embed.FS

var (
	tmplCache = map[string]*template.Template{}
	tmplMu    sync.Mutex
)

// Render executes an embedded page template within the shared base layout.
func Render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t := getTemplate(name)
	_ = t.ExecuteTemplate(w, "base", data)
}

// StaticHandler serves embedded JS/CSS assets under /static/.
func StaticHandler() http.Handler {
	sub, _ := fs.Sub(staticFS, "assets/static")
	return http.FileServer(http.FS(sub))
}

func asciiArt() string {
	return asciiArtText
}

func getTemplate(name string) *template.Template {
	tmplMu.Lock()
	defer tmplMu.Unlock()
	if t, ok := tmplCache[name]; ok {
		return t
	}
	base := "assets/templates/base.html"
	page := filepath.Join("assets/templates", name+".html")
	t := template.Must(template.New("base").Funcs(template.FuncMap{
		"asciiArt": asciiArt,
	}).ParseFS(templateFS, base, page))
	tmplCache[name] = t
	return t
}
