package server

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// spaHandler 从磁盘提供 Vite 生产构建，并为不存在的前端路由回退到 index.html。
// 它不接受写方法，也不会把请求路径直接拼到根目录外，避免控制台静态托管扩大文件暴露面。
type spaHandler struct {
	root  string
	files http.Handler
}

func newSPAHandler(root string) http.Handler {
	// filepath.Abs 失败时使用原值仍会被 http.Dir 限制在指定目录内；正常部署中该调用不会失败。
	absolute, err := filepath.Abs(root)
	if err != nil {
		absolute = root
	}
	return &spaHandler{root: absolute, files: http.FileServer(http.Dir(absolute))}
}

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// path.Clean 使用 URL 的斜杠语义；随后再用 filepath.Rel 做第二层目录逃逸检查。
	requested := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if requested == "" || requested == "." {
		requested = "index.html"
	}
	candidate := filepath.Join(h.root, filepath.FromSlash(requested))
	relative, err := filepath.Rel(h.root, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		http.NotFound(w, r)
		return
	}

	// 只有真实文件才按原路径返回；目录或未知前端路由统一交给 SPA 入口处理。
	if info, statErr := os.Stat(candidate); statErr != nil || info.IsDir() {
		requested = "index.html"
	}
	if strings.HasPrefix(requested, "assets/") {
		// Vite 文件名包含内容哈希，允许一年强缓存；入口 HTML 必须每次确认最新版本。
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}

	clone := r.Clone(r.Context())
	// net/http 会把显式 /index.html 规范化重定向为 ./；入口页直接用根路径即可避免
	// SPA 深层路由先收到一次无意义的 301。
	if requested == "index.html" {
		clone.URL.Path = "/"
	} else {
		clone.URL.Path = "/" + requested
	}
	h.files.ServeHTTP(w, clone)
}
