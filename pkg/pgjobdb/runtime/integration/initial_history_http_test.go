package integration_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Preserve the actual transport response when the owner-deferred chapter-zero
// race is surfaced as unsupported selection by the published remote client.
// This recorder changes neither the response nor the client's error mapping.
func initialHistoryHTTPDiagnostics(t *testing.T, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/primary-goal") {
			next.ServeHTTP(w, r)
			return
		}
		captured := httptest.NewRecorder()
		next.ServeHTTP(captured, r)
		if captured.Code == http.StatusNotFound && strings.TrimSpace(captured.Body.String()) == "chapter not found: chapterstore: resource not found: chapterstore: chapter not found" {
			t.Log("initial history HTTP response: status=404 error=chapter not found: chapterstore: resource not found: chapterstore: chapter not found")
		}
		for name, values := range captured.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(captured.Code)
		_, _ = w.Write(captured.Body.Bytes())
	})
}
