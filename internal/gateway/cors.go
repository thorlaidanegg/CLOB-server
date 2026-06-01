package gateway

import "net/http"

// corsMiddleware allows browser frontends served from the given origins to call
// the API. CORS is a browser-only mechanism — non-browser clients (bots, mobile,
// server-to-server) are unaffected whether this is enabled or not.
//
// Pass []string{"*"} to allow any origin. An empty list disables CORS entirely
// (the middleware is not installed in that case — see NewRouter).
func corsMiddleware(allowed []string) func(http.Handler) http.Handler {
	allowAny := false
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		if o == "*" {
			allowAny = true
		}
		set[o] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && (allowAny || set[origin]) {
				if allowAny {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Add("Vary", "Origin")
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-ID")
				w.Header().Set("Access-Control-Max-Age", "300")
			}

			// Preflight requests end here.
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
