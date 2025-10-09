package handlers

import "net/http"

func tenantIDFromRequest(r *http.Request) (string, bool) {
	tid, ok := r.Context().Value("tenant_id").(string)
	if !ok || tid == "" {
		return "", false
	}
	return tid, true
}
