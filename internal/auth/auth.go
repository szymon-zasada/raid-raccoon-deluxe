// Package auth provides HTTP Basic authentication with salted SHA-256 verification.
package auth

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"raidraccoon/internal/config"
)

type ctxKey string

const userKey ctxKey = "rrd-user"

// UserFromContext returns the authenticated username, or "" if missing.
func UserFromContext(ctx context.Context) string {
	if v := ctx.Value(userKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// Middleware enforces HTTP Basic Auth for all requests under next.
// Passwords are verified by comparing a salted SHA-256 hash stored in config.
func Middleware(cfg config.AuthConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := parseBasic(r.Header.Get("Authorization"))
		if !ok {
			unauthorized(w)
			return
		}
		if user != cfg.Username {
			unauthorized(w)
			return
		}
		if cfg.SaltHex == "" || cfg.PasswordHashHex == "" {
			unauthorized(w)
			return
		}
		hash := config.HashPasswordHex(cfg.SaltHex, pass)
		if subtle.ConstantTimeCompare([]byte(hash), []byte(cfg.PasswordHashHex)) != 1 {
			unauthorized(w)
			return
		}
		ctx := context.WithValue(r.Context(), userKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func parseBasic(header string) (string, string, bool) {
	if header == "" {
		return "", "", false
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "basic") {
		return "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", false
	}
	pair := strings.SplitN(string(decoded), ":", 2)
	if len(pair) != 2 {
		return "", "", false
	}
	return pair[0], pair[1], true
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Basic realm=\"RaidRaccoon\"")
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}

var ErrUnauthorized = errors.New("unauthorized")
