// Package rsync provides helpers for running rsync via sudo.
package rsync

import (
	"context"
	"strings"

	"raidraccoon/internal/config"
	"raidraccoon/internal/execwrap"
)

func Run(ctx context.Context, cfg config.Config, source, target string, flags []string) (execwrap.Result, error) {
	args := make([]string, 0, len(flags)+2)
	for _, flag := range flags {
		if strings.TrimSpace(flag) == "" {
			continue
		}
		args = append(args, flag)
	}
	args = append(args, source, target)
	return execwrap.Run(ctx, cfg.Paths.Rsync, args, nil, cfg.Limits)
}

// SplitFlags parses comma-separated rsync flags from config/UI input.
func SplitFlags(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

// JoinFlags formats flags as a comma-separated string for persistence.
func JoinFlags(flags []string) string {
	if len(flags) == 0 {
		return ""
	}
	out := make([]string, 0, len(flags))
	for _, flag := range flags {
		value := strings.TrimSpace(flag)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return strings.Join(out, ",")
}
