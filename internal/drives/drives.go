// Package drives provides helpers for listing physical disks via geom.
package drives

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"raidraccoon/internal/config"
	"raidraccoon/internal/execwrap"
)

type Drive struct {
	Name        string `json:"name"`
	Mediasize   string `json:"mediasize"`
	Description string `json:"description"`
	Ident       string `json:"ident"`
}

// ListDrives parses `geom disk list` into a stable JSON-friendly form for the UI.
func ListDrives(ctx context.Context, cfg config.Config) ([]Drive, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.Geom, []string{"disk", "list"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var drives []Drive
	var current *Drive
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "Geom name:") {
			if current != nil {
				drives = append(drives, *current)
			}
			name := strings.TrimSpace(strings.TrimPrefix(line, "Geom name:"))
			current = &Drive{Name: name}
			continue
		}
		if current == nil {
			continue
		}
		switch {
		case strings.HasPrefix(line, "Mediasize:"):
			current.Mediasize = strings.TrimSpace(strings.TrimPrefix(line, "Mediasize:"))
		case strings.HasPrefix(line, "descr:"):
			current.Description = strings.TrimSpace(strings.TrimPrefix(line, "descr:"))
		case strings.HasPrefix(line, "ident:"):
			current.Ident = strings.TrimSpace(strings.TrimPrefix(line, "ident:"))
		}
	}
	if current != nil {
		drives = append(drives, *current)
	}
	return drives, nil
}

// ListLabels returns the label -> provider mapping from `geom label status`.
func ListLabels(ctx context.Context, cfg config.Config) (map[string]string, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.Geom, []string{"label", "status"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	out := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(line), "name") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		label := fields[0]
		provider := fields[len(fields)-1]
		if label == "" || provider == "" {
			continue
		}
		out[label] = provider
	}
	return out, nil
}

// CreateGPTLabel creates a `gpt/<label>` entry pointing at provider (e.g. da0p2).
func CreateGPTLabel(ctx context.Context, cfg config.Config, label, provider string) (execwrap.Result, error) {
	if strings.TrimSpace(label) == "" || strings.TrimSpace(provider) == "" {
		return execwrap.Result{ExitCode: 1, Stderr: "label and provider required"}, fmt.Errorf("label and provider required")
	}
	args := []string{"label", "label", "gpt/" + strings.TrimSpace(label), strings.TrimSpace(provider)}
	return execwrap.Run(ctx, cfg.Paths.Geom, args, nil, cfg.Limits)
}
