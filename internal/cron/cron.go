// Package cron manages schedule records stored in a cron file with metadata comments.
package cron

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Schedule struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Dataset   string            `json:"dataset"`
	Retention int               `json:"retention"`
	Prefix    string            `json:"prefix"`
	Enabled   bool              `json:"enabled"`
	Cron      CronSpec          `json:"schedule"`
	RawCron   string            `json:"cron"`
	Meta      map[string]string `json:"meta"`
}

// CronSpec is a 5-field cron schedule. If cron_user is set, the user field is handled separately.
type CronSpec struct {
	Minute string `json:"minute"`
	Hour   string `json:"hour"`
	Dom    string `json:"dom"`
	Month  string `json:"month"`
	Dow    string `json:"dow"`
}

type lineKind uint8

const (
	lineKeep lineKind = iota
	lineManaged
)

type File struct {
	Items     []Schedule
	Updated   string
	Lines     []string
	LineKinds []lineKind
}

type pendingMeta struct {
	index int
	meta  map[string]string
}

// Load reads path and returns both parsed schedules and the original file lines so
// non-managed cron lines can be preserved on Save.
func Load(path, cronUser string) (File, error) {
	info, statErr := os.Stat(path)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return File{Items: []Schedule{}, Updated: ""}, nil
		}
		return File{}, err
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var items []Schedule
	var lines []string
	var kinds []lineKind
	var pending *pendingMeta
	for scanner.Scan() {
		rawLine := scanner.Text()
		lines = append(lines, rawLine)
		kinds = append(kinds, lineKeep)
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# rrd:") {
			meta := parseMeta(strings.TrimPrefix(line, "# rrd:"))
			pending = &pendingMeta{index: len(lines) - 1, meta: meta}
			continue
		}
		if pending != nil {
			spec, rawCron, enabled, _, ok := parseCronLine(line, cronUser)
			if ok {
				jobType := pending.meta["type"]
				if jobType == "" {
					jobType = "snapshot"
				}
				item := Schedule{
					ID:        pending.meta["id"],
					Type:      jobType,
					Dataset:   pending.meta["dataset"],
					Prefix:    pending.meta["prefix"],
					Enabled:   enabled && pending.meta["enabled"] != "0",
					Retention: atoi(pending.meta["retention"], 0),
					Meta:      pending.meta,
					Cron:      spec,
					RawCron:   rawCron,
				}
				items = append(items, item)
				kinds[pending.index] = lineManaged
				kinds[len(lines)-1] = lineManaged
			}
			pending = nil
			continue
		}
		if item, ok := parseUnmanaged(line, cronUser); ok {
			items = append(items, item)
			kinds[len(lines)-1] = lineManaged
		}
	}
	updated := ""
	if statErr == nil {
		updated = info.ModTime().UTC().Format(time.RFC3339)
	}
	return File{Items: items, Updated: updated, Lines: lines, LineKinds: kinds}, nil
}

// Save writes schedules back to path while preserving non-managed lines.
// The managed block is rebuilt from Items each time.
func Save(path string, file File, binaryPath, cronUser string) (string, error) {
	if binaryPath == "" {
		binaryPath = "/usr/local/bin/raidraccoon"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	w := bufio.NewWriter(f)
	managedLines := buildManagedLines(file.Items, binaryPath, cronUser)

	wroteManaged := false
	if len(file.Lines) > 0 && len(file.LineKinds) == len(file.Lines) {
		for i, line := range file.Lines {
			if file.LineKinds[i] == lineManaged {
				if !wroteManaged {
					writeManaged(w, managedLines)
					wroteManaged = len(managedLines) > 0
				}
				continue
			}
			_, _ = w.WriteString(line + "\n")
		}
	}
	if !wroteManaged && len(managedLines) > 0 {
		if len(file.Lines) > 0 {
			last := file.Lines[len(file.Lines)-1]
			if strings.TrimSpace(last) != "" {
				_, _ = w.WriteString("\n")
			}
		}
		writeManaged(w, managedLines)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", err
	}
	return time.Now().UTC().Format(time.RFC3339), nil
}

// NewID returns a short random identifier suitable for schedule IDs.
func NewID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Upsert inserts or replaces item in items (by ID).
func Upsert(items []Schedule, item Schedule) []Schedule {
	if item.ID == "" {
		item.ID = NewID()
	}
	for i := range items {
		if items[i].ID == item.ID {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}

// Delete removes the schedule with id from items.
func Delete(items []Schedule, id string) []Schedule {
	var out []Schedule
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}

// Toggle flips Enabled for the schedule with id.
func Toggle(items []Schedule, id string) []Schedule {
	for i := range items {
		if items[i].ID == id {
			items[i].Enabled = !items[i].Enabled
		}
	}
	return items
}

func parseMeta(raw string) map[string]string {
	out := map[string]string{}
	parts := strings.Fields(raw)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func atoi(val string, def int) int {
	if val == "" {
		return def
	}
	var out int
	for _, r := range val {
		if r < '0' || r > '9' {
			return def
		}
		out = out*10 + int(r-'0')
	}
	return out
}

func boolToInt(val bool) int {
	if val {
		return 1
	}
	return 0
}

func stableID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:8])
}

func parseCronLine(line, cronUser string) (CronSpec, string, bool, []string, bool) {
	enabled := true
	raw := strings.TrimSpace(line)
	if strings.HasPrefix(raw, "#") {
		enabled = false
		raw = strings.TrimSpace(strings.TrimPrefix(raw, "#"))
	}
	if raw == "" {
		return CronSpec{}, "", enabled, nil, false
	}
	fields := strings.Fields(raw)
	if cronUser != "" {
		if len(fields) < 7 || fields[5] != cronUser {
			return CronSpec{}, "", enabled, nil, false
		}
		return CronSpec{
			Minute: fields[0],
			Hour:   fields[1],
			Dom:    fields[2],
			Month:  fields[3],
			Dow:    fields[4],
		}, strings.Join(fields[:5], " "), enabled, fields[6:], true
	}
	if len(fields) < 6 {
		return CronSpec{}, "", enabled, nil, false
	}
	return CronSpec{
		Minute: fields[0],
		Hour:   fields[1],
		Dom:    fields[2],
		Month:  fields[3],
		Dow:    fields[4],
	}, strings.Join(fields[:5], " "), enabled, fields[5:], true
}

func parseUnmanaged(line, cronUser string) (Schedule, bool) {
	spec, rawCron, enabled, cmd, ok := parseCronLine(line, cronUser)
	if !ok {
		return Schedule{}, false
	}
	binary, args, ok := parseSnapshotCommand(cmd)
	if !ok {
		return Schedule{}, false
	}
	dataset, retention, prefix, retentionSet := parseSnapshotArgs(args)
	if dataset == "" {
		return Schedule{}, false
	}
	if !retentionSet {
		retention = 7
	}
	seed := rawCron + "|" + strings.Join(append([]string{binary}, args...), " ")
	item := Schedule{
		ID:        stableID(seed),
		Type:      "snapshot",
		Dataset:   dataset,
		Retention: retention,
		Prefix:    prefix,
		Enabled:   enabled,
		Cron:      spec,
		RawCron:   rawCron,
		Meta:      map[string]string{"source": "cron", "type": "snapshot"},
	}
	return item, true
}

func parseSnapshotCommand(cmd []string) (string, []string, bool) {
	if len(cmd) < 2 {
		return "", nil, false
	}
	if filepath.Base(cmd[0]) != "raidraccoon" {
		return "", nil, false
	}
	if cmd[1] != "snapshot" {
		return "", nil, false
	}
	return cmd[0], cmd[2:], true
}

func parseSnapshotArgs(args []string) (string, int, string, bool) {
	dataset := ""
	retention := 0
	retentionSet := false
	prefix := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--dataset" && i+1 < len(args):
			dataset = args[i+1]
			i++
		case strings.HasPrefix(arg, "--dataset="):
			dataset = strings.TrimPrefix(arg, "--dataset=")
		case arg == "--retention" && i+1 < len(args):
			retention = atoi(args[i+1], 0)
			retentionSet = true
			i++
		case strings.HasPrefix(arg, "--retention="):
			retention = atoi(strings.TrimPrefix(arg, "--retention="), 0)
			retentionSet = true
		case arg == "--prefix" && i+1 < len(args):
			prefix = args[i+1]
			i++
		case strings.HasPrefix(arg, "--prefix="):
			prefix = strings.TrimPrefix(arg, "--prefix=")
		case arg == "--config" && i+1 < len(args):
			i++
		}
	}
	return dataset, retention, prefix, retentionSet
}

func buildManagedLines(items []Schedule, binaryPath, cronUser string) []string {
	if len(items) == 0 {
		return nil
	}
	lines := []string{"# Managed by RaidRaccoon Deluxe"}
	for i := range items {
		item := items[i]
		if item.ID == "" {
			item.ID = NewID()
		}
		kind := scheduleType(item)
		metaLine := fmt.Sprintf("# rrd:%s", metaString(item, kind))
		lines = append(lines, metaLine)
		fields := []string{item.Cron.Minute, item.Cron.Hour, item.Cron.Dom, item.Cron.Month, item.Cron.Dow}
		if cronUser != "" {
			fields = append(fields, cronUser)
		}
		cmdFields := buildCommandFields(item, kind, binaryPath)
		if len(cmdFields) == 0 {
			continue
		}
		fields = append(fields, cmdFields...)
		cronLine := strings.Join(fields, " ")
		if !item.Enabled {
			cronLine = "# " + cronLine
		}
		lines = append(lines, cronLine)
	}
	return lines
}

func writeManaged(w *bufio.Writer, lines []string) {
	if len(lines) == 0 {
		return
	}
	for _, line := range lines {
		_, _ = w.WriteString(line + "\n")
	}
}

func scheduleType(item Schedule) string {
	if item.Type != "" {
		return item.Type
	}
	if item.Meta != nil {
		if t := item.Meta["type"]; t != "" {
			return t
		}
	}
	return "snapshot"
}

func metaString(item Schedule, kind string) string {
	meta := map[string]string{}
	for key, value := range item.Meta {
		meta[key] = value
	}
	meta["id"] = item.ID
	meta["type"] = kind
	meta["enabled"] = fmt.Sprintf("%d", boolToInt(item.Enabled))
	switch kind {
	case "snapshot":
		if meta["dataset"] == "" {
			meta["dataset"] = item.Dataset
		}
		if meta["retention"] == "" {
			meta["retention"] = fmt.Sprintf("%d", item.Retention)
		}
		if meta["prefix"] == "" {
			meta["prefix"] = item.Prefix
		}
	case "replication":
		if meta["retention"] == "" {
			meta["retention"] = fmt.Sprintf("%d", item.Retention)
		}
		if meta["prefix"] == "" {
			meta["prefix"] = item.Prefix
		}
	}
	keys := []string{}
	for key := range meta {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := meta[key]
		if value == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%s", key, value))
	}
	return strings.Join(parts, " ")
}

func buildCommandFields(item Schedule, kind, binaryPath string) []string {
	switch kind {
	case "snapshot":
		dataset := item.Dataset
		if dataset == "" && item.Meta != nil {
			dataset = item.Meta["dataset"]
		}
		if dataset == "" {
			return nil
		}
		fields := []string{binaryPath, "snapshot", "--dataset", dataset}
		retention := item.Retention
		if retention == 0 && item.Meta != nil {
			retention = atoi(item.Meta["retention"], 0)
		}
		if retention > 0 {
			fields = append(fields, "--retention", fmt.Sprintf("%d", retention))
		}
		prefix := item.Prefix
		if prefix == "" && item.Meta != nil {
			prefix = item.Meta["prefix"]
		}
		if prefix != "" {
			fields = append(fields, "--prefix", prefix)
		}
		return fields
	case "replication":
		meta := item.Meta
		if meta == nil {
			meta = map[string]string{}
		}
		source := meta["source"]
		target := meta["target"]
		if source == "" || target == "" {
			return nil
		}
		fields := []string{binaryPath, "replicate", "--source", source, "--target", target}
		prefix := meta["prefix"]
		if prefix == "" {
			prefix = item.Prefix
		}
		if prefix != "" {
			fields = append(fields, "--prefix", prefix)
		}
		retention := atoi(meta["retention"], 0)
		if retention == 0 {
			retention = item.Retention
		}
		if retention > 0 {
			fields = append(fields, "--retention", fmt.Sprintf("%d", retention))
		}
		if meta["recursive"] == "1" {
			fields = append(fields, "--recursive")
		}
		if meta["force"] == "1" {
			fields = append(fields, "--force")
		}
		return fields
	case "rsync":
		meta := item.Meta
		if meta == nil {
			meta = map[string]string{}
		}
		source := meta["source"]
		target := meta["target"]
		if source == "" || target == "" {
			return nil
		}
		fields := []string{binaryPath, "rsync", "--source", source, "--target", target}
		if flags := meta["flags"]; flags != "" {
			fields = append(fields, "--flags", flags)
		}
		return fields
	default:
		return nil
	}
}
