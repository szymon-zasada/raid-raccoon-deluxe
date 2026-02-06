// Package zfs provides helpers for listing pools, datasets, and snapshots using sudo.
package zfs

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"raidraccoon/internal/config"
	"raidraccoon/internal/execwrap"
)

type Pool struct {
	Name   string `json:"name"`
	Size   string `json:"size"`
	Alloc  string `json:"alloc"`
	Free   string `json:"free"`
	Health string `json:"health"`
}

// ImportablePool represents a pool listed by `zpool import`.
type ImportablePool struct {
	Name  string `json:"name"`
	ID    string `json:"id,omitempty"`
	State string `json:"state,omitempty"`
}

// PoolDevice describes a vdev line from `zpool list -v` for inventory views.
type PoolDevice struct {
	Name  string `json:"name"`
	Pool  string `json:"pool"`
	Role  string `json:"role"`
	Size  string `json:"size"`
	Alloc string `json:"alloc"`
	Free  string `json:"free"`
}

// Dataset represents a filesystem/volume row from `zfs list`.
type Dataset struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Used       string `json:"used"`
	Available  string `json:"available"`
	Referenced string `json:"referenced"`
	Mountpoint string `json:"mountpoint"`
}

// Mount represents mount state from `zfs list -t filesystem`.
type Mount struct {
	Name       string `json:"name"`
	Mountpoint string `json:"mountpoint"`
	Canmount   string `json:"canmount"`
	Mounted    bool   `json:"mounted"`
}

// Snapshot is a lightweight snapshot listing entry.
type Snapshot struct {
	Name    string `json:"name"`
	Created string `json:"created"`
}

// ListPools returns ZFS pools with basic health/space fields.
func ListPools(ctx context.Context, cfg config.Config) ([]Pool, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.ZPool, []string{"list", "-H", "-o", "name,size,alloc,free,health"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var pools []Pool
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 5 {
			parts = strings.Fields(line)
		}
		if len(parts) < 5 {
			continue
		}
		pools = append(pools, Pool{parts[0], parts[1], parts[2], parts[3], parts[4]})
	}
	return pools, nil
}

// ListImportablePools returns pools visible via `zpool import` (not currently imported).
func ListImportablePools(ctx context.Context, cfg config.Config) ([]ImportablePool, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.ZPool, []string{"import"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		needle := "no pools available to import"
		if strings.Contains(strings.ToLower(res.Stdout), needle) || strings.Contains(strings.ToLower(res.Stderr), needle) {
			return []ImportablePool{}, nil
		}
		return nil, fmt.Errorf(res.Stderr)
	}
	var pools []ImportablePool
	var current *ImportablePool
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "pool:") {
			if current != nil && current.Name != "" {
				pools = append(pools, *current)
			}
			name := strings.TrimSpace(strings.TrimPrefix(line, "pool:"))
			current = &ImportablePool{Name: name}
			continue
		}
		if current == nil {
			continue
		}
		if strings.HasPrefix(line, "id:") {
			current.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
			continue
		}
		if strings.HasPrefix(line, "state:") {
			current.State = strings.TrimSpace(strings.TrimPrefix(line, "state:"))
			continue
		}
	}
	if current != nil && current.Name != "" {
		pools = append(pools, *current)
	}
	return pools, nil
}

// ImportPool imports a pool by name.
func ImportPool(ctx context.Context, cfg config.Config, identifier string) (execwrap.Result, error) {
	if identifier == "" {
		return execwrap.Result{}, fmt.Errorf("pool identifier required")
	}
	return execwrap.Run(ctx, cfg.Paths.ZPool, []string{"import", identifier}, nil, cfg.Limits)
}

// PoolStatus returns `zpool status -v` output for one pool.
func PoolStatus(ctx context.Context, cfg config.Config, pool string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.ZPool, []string{"status", "-v", pool}, nil, cfg.Limits)
}

func ListPoolDevices(ctx context.Context, cfg config.Config) ([]PoolDevice, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.ZPool, []string{"list", "-v", "-H", "-o", "name,size,alloc,free"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var devices []PoolDevice
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	currentPool := ""
	currentRole := "data"
	roleDepth := -1
	for scanner.Scan() {
		raw := scanner.Text()
		if strings.TrimSpace(raw) == "" {
			continue
		}
		depth := indentDepth(raw)
		fields := strings.Fields(raw)
		if len(fields) < 4 {
			continue
		}
		name := fields[0]
		size := fields[1]
		alloc := fields[2]
		free := fields[3]
		if depth == 0 {
			currentPool = name
			currentRole = "data"
			roleDepth = -1
			continue
		}
		if roleDepth >= 0 && depth <= roleDepth {
			currentRole = "data"
			roleDepth = -1
		}
		if sectionRole, ok := vdevSectionRole(name); ok {
			currentRole = sectionRole
			roleDepth = depth
			continue
		}
		if isVdevGroup(name) {
			continue
		}
		devices = append(devices, PoolDevice{
			Name:  name,
			Pool:  currentPool,
			Role:  currentRole,
			Size:  size,
			Alloc: alloc,
			Free:  free,
		})
	}
	return devices, nil
}

func ListDatasets(ctx context.Context, cfg config.Config) ([]Dataset, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.ZFS, []string{"list", "-H", "-t", "filesystem,volume", "-o", "name,type,used,avail,refer,mountpoint"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var datasets []Dataset
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 6 {
			parts = strings.Fields(line)
		}
		if len(parts) < 6 {
			continue
		}
		datasets = append(datasets, Dataset{parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]})
	}
	return datasets, nil
}

func ListMounts(ctx context.Context, cfg config.Config) ([]Mount, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.ZFS, []string{"list", "-H", "-t", "filesystem", "-o", "name,mountpoint,canmount,mounted"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var mounts []Mount
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 4 {
			parts = strings.Fields(line)
		}
		if len(parts) < 4 {
			continue
		}
		mounted := strings.EqualFold(parts[3], "yes") || strings.EqualFold(parts[3], "on") || strings.EqualFold(parts[3], "true")
		mounts = append(mounts, Mount{
			Name:       parts[0],
			Mountpoint: parts[1],
			Canmount:   parts[2],
			Mounted:    mounted,
		})
	}
	return mounts, nil
}

func ListSnapshots(ctx context.Context, cfg config.Config, dataset string) ([]Snapshot, error) {
	args := []string{"list", "-H", "-t", "snapshot", "-o", "name,creation", "-s", "creation"}
	if dataset != "" {
		args = append(args, dataset)
	}
	res, err := execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var snaps []Snapshot
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			parts = strings.Fields(line)
		}
		if len(parts) < 2 {
			continue
		}
		snaps = append(snaps, Snapshot{Name: parts[0], Created: parts[1]})
	}
	return snaps, nil
}

func CreateSnapshot(ctx context.Context, cfg config.Config, dataset, name string, recursive bool) (execwrap.Result, error) {
	args := []string{"snapshot"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, dataset+"@"+name)
	return execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
}

func DestroySnapshot(ctx context.Context, cfg config.Config, snapshot string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.ZFS, []string{"destroy", snapshot}, nil, cfg.Limits)
}

func MountDataset(ctx context.Context, cfg config.Config, dataset string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.ZFS, []string{"mount", dataset}, nil, cfg.Limits)
}

func UnmountDataset(ctx context.Context, cfg config.Config, dataset string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.ZFS, []string{"unmount", dataset}, nil, cfg.Limits)
}

func EnforceRetention(ctx context.Context, cfg config.Config, dataset, prefix string, retention int) ([]string, error) {
	if retention <= 0 {
		return nil, nil
	}
	snaps, err := ListSnapshots(ctx, cfg, dataset)
	if err != nil {
		return nil, err
	}
	var filtered []Snapshot
	for _, snap := range snaps {
		parts := strings.SplitN(snap.Name, "@", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.HasPrefix(parts[1], prefix) {
			filtered = append(filtered, snap)
		}
	}
	if len(filtered) <= retention {
		return nil, nil
	}
	var destroyed []string
	for i := 0; i < len(filtered)-retention; i++ {
		res, err := DestroySnapshot(ctx, cfg, filtered[i].Name)
		if err != nil {
			return destroyed, err
		}
		if res.ExitCode != 0 {
			return destroyed, fmt.Errorf(res.Stderr)
		}
		destroyed = append(destroyed, filtered[i].Name)
	}
	return destroyed, nil
}

// ValidateDataset enforces the allowlist in cfg.ZFS.AllowedPrefixes.
func ValidateDataset(cfg config.Config, dataset string) bool {
	for _, prefix := range cfg.ZFS.AllowedPrefixes {
		if dataset == prefix || strings.HasPrefix(dataset, prefix+"/") {
			return true
		}
	}
	return false
}

func ValidPoolName(name string) bool {
	if name == "" {
		return false
	}
	if strings.Contains(name, "/") {
		return false
	}
	return validToken(name)
}

func CreatePool(ctx context.Context, cfg config.Config, name string, vdevs []string, cache []string) (execwrap.Result, error) {
	if name == "" {
		return execwrap.Result{}, fmt.Errorf("pool name required")
	}
	if len(vdevs) == 0 {
		return execwrap.Result{}, fmt.Errorf("at least one vdev required")
	}
	args := []string{"create", name}
	args = append(args, vdevs...)
	if len(cache) > 0 {
		args = append(args, "cache")
		args = append(args, cache...)
	}
	return execwrap.Run(ctx, cfg.Paths.ZPool, args, nil, cfg.Limits)
}

func SetPoolProperty(ctx context.Context, cfg config.Config, pool, prop, value string) (execwrap.Result, error) {
	if pool == "" || prop == "" || value == "" {
		return execwrap.Result{}, fmt.Errorf("pool, property, and value required")
	}
	return execwrap.Run(ctx, cfg.Paths.ZPool, []string{"set", fmt.Sprintf("%s=%s", prop, value), pool}, nil, cfg.Limits)
}

func L2ARCSize(ctx context.Context, cfg config.Config) (int64, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.Sysctl, []string{"kstat.zfs.misc.arcstats.l2_size"}, nil, cfg.Limits)
	if err != nil {
		return 0, err
	}
	if res.ExitCode != 0 {
		return 0, fmt.Errorf(res.Stderr)
	}
	value, ok := parseSysctlInt(res.Stdout)
	if !ok {
		return 0, fmt.Errorf("unable to parse l2_size")
	}
	return value, nil
}

func PoolCacheDevices(ctx context.Context, cfg config.Config, pool string) ([]string, error) {
	res, err := PoolStatus(ctx, cfg, pool)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var devices []string
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	inCache := false
	cacheIndent := -1
	for scanner.Scan() {
		raw := scanner.Text()
		if strings.TrimSpace(raw) == "" {
			continue
		}
		trimmed := strings.TrimLeft(raw, " \t")
		if strings.EqualFold(strings.TrimSpace(trimmed), "cache") {
			inCache = true
			cacheIndent = len(raw) - len(trimmed)
			continue
		}
		if !inCache {
			continue
		}
		indent := len(raw) - len(trimmed)
		if indent <= cacheIndent {
			inCache = false
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 0 {
			continue
		}
		name := fields[0]
		if name == "" {
			continue
		}
		devices = append(devices, name)
	}
	return devices, nil
}

// ValidDatasetName is a syntactic check (not an allowlist check).
func ValidDatasetName(name string) bool {
	if name == "" {
		return false
	}
	if strings.Contains(name, "@") {
		return false
	}
	parts := strings.Split(name, "/")
	for _, part := range parts {
		if part == "" || !validToken(part) {
			return false
		}
	}
	return true
}

// BuildSnapshotName produces a timestamped snapshot token for UI/cron usage.
func BuildSnapshotName(prefix string, t time.Time) string {
	if prefix == "" {
		prefix = "snapshot"
	}
	return fmt.Sprintf("%s-%s", prefix, t.Format("20060102-150405"))
}

func ValidSnapshotName(name string) bool {
	if name == "" {
		return false
	}
	return validToken(name) && !strings.Contains(name, "@")
}

func ValidSnapshotToken(token string) bool {
	if token == "" {
		return false
	}
	return validToken(token)
}

func validToken(token string) bool {
	for _, r := range token {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '-', '_', '.', ':':
			continue
		default:
			return false
		}
	}
	return true
}

// ReplicateDataset runs a `zfs send | zfs recv` replication job, optionally enforcing retention.
func ReplicateDataset(ctx context.Context, cfg config.Config, source, target, prefix string, retention int, recursive, force bool) (execwrap.Result, error) {
	if prefix == "" {
		if cfg.ZFS.SnapshotPrefix != "" {
			prefix = cfg.ZFS.SnapshotPrefix + "-repl"
		} else {
			prefix = "replication"
		}
	}
	name := BuildSnapshotName(prefix, time.Now())
	createRes, err := CreateSnapshot(ctx, cfg, source, name, recursive)
	if err != nil || createRes.ExitCode != 0 {
		if err != nil {
			return createRes, err
		}
		return createRes, fmt.Errorf(createRes.Stderr)
	}

	snaps, err := ListSnapshots(ctx, cfg, source)
	if err != nil {
		return execwrap.Result{ExitCode: 1, Stderr: err.Error()}, err
	}
	matches := snapshotsWithPrefix(snaps, prefix)
	if len(matches) == 0 {
		return execwrap.Result{ExitCode: 1, Stderr: "no replication snapshots found"}, fmt.Errorf("no replication snapshots found")
	}
	curr := source + "@" + name
	prev := ""
	index := -1
	for i, snap := range matches {
		if snap == curr {
			index = i
			break
		}
	}
	if index == -1 {
		index = len(matches) - 1
		curr = matches[index]
	}
	if index > 0 {
		prev = matches[index-1]
	}

	sendArgs := []string{"send"}
	if recursive {
		sendArgs = append(sendArgs, "-R")
	}
	if prev != "" {
		sendArgs = append(sendArgs, "-I", prev)
	}
	sendArgs = append(sendArgs, curr)

	recvArgs := []string{"recv"}
	if force {
		recvArgs = append(recvArgs, "-F")
	}
	recvArgs = append(recvArgs, target)

	pipeRes, err := runZfsPipeline(ctx, cfg, sendArgs, recvArgs)
	if err != nil || pipeRes.ExitCode != 0 {
		return pipeRes, err
	}

	if retention > 0 {
		_, _ = EnforceRetention(ctx, cfg, source, prefix, retention)
		_, _ = EnforceRetention(ctx, cfg, target, prefix, retention)
	}
	return pipeRes, nil
}

func snapshotsWithPrefix(snaps []Snapshot, prefix string) []string {
	out := []string{}
	if prefix == "" {
		return out
	}
	for _, snap := range snaps {
		parts := strings.SplitN(snap.Name, "@", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.HasPrefix(parts[1], prefix) {
			out = append(out, snap.Name)
		}
	}
	return out
}

type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int64
	truncated bool
	mu        sync.Mutex
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limit <= 0 {
		l.limit = 1 << 20
	}
	if int64(l.buf.Len()) >= l.limit {
		l.truncated = true
		return len(p), nil
	}
	remain := int(l.limit - int64(l.buf.Len()))
	if len(p) > remain {
		_, _ = l.buf.Write(p[:remain])
		l.truncated = true
		return len(p), nil
	}
	return l.buf.Write(p)
}

func (l *limitedBuffer) String() string {
	return l.buf.String()
}

func runZfsPipeline(ctx context.Context, cfg config.Config, sendArgs, recvArgs []string) (execwrap.Result, error) {
	limit := cfg.Limits.MaxOutputBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Limits.MaxRuntimeSeconds)*time.Second)
	if cfg.Limits.MaxRuntimeSeconds <= 0 {
		execCtx, cancel = context.WithTimeout(ctx, 120*time.Second)
	}
	defer cancel()

	sendCmd := exec.CommandContext(execCtx, "sudo", append([]string{"-n", cfg.Paths.ZFS}, sendArgs...)...)
	recvCmd := exec.CommandContext(execCtx, "sudo", append([]string{"-n", cfg.Paths.ZFS}, recvArgs...)...)

	reader, writer := io.Pipe()
	sendCmd.Stdout = writer
	recvCmd.Stdin = reader

	errBuf := &limitedBuffer{limit: limit}
	sendCmd.Stderr = errBuf
	recvCmd.Stderr = errBuf

	if err := recvCmd.Start(); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		return execwrap.Result{ExitCode: 1, Stderr: err.Error()}, err
	}
	if err := sendCmd.Start(); err != nil {
		_ = writer.Close()
		_ = reader.Close()
		_ = recvCmd.Process.Kill()
		return execwrap.Result{ExitCode: 1, Stderr: err.Error()}, err
	}

	sendErr := sendCmd.Wait()
	_ = writer.Close()
	recvErr := recvCmd.Wait()
	_ = reader.Close()

	sendExit := exitCodeFromErr(sendErr)
	recvExit := exitCodeFromErr(recvErr)
	exitCode := 0
	if sendExit != 0 {
		exitCode = sendExit
	}
	if recvExit != 0 {
		exitCode = recvExit
	}
	if sendErr != nil || recvErr != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = "zfs replication failed"
		}
		return execwrap.Result{ExitCode: exitCode, Stderr: msg, Truncated: errBuf.truncated}, fmt.Errorf(msg)
	}
	return execwrap.Result{ExitCode: exitCode, Stderr: errBuf.String(), Truncated: errBuf.truncated}, nil
}

func exitCodeFromErr(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 124
	}
	return 1
}

func parseSysctlInt(output string) (int64, bool) {
	fields := strings.Fields(output)
	for i := len(fields) - 1; i >= 0; i-- {
		token := strings.TrimSuffix(strings.TrimSpace(fields[i]), ":")
		if token == "" {
			continue
		}
		if !isDigits(token) {
			continue
		}
		val, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			return 0, false
		}
		return val, true
	}
	return 0, false
}

func isDigits(value string) bool {
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return value != ""
}

func indentDepth(line string) int {
	if line == "" {
		return 0
	}
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == line {
		return 0
	}
	prefix := line[:len(line)-len(trimmed)]
	depth := 0
	for _, r := range prefix {
		if r == '\t' {
			depth++
		}
	}
	if depth == 0 {
		depth = len(prefix) / 2
		if depth == 0 {
			depth = 1
		}
	}
	return depth
}

func vdevSectionRole(name string) (string, bool) {
	switch strings.ToLower(name) {
	case "cache":
		return "cache", true
	case "logs", "log":
		return "log", true
	case "spares", "spare":
		return "spare", true
	case "special":
		return "special", true
	default:
		return "", false
	}
}

func isVdevGroup(name string) bool {
	lower := strings.ToLower(name)
	switch {
	case strings.HasPrefix(lower, "mirror"):
		return true
	case strings.HasPrefix(lower, "raidz"):
		return true
	case strings.HasPrefix(lower, "draid"):
		return true
	default:
		return false
	}
}

// CreateDataset creates a ZFS filesystem or volume with a small allowlisted set of properties.
func CreateDataset(ctx context.Context, cfg config.Config, name, kind, size string, props map[string]string) (execwrap.Result, error) {
	args := []string{"create"}
	if kind == "volume" {
		if size == "" {
			return execwrap.Result{}, fmt.Errorf("volume size required")
		}
		args = append(args, "-V", size)
	}
	for key, val := range props {
		if val == "" {
			continue
		}
		args = append(args, "-o", fmt.Sprintf("%s=%s", key, val))
	}
	args = append(args, name)
	return execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
}

func SetDatasetProperties(ctx context.Context, cfg config.Config, name string, props map[string]string) (execwrap.Result, error) {
	args := []string{"set"}
	for key, val := range props {
		if val == "" {
			continue
		}
		args = append(args, fmt.Sprintf("%s=%s", key, val))
	}
	args = append(args, name)
	return execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
}

func DestroyDataset(ctx context.Context, cfg config.Config, name string, recursive bool) (execwrap.Result, error) {
	args := []string{"destroy"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, name)
	return execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
}

func RenameDataset(ctx context.Context, cfg config.Config, oldName, newName string) (execwrap.Result, error) {
	args := []string{"rename", oldName, newName}
	return execwrap.Run(ctx, cfg.Paths.ZFS, args, nil, cfg.Limits)
}
