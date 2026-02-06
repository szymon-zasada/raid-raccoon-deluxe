// Package samba manages Samba users and share config files via sudo-executed tools.
package samba

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"raidraccoon/internal/config"
	"raidraccoon/internal/execwrap"
)

type User struct {
	Name string `json:"name"`
}

// Share is a UI-friendly view of a Samba share section.
// Params stores additional raw key/value pairs preserved on round-trip.
type Share struct {
	Name       string            `json:"name"`
	Path       string            `json:"path"`
	ReadOnly   string            `json:"read_only"`
	Browseable string            `json:"browseable"`
	GuestOK    string            `json:"guest_ok"`
	Comment    string            `json:"comment"`
	Params     map[string]string `json:"params"`
	ParamOrder []string          `json:"-"`
}

var primaryKeys = []string{
	"path",
	"read only",
	"writable",
	"browsable",
	"browseable",
	"guest ok",
	"comment",
}

type sambaFile struct {
	preamble      []string
	globalLines   []string
	globalPresent bool
	order         []string
	shares        map[string]Share
}

const defaultConfigPath = "/usr/local/etc/smb4.conf"

// ListUsers returns Samba users parsed from `pdbedit -L`.
func ListUsers(ctx context.Context, cfg config.Config) ([]User, error) {
	res, err := execwrap.Run(ctx, cfg.Paths.PDBEdit, []string{"-L"}, nil, cfg.Limits)
	if err != nil {
		return nil, err
	}
	if res.ExitCode != 0 {
		return nil, fmt.Errorf(res.Stderr)
	}
	var users []User
	scanner := bufio.NewScanner(strings.NewReader(res.Stdout))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		users = append(users, User{Name: parts[0]})
	}
	return users, nil
}

// AddUser creates a Samba user and sets the initial password via smbpasswd.
func AddUser(ctx context.Context, cfg config.Config, username, password string) (execwrap.Result, error) {
	stdin := []byte(password + "\n" + password + "\n")
	return execwrap.Run(ctx, cfg.Paths.SMBPasswd, []string{"-a", username}, stdin, cfg.Limits)
}

// EnableUser enables a Samba user account.
func EnableUser(ctx context.Context, cfg config.Config, username string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.SMBPasswd, []string{"-e", username}, nil, cfg.Limits)
}

// DisableUser disables a Samba user account.
func DisableUser(ctx context.Context, cfg config.Config, username string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.SMBPasswd, []string{"-d", username}, nil, cfg.Limits)
}

// DeleteUser removes a Samba user account.
func DeleteUser(ctx context.Context, cfg config.Config, username string) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.SMBPasswd, []string{"-x", username}, nil, cfg.Limits)
}

// PasswdUser updates the Samba user's password.
func PasswdUser(ctx context.Context, cfg config.Config, username, password string) (execwrap.Result, error) {
	stdin := []byte(password + "\n" + password + "\n")
	return execwrap.Run(ctx, cfg.Paths.SMBPasswd, []string{"-s", username}, stdin, cfg.Limits)
}

// TestConfig runs testparm with configured args.
func TestConfig(ctx context.Context, cfg config.Config) (execwrap.Result, error) {
	return execwrap.Run(ctx, cfg.Paths.TestParm, cfg.Samba.TestparmArgs, nil, cfg.Limits)
}

// Reload applies Samba config changes by invoking the configured service command.
func Reload(ctx context.Context, cfg config.Config) (execwrap.Result, error) {
	if len(cfg.Samba.ReloadArgs) == 0 {
		return execwrap.Result{}, errors.New("reload_args not configured")
	}
	return execwrap.Run(ctx, cfg.Paths.Service, cfg.Samba.ReloadArgs, nil, cfg.Limits)
}

// ListShares reads a Samba config file and returns only share sections.
func ListShares(path string) ([]Share, error) {
	target := resolveConfigPath(path)
	file, err := readSambaFile(target)
	if err != nil {
		return nil, err
	}
	if !hasShares(file) && target != defaultConfigPath {
		if fallbackFile, err := readSambaFile(defaultConfigPath); err == nil && hasShares(fallbackFile) {
			file = fallbackFile
		}
	}
	if file == nil || len(file.order) == 0 {
		return []Share{}, nil
	}
	shares := make([]Share, 0, len(file.order))
	for _, name := range file.order {
		if strings.EqualFold(name, "global") {
			continue
		}
		share, ok := file.shares[name]
		if !ok {
			continue
		}
		shares = append(shares, share)
	}
	return shares, nil
}

// SaveShares rewrites share sections in the Samba config while preserving global/preamble.
func SaveShares(path string, shares []Share) error {
	target := resolveConfigPath(path)
	file, err := readSambaFile(target)
	if err != nil {
		return err
	}
	if !hasShares(file) && target != defaultConfigPath {
		if fallbackFile, err := readSambaFile(defaultConfigPath); err == nil && hasShares(fallbackFile) {
			target = defaultConfigPath
			file = fallbackFile
		}
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	shareMap := map[string]Share{}
	for _, share := range shares {
		if share.Name == "" {
			continue
		}
		shareMap[share.Name] = share
	}
	order := buildWriteOrder(file, shares, shareMap)
	tmp := target + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	wrote := writePreamble(w, file, true)
	wrote = writeGlobalSection(w, file, wrote, true)
	for _, name := range order {
		if strings.EqualFold(name, "global") {
			continue
		}
		share, ok := shareMap[name]
		if !ok || share.Name == "" {
			continue
		}
		if wrote {
			_, _ = w.WriteString("\n")
		}
		_, _ = w.WriteString("[" + share.Name + "]\n")
		writeParam(w, "path", share.Path)
		writeParam(w, "read only", fallback(share.ReadOnly, "no"))
		writeParam(w, "browsable", fallback(share.Browseable, "yes"))
		writeParam(w, "guest ok", fallback(share.GuestOK, "no"))
		if share.Comment != "" {
			writeParam(w, "comment", share.Comment)
		}
		for _, key := range extraKeys(share) {
			writeParam(w, key, share.Params[key])
		}
		wrote = true
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// UpsertShare inserts or replaces share (by Name) in shares.
func UpsertShare(shares []Share, share Share) []Share {
	for i := range shares {
		if shares[i].Name == share.Name {
			shares[i] = mergeShare(shares[i], share)
			return shares
		}
	}
	return append(shares, share)
}

func DeleteShare(shares []Share, name string) []Share {
	var out []Share
	for _, s := range shares {
		if s.Name != name {
			out = append(out, s)
		}
	}
	return out
}

func mergeShare(existing, incoming Share) Share {
	if incoming.Path != "" {
		existing.Path = incoming.Path
	}
	if incoming.ReadOnly != "" {
		existing.ReadOnly = incoming.ReadOnly
	}
	if incoming.Browseable != "" {
		existing.Browseable = incoming.Browseable
	}
	if incoming.GuestOK != "" {
		existing.GuestOK = incoming.GuestOK
	}
	if incoming.Comment != "" {
		existing.Comment = incoming.Comment
	}
	if existing.Params == nil {
		existing.Params = map[string]string{}
	}
	for k, v := range incoming.Params {
		existing.Params[k] = v
	}
	return existing
}

func pickParam(params map[string]string, key string) string {
	for k, v := range params {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func writeParam(w *bufio.Writer, key, val string) {
	if val == "" {
		return
	}
	_, _ = w.WriteString(fmt.Sprintf("%s = %s\n", key, val))
}

func fallback(val, def string) string {
	if val == "" {
		return def
	}
	return val
}

func hasShares(file *sambaFile) bool {
	if file == nil {
		return false
	}
	for _, name := range file.order {
		if !strings.EqualFold(name, "global") {
			return true
		}
	}
	return false
}

func resolveConfigPath(path string) string {
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	if path != defaultConfigPath {
		if _, err := os.Stat(defaultConfigPath); err == nil {
			return defaultConfigPath
		}
	}
	if path != "" {
		return path
	}
	return defaultConfigPath
}

func readSambaFile(path string) (*sambaFile, error) {
	seen := map[string]bool{}
	return readSambaFileRecursive(path, seen)
}

func readSambaFileRecursive(path string, seen map[string]bool) (*sambaFile, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = filepath.Clean(path)
	}
	if seen[absPath] {
		return &sambaFile{shares: map[string]Share{}}, nil
	}
	seen[absPath] = true
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &sambaFile{shares: map[string]Share{}}, nil
		}
		return nil, err
	}
	file := &sambaFile{shares: map[string]Share{}}
	var includePaths []string
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var currentName string
	var currentLines []string
	var currentParams map[string]string
	var currentOrder []string
	seenSections := map[string]bool{}
	inSection := false
	flush := func() {
		if currentName == "" {
			return
		}
		if !seenSections[currentName] {
			file.order = append(file.order, currentName)
			seenSections[currentName] = true
		}
		if strings.EqualFold(currentName, "global") {
			file.globalLines = append([]string{}, currentLines...)
			file.globalPresent = true
			return
		}
		share := Share{
			Name:       currentName,
			Path:       pickParam(currentParams, "path"),
			ReadOnly:   pickReadOnly(currentParams),
			Browseable: pickBrowseable(currentParams),
			GuestOK:    pickParam(currentParams, "guest ok"),
			Comment:    pickParam(currentParams, "comment"),
			Params:     currentParams,
			ParamOrder: currentOrder,
		}
		file.shares[currentName] = share
	}
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if name, ok := parseSectionHeader(trimmed); ok {
			flush()
			currentName = name
			currentLines = nil
			currentParams = map[string]string{}
			currentOrder = nil
			inSection = true
			continue
		}
		if !inSection {
			file.preamble = append(file.preamble, line)
			if include := parseIncludeLine(line); include != "" {
				includePaths = append(includePaths, include)
			}
			continue
		}
		currentLines = append(currentLines, line)
		if strings.EqualFold(currentName, "global") {
			if include := parseIncludeLine(line); include != "" {
				includePaths = append(includePaths, include)
			}
		}
		if trimmed == "" || strings.HasPrefix(trimmed, ";") || strings.HasPrefix(trimmed, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		val := strings.TrimSpace(parts[1])
		if _, ok := currentParams[key]; !ok {
			currentOrder = append(currentOrder, key)
		}
		currentParams[key] = val
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	flush()
	if err := mergeIncludes(path, includePaths, file, seen); err != nil {
		return nil, err
	}
	return file, nil
}

func buildWriteOrder(file *sambaFile, shares []Share, shareMap map[string]Share) []string {
	var order []string
	seen := map[string]bool{}
	if file != nil {
		for _, name := range file.order {
			if _, ok := shareMap[name]; ok || strings.EqualFold(name, "global") {
				order = append(order, name)
				seen[name] = true
			}
		}
	}
	for _, share := range shares {
		if share.Name == "" {
			continue
		}
		if !seen[share.Name] {
			order = append(order, share.Name)
			seen[share.Name] = true
		}
	}
	return order
}

func writePreamble(w *bufio.Writer, file *sambaFile, stripIncludes bool) bool {
	if file == nil || len(file.preamble) == 0 {
		return false
	}
	for _, line := range file.preamble {
		if stripIncludes && parseIncludeLine(line) != "" {
			continue
		}
		_, _ = w.WriteString(line + "\n")
	}
	return true
}

func writeGlobalSection(w *bufio.Writer, file *sambaFile, wrote bool, stripIncludes bool) bool {
	if file == nil || (!file.globalPresent && len(file.globalLines) == 0) {
		return wrote
	}
	if wrote {
		_, _ = w.WriteString("\n")
	}
	_, _ = w.WriteString("[global]\n")
	for _, line := range file.globalLines {
		if stripIncludes && parseIncludeLine(line) != "" {
			continue
		}
		_, _ = w.WriteString(line + "\n")
	}
	return true
}

func extraKeys(share Share) []string {
	var keys []string
	for _, key := range share.ParamOrder {
		if isPrimaryKey(key) {
			continue
		}
		if _, ok := share.Params[key]; !ok {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) > 0 {
		return keys
	}
	for key := range share.Params {
		if isPrimaryKey(key) {
			continue
		}
		keys = append(keys, key)
	}
	if len(keys) <= 1 {
		return keys
	}
	sort.Strings(keys)
	return keys
}

func isPrimaryKey(key string) bool {
	for _, primary := range primaryKeys {
		if strings.EqualFold(primary, key) {
			return true
		}
	}
	return false
}

func pickBrowseable(params map[string]string) string {
	if val := pickParam(params, "browsable"); val != "" {
		return val
	}
	return pickParam(params, "browseable")
}

func pickReadOnly(params map[string]string) string {
	if val := pickParam(params, "read only"); val != "" {
		return val
	}
	writable := strings.ToLower(pickParam(params, "writable"))
	switch writable {
	case "yes", "true", "1":
		return "no"
	case "no", "false", "0":
		return "yes"
	default:
		return ""
	}
}

func parseSectionHeader(trimmed string) (string, bool) {
	if !strings.HasPrefix(trimmed, "[") {
		return "", false
	}
	end := strings.Index(trimmed, "]")
	if end <= 1 {
		return "", false
	}
	name := strings.TrimSpace(trimmed[1:end])
	if name == "" {
		return "", false
	}
	return name, true
}

func parseIncludeLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, ";") || strings.HasPrefix(trimmed, "#") {
		return ""
	}
	parts := strings.SplitN(trimmed, "=", 2)
	if len(parts) != 2 {
		return ""
	}
	key := strings.TrimSpace(parts[0])
	if !strings.EqualFold(key, "include") {
		return ""
	}
	val := strings.TrimSpace(parts[1])
	if val == "" {
		return ""
	}
	val = stripInlineComment(val)
	val = strings.TrimSpace(val)
	val = strings.Trim(val, "\"'")
	return val
}

func stripInlineComment(val string) string {
	for i := 0; i < len(val); i++ {
		if val[i] == '#' || val[i] == ';' {
			if i == 0 {
				return ""
			}
			if val[i-1] == ' ' || val[i-1] == '\t' {
				return strings.TrimSpace(val[:i])
			}
		}
	}
	return val
}

func mergeIncludes(basePath string, includePaths []string, file *sambaFile, seen map[string]bool) error {
	if len(includePaths) == 0 {
		return nil
	}
	baseDir := filepath.Dir(basePath)
	seenIncludes := map[string]bool{}
	for _, include := range includePaths {
		if include == "" {
			continue
		}
		for _, resolved := range resolveIncludePaths(baseDir, include) {
			if resolved == "" {
				continue
			}
			cleaned, err := filepath.Abs(resolved)
			if err != nil {
				cleaned = filepath.Clean(resolved)
			}
			if seenIncludes[cleaned] {
				continue
			}
			seenIncludes[cleaned] = true
			incFile, err := readSambaFileRecursive(cleaned, seen)
			if err != nil {
				return err
			}
			if incFile == nil {
				continue
			}
			for _, name := range incFile.order {
				if strings.EqualFold(name, "global") {
					continue
				}
				share, ok := incFile.shares[name]
				if !ok {
					continue
				}
				if _, exists := file.shares[name]; exists {
					continue
				}
				file.shares[name] = share
				file.order = append(file.order, name)
			}
		}
	}
	return nil
}

func resolveIncludePaths(baseDir, include string) []string {
	if include == "" {
		return nil
	}
	if strings.ContainsAny(include, "%$") {
		return nil
	}
	path := include
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	if strings.ContainsAny(path, "*?[") {
		matches, err := filepath.Glob(path)
		if err != nil || len(matches) == 0 {
			return nil
		}
		sort.Strings(matches)
		return matches
	}
	return []string{path}
}
