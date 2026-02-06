// Package config loads, validates, and writes RaidRaccoon configuration.
package config

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Limits struct {
	MaxRequestBytes   int64 `json:"max_request_bytes"`
	MaxOutputBytes    int64 `json:"max_output_bytes"`
	MaxRuntimeSeconds int64 `json:"max_runtime_seconds"`
}

type Paths struct {
	ZFS       string `json:"zfs"`
	ZPool     string `json:"zpool"`
	Geom      string `json:"geom"`
	Service   string `json:"service"`
	SMBPasswd string `json:"smbpasswd"`
	PDBEdit   string `json:"pdbedit"`
	TestParm  string `json:"testparm"`
	Sysctl    string `json:"sysctl"`
	Sysrc     string `json:"sysrc"`
	Shutdown  string `json:"shutdown"`
	Rsync     string `json:"rsync"`
}

type SambaConfig struct {
	IncludeFile  string   `json:"include_file"`
	ReloadArgs   []string `json:"reload_args"`
	TestparmArgs []string `json:"testparm_args"`
}

type ZFSConfig struct {
	AllowedPrefixes []string `json:"allowed_prefixes"`
	SnapshotPrefix  string   `json:"snapshot_prefix"`
}

type CronConfig struct {
	CronFile string `json:"cron_file"`
	CronUser string `json:"cron_user"`
}

type AuthConfig struct {
	Username        string `json:"username"`
	SaltHex         string `json:"salt_hex"`
	PasswordHashHex string `json:"password_hash_hex"`
}

type ServerConfig struct {
	ListenAddr string `json:"listen_addr"`
}

type AuditConfig struct {
	LogFile string `json:"log_file"`
}

type TerminalConfig struct {
	Aliases      map[string]string `json:"aliases"`
	Favorites    []string          `json:"favorites"`
	HistoryLimit int               `json:"history_limit"`
}

type DashboardWidget struct {
	ID      string `json:"id"`
	Enabled bool   `json:"enabled"`
}

type DashboardConfig struct {
	Widgets []DashboardWidget `json:"widgets"`
}

type Config struct {
	Server      ServerConfig    `json:"server"`
	Auth        AuthConfig      `json:"auth"`
	Paths       Paths           `json:"paths"`
	Samba       SambaConfig     `json:"samba"`
	ZFS         ZFSConfig       `json:"zfs"`
	Cron        CronConfig      `json:"cron"`
	Terminal    TerminalConfig  `json:"terminal"`
	Dashboard   DashboardConfig `json:"dashboard"`
	Limits      Limits          `json:"limits"`
	Audit       AuditConfig     `json:"audit"`
	AllowedCmds []string        `json:"allowed_cmds"`
	BinaryPath  string          `json:"binary_path"`
	ConfigPath  string          `json:"-"`
	Unsafe      bool            `json:"-"`
}

// DefaultConfig returns a safe baseline configuration suitable for FreeBSD.
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{ListenAddr: "0.0.0.0:8080"},
		Auth: AuthConfig{
			Username:        "admin",
			SaltHex:         "",
			PasswordHashHex: "",
		},
		Paths: Paths{
			ZFS:       "/sbin/zfs",
			ZPool:     "/sbin/zpool",
			Geom:      "/sbin/geom",
			Service:   "/usr/sbin/service",
			SMBPasswd: "/usr/local/bin/smbpasswd",
			PDBEdit:   "/usr/local/bin/pdbedit",
			TestParm:  "/usr/local/bin/testparm",
			Sysctl:    "/sbin/sysctl",
			Sysrc:     "/usr/sbin/sysrc",
			Shutdown:  "/sbin/shutdown",
			Rsync:     "/usr/local/bin/rsync",
		},
		Samba: SambaConfig{
			IncludeFile:  "/usr/local/etc/smb4.conf",
			ReloadArgs:   []string{"samba_server", "restart"},
			TestparmArgs: []string{"-s", "/usr/local/etc/smb4.conf"},
		},
		ZFS: ZFSConfig{
			AllowedPrefixes: []string{},
			SnapshotPrefix:  "raidraccoon",
		},
		Cron: CronConfig{
			CronFile: "/etc/crontab",
			CronUser: "root",
		},
		Terminal: TerminalConfig{
			Aliases: map[string]string{
				"zfs":       "/sbin/zfs",
				"zpool":     "/sbin/zpool",
				"geom":      "/sbin/geom",
				"service":   "/usr/sbin/service",
				"smbpasswd": "/usr/local/bin/smbpasswd",
				"pdbedit":   "/usr/local/bin/pdbedit",
				"testparm":  "/usr/local/bin/testparm",
				"rsync":     "/usr/local/bin/rsync",
			},
			Favorites:    []string{},
			HistoryLimit: 20,
		},
		Dashboard: DashboardConfig{
			Widgets: defaultDashboardWidgets(),
		},
		Limits: Limits{
			MaxRequestBytes:   1 << 20,
			MaxOutputBytes:    1 << 20,
			MaxRuntimeSeconds: 120,
		},
		Audit: AuditConfig{
			LogFile: "/var/log/raidraccoon-audit.log",
		},
		AllowedCmds: []string{
			"/sbin/zfs",
			"/sbin/zpool",
			"/sbin/geom",
			"/usr/sbin/service",
			"/usr/local/bin/smbpasswd",
			"/usr/local/bin/pdbedit",
			"/usr/local/bin/testparm",
			"/usr/local/bin/rsync",
		},
		BinaryPath: "",
	}
}

// Load reads a JSON configuration from disk and applies defaults for missing fields.
func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	applyDefaults(&cfg)
	return cfg, nil
}

// Save writes cfg to path atomically (via a temporary file + rename).
func Save(path string, cfg Config) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Exists reports whether a file exists at path.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// DefaultConfigWithPassword returns the default config with auth initialized
// to the provided password (salted SHA-256).
func DefaultConfigWithPassword(password string) (Config, error) {
	cfg := DefaultConfig()
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return Config{}, err
	}
	cfg.Auth.SaltHex = hex.EncodeToString(salt)
	hash := HashPasswordHex(cfg.Auth.SaltHex, password)
	cfg.Auth.PasswordHashHex = hash
	return cfg, nil
}

// HashPasswordHex returns hex(sha256(salt || password)).
func HashPasswordHex(saltHex, password string) string {
	salt, _ := hex.DecodeString(strings.TrimSpace(saltHex))
	h := sha256.Sum256(append(salt, []byte(password)...))
	return hex.EncodeToString(h[:])
}

func applyDefaults(cfg *Config) {
	def := DefaultConfig()
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = def.Server.ListenAddr
	}
	if cfg.Auth.Username == "" {
		cfg.Auth.Username = def.Auth.Username
	}
	if cfg.Paths.ZFS == "" {
		cfg.Paths.ZFS = def.Paths.ZFS
	}
	if cfg.Paths.ZPool == "" {
		cfg.Paths.ZPool = def.Paths.ZPool
	}
	if cfg.Paths.Geom == "" {
		cfg.Paths.Geom = def.Paths.Geom
	}
	if cfg.Paths.Service == "" {
		cfg.Paths.Service = def.Paths.Service
	}
	if cfg.Paths.SMBPasswd == "" {
		cfg.Paths.SMBPasswd = def.Paths.SMBPasswd
	}
	if cfg.Paths.PDBEdit == "" {
		cfg.Paths.PDBEdit = def.Paths.PDBEdit
	}
	if cfg.Paths.TestParm == "" {
		cfg.Paths.TestParm = def.Paths.TestParm
	}
	if cfg.Paths.Sysctl == "" {
		cfg.Paths.Sysctl = def.Paths.Sysctl
	}
	if cfg.Paths.Sysrc == "" {
		cfg.Paths.Sysrc = def.Paths.Sysrc
	}
	if cfg.Paths.Shutdown == "" {
		cfg.Paths.Shutdown = def.Paths.Shutdown
	}
	if cfg.Paths.Rsync == "" {
		cfg.Paths.Rsync = def.Paths.Rsync
	}
	if cfg.Samba.IncludeFile == "" {
		cfg.Samba.IncludeFile = def.Samba.IncludeFile
	}
	if len(cfg.Samba.ReloadArgs) == 0 {
		cfg.Samba.ReloadArgs = def.Samba.ReloadArgs
	}
	if len(cfg.Samba.TestparmArgs) == 0 {
		cfg.Samba.TestparmArgs = def.Samba.TestparmArgs
	}
	if cfg.ZFS.SnapshotPrefix == "" {
		cfg.ZFS.SnapshotPrefix = def.ZFS.SnapshotPrefix
	}
	if cfg.Cron.CronFile == "" {
		cfg.Cron.CronFile = def.Cron.CronFile
	}
	if cfg.Cron.CronUser == "" {
		cfg.Cron.CronUser = def.Cron.CronUser
	}
	if cfg.Terminal.HistoryLimit == 0 {
		cfg.Terminal.HistoryLimit = def.Terminal.HistoryLimit
	}
	if cfg.Terminal.Aliases == nil {
		cfg.Terminal.Aliases = map[string]string{}
	}
	if len(cfg.Terminal.Aliases) == 0 {
		cfg.Terminal.Aliases = def.Terminal.Aliases
	} else {
		for key, value := range def.Terminal.Aliases {
			if _, ok := cfg.Terminal.Aliases[key]; !ok {
				cfg.Terminal.Aliases[key] = value
			}
		}
	}
	if cfg.Terminal.Favorites == nil {
		cfg.Terminal.Favorites = []string{}
	}
	if len(cfg.Dashboard.Widgets) == 0 {
		cfg.Dashboard.Widgets = defaultDashboardWidgets()
	} else {
		cfg.Dashboard.Widgets = mergeDashboardWidgets(cfg.Dashboard.Widgets, defaultDashboardWidgets())
	}
	if cfg.Limits.MaxRequestBytes == 0 {
		cfg.Limits.MaxRequestBytes = def.Limits.MaxRequestBytes
	}
	if cfg.Limits.MaxOutputBytes == 0 {
		cfg.Limits.MaxOutputBytes = def.Limits.MaxOutputBytes
	}
	if cfg.Limits.MaxRuntimeSeconds == 0 {
		cfg.Limits.MaxRuntimeSeconds = def.Limits.MaxRuntimeSeconds
	}
	if cfg.Audit.LogFile == "" {
		cfg.Audit.LogFile = def.Audit.LogFile
	}
	if len(cfg.AllowedCmds) == 0 {
		cfg.AllowedCmds = def.AllowedCmds
	}
}

func DefaultDashboardWidgets() []DashboardWidget {
	return append([]DashboardWidget{}, defaultDashboardWidgets()...)
}

func defaultDashboardWidgets() []DashboardWidget {
	return []DashboardWidget{
		{ID: "pools", Enabled: true},
		{ID: "cache", Enabled: true},
		{ID: "datasets", Enabled: true},
		{ID: "snapshots", Enabled: true},
		{ID: "schedules", Enabled: true},
		{ID: "samba", Enabled: true},
		{ID: "settings", Enabled: true},
	}
}

func mergeDashboardWidgets(current, defaults []DashboardWidget) []DashboardWidget {
	out := make([]DashboardWidget, 0, len(current)+len(defaults))
	seen := map[string]struct{}{}
	for _, item := range current {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		out = append(out, DashboardWidget{ID: id, Enabled: item.Enabled})
		seen[id] = struct{}{}
	}
	for _, item := range defaults {
		if _, ok := seen[item.ID]; ok {
			continue
		}
		out = append(out, item)
		seen[item.ID] = struct{}{}
	}
	return out
}

// EnsureDir creates the parent directory for path.
func EnsureDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0o755)
}

// ReadAllLimited reads up to limit bytes and reports whether output was truncated.
func ReadAllLimited(r io.Reader, limit int64) ([]byte, bool, error) {
	if limit <= 0 {
		limit = 1 << 20
	}
	var out []byte
	buf := make([]byte, 4096)
	var total int64
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if total+int64(n) > limit {
				n = int(limit - total)
				out = append(out, buf[:n]...)
				return out, true, nil
			}
			out = append(out, buf[:n]...)
			total += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return out, false, err
		}
	}
	return out, false, nil
}

// NowTimestamp returns an RFC3339 UTC timestamp.
func NowTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339)
}
