// Package httpd handles settings and system actions.
package httpd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"raidraccoon/internal/auth"
	"raidraccoon/internal/config"
	"raidraccoon/internal/execwrap"
)

const (
	autostartServiceName = "raidraccoon"
	autostartScriptPath  = "/usr/local/etc/rc.d/raidraccoon"
)

type settingsAuth struct {
	Username string `json:"username"`
}

type settingsPayload struct {
	Server      config.ServerConfig   `json:"server"`
	Auth        settingsAuth          `json:"auth"`
	Paths       config.Paths          `json:"paths"`
	Samba       config.SambaConfig    `json:"samba"`
	ZFS         config.ZFSConfig      `json:"zfs"`
	Cron        config.CronConfig     `json:"cron"`
	Terminal    config.TerminalConfig `json:"terminal"`
	Limits      config.Limits         `json:"limits"`
	Audit       config.AuditConfig    `json:"audit"`
	AllowedCmds []string              `json:"allowed_cmds"`
	BinaryPath  string                `json:"binary_path"`
}

type settingsMeta struct {
	ConfigPath       string `json:"config_path"`
	AutostartEnabled bool   `json:"autostart_enabled"`
	RcScriptPresent  bool   `json:"rc_script_present"`
	AutostartError   string `json:"autostart_error,omitempty"`
	PasswordSet      bool   `json:"password_set"`
}

type settingsResponse struct {
	Config settingsPayload `json:"config"`
	Meta   settingsMeta    `json:"meta"`
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfg := s.snapshotConfig()
		meta := s.buildSettingsMeta(cfg)
		payload := settingsPayloadFromConfig(cfg)
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: settingsResponse{Config: payload, Meta: meta}})
	case http.MethodPut:
		var req settingsPayload
		if !s.decodeJSON(w, r, &req) {
			return
		}
		restartRequired, err := s.applySettingsUpdate(req)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "settings update failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"restart_required": restartRequired}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleSettingsPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Password        string `json:"password"`
		PasswordConfirm string `json:"password_confirm"`
		Confirm         bool   `json:"confirm"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if !req.Confirm {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
		return
	}
	if req.Password == "" || req.Password != req.PasswordConfirm {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "passwords do not match"})
		return
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.cfg.ConfigPath == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "config path not set"})
		return
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "salt generation failed", Details: err.Error()})
		return
	}
	saltHex := hex.EncodeToString(salt)
	hash := config.HashPasswordHex(saltHex, req.Password)
	previous := s.cfg
	s.cfg.Auth.SaltHex = saltHex
	s.cfg.Auth.PasswordHashHex = hash
	if err := config.Save(s.cfg.ConfigPath, s.cfg); err != nil {
		s.cfg = previous
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "settings update failed", Details: err.Error()})
		return
	}
	s.audit.Log(auth.UserFromContext(r.Context()), "auth.password", "password update", 0)
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"restart_required": true}})
}

func (s *Server) handleSystemAutostart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Enable  bool `json:"enable"`
		Confirm bool `json:"confirm"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if !req.Confirm {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
		return
	}
	if _, err := os.Stat(autostartScriptPath); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "rc script not found", Details: autostartScriptPath})
		return
	}
	cfg := s.snapshotConfig()
	if err := validateAbsPath("paths.sysrc", cfg.Paths.Sysrc); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid sysrc path", Details: err.Error()})
		return
	}
	value := "NO"
	if req.Enable {
		value = "YES"
	}
	arg := fmt.Sprintf("%s_enable=%s", autostartServiceName, value)
	res, err := execwrap.Run(context.Background(), cfg.Paths.Sysrc, []string{arg}, nil, cfg.Limits)
	s.audit.Log(auth.UserFromContext(r.Context()), "system.autostart", fmt.Sprintf("%s %s", cfg.Paths.Sysrc, arg), res.ExitCode)
	if err != nil || res.ExitCode != 0 {
		details := strings.TrimSpace(res.Stderr)
		if details == "" && err != nil {
			details = err.Error()
		}
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "autostart update failed", Details: details})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"enabled": req.Enable}})
}

func (s *Server) handleSystemReboot(w http.ResponseWriter, r *http.Request) {
	s.handleSystemPower(w, r, "reboot")
}

func (s *Server) handleSystemShutdown(w http.ResponseWriter, r *http.Request) {
	s.handleSystemPower(w, r, "shutdown")
}

func (s *Server) handleSystemPower(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Confirm bool `json:"confirm"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if !req.Confirm {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
		return
	}
	cfg := s.snapshotConfig()
	if err := validateAbsPath("paths.shutdown", cfg.Paths.Shutdown); err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid shutdown path", Details: err.Error()})
		return
	}
	args := []string{"-p", "now"}
	logAction := "system.shutdown"
	if action == "reboot" {
		args = []string{"-r", "now"}
		logAction = "system.reboot"
	}
	res, err := execwrap.Run(context.Background(), cfg.Paths.Shutdown, args, nil, cfg.Limits)
	s.audit.Log(auth.UserFromContext(r.Context()), logAction, fmt.Sprintf("%s %s", cfg.Paths.Shutdown, strings.Join(args, " ")), res.ExitCode)
	if err != nil || res.ExitCode != 0 {
		details := strings.TrimSpace(res.Stderr)
		if details == "" && err != nil {
			details = err.Error()
		}
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "power action failed", Details: details})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"action": action}})
}

func (s *Server) snapshotConfig() config.Config {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	return cloneConfig(s.cfg)
}

func cloneConfig(cfg config.Config) config.Config {
	out := cfg
	out.Samba.ReloadArgs = append([]string{}, cfg.Samba.ReloadArgs...)
	out.Samba.TestparmArgs = append([]string{}, cfg.Samba.TestparmArgs...)
	out.ZFS.AllowedPrefixes = append([]string{}, cfg.ZFS.AllowedPrefixes...)
	out.Terminal.Aliases = cloneMap(cfg.Terminal.Aliases)
	out.Terminal.Favorites = append([]string{}, cfg.Terminal.Favorites...)
	out.Dashboard.Widgets = append([]config.DashboardWidget{}, cfg.Dashboard.Widgets...)
	out.AllowedCmds = append([]string{}, cfg.AllowedCmds...)
	return out
}

func cloneMap(input map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range input {
		out[key] = value
	}
	return out
}

func settingsPayloadFromConfig(cfg config.Config) settingsPayload {
	return settingsPayload{
		Server: cfg.Server,
		Auth: settingsAuth{
			Username: cfg.Auth.Username,
		},
		Paths:       cfg.Paths,
		Samba:       cfg.Samba,
		ZFS:         cfg.ZFS,
		Cron:        cfg.Cron,
		Terminal:    cfg.Terminal,
		Limits:      cfg.Limits,
		Audit:       cfg.Audit,
		AllowedCmds: append([]string{}, cfg.AllowedCmds...),
		BinaryPath:  cfg.BinaryPath,
	}
}

func (s *Server) buildSettingsMeta(cfg config.Config) settingsMeta {
	enabled, rcPresent, errMsg := autostartStatus(cfg)
	meta := settingsMeta{
		ConfigPath:       cfg.ConfigPath,
		AutostartEnabled: enabled,
		RcScriptPresent:  rcPresent,
		PasswordSet:      cfg.Auth.SaltHex != "" && cfg.Auth.PasswordHashHex != "",
	}
	if errMsg != "" {
		meta.AutostartError = errMsg
	}
	return meta
}

func autostartStatus(cfg config.Config) (bool, bool, string) {
	rcPresent := true
	if _, err := os.Stat(autostartScriptPath); err != nil {
		rcPresent = false
	}
	if !rcPresent {
		return false, false, "rc script not found"
	}
	if err := validateAbsPath("paths.sysrc", cfg.Paths.Sysrc); err != nil {
		return false, rcPresent, err.Error()
	}
	res, err := execwrap.Run(context.Background(), cfg.Paths.Sysrc, []string{"-n", autostartServiceName + "_enable"}, nil, cfg.Limits)
	if err != nil {
		return false, rcPresent, err.Error()
	}
	if res.ExitCode != 0 {
		msg := strings.TrimSpace(res.Stderr)
		if isUnknownSysrcVar(msg) {
			return false, rcPresent, ""
		}
		return false, rcPresent, msg
	}
	value := strings.TrimSpace(res.Stdout)
	return isTruthy(value), rcPresent, ""
}

func isUnknownSysrcVar(msg string) bool {
	if msg == "" {
		return false
	}
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "unknown variable") || strings.Contains(lower, "not defined")
}

func isTruthy(value string) bool {
	v := strings.TrimSpace(strings.ToLower(value))
	switch v {
	case "1", "yes", "true", "on", "enabled":
		return true
	default:
		return false
	}
}

func (s *Server) applySettingsUpdate(req settingsPayload) (bool, error) {
	normalizeSettings(&req)
	if err := validateSettings(req); err != nil {
		return false, err
	}

	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.cfg.ConfigPath == "" {
		return false, errors.New("config path not set")
	}

	previous := s.cfg
	updated := s.cfg
	updated.Server = req.Server
	updated.Auth.Username = req.Auth.Username
	updated.Paths = req.Paths
	updated.Samba = req.Samba
	updated.ZFS = req.ZFS
	updated.Cron = req.Cron
	updated.Terminal = req.Terminal
	updated.Limits = req.Limits
	updated.Audit = req.Audit
	updated.AllowedCmds = append([]string{}, req.AllowedCmds...)
	updated.BinaryPath = req.BinaryPath
	updated.ConfigPath = previous.ConfigPath
	updated.Unsafe = previous.Unsafe
	updated.Auth.SaltHex = previous.Auth.SaltHex
	updated.Auth.PasswordHashHex = previous.Auth.PasswordHashHex

	restartRequired := settingsNeedsRestart(previous, updated)
	if err := config.Save(s.cfg.ConfigPath, updated); err != nil {
		return false, err
	}
	s.cfg = updated
	s.jobs.UpdateConfig(updated)
	s.terminal.SetHistoryLimit(updated.Terminal.HistoryLimit)
	s.audit.SetPath(updated.Audit.LogFile)
	return restartRequired, nil
}

func settingsNeedsRestart(before, after config.Config) bool {
	if before.Server.ListenAddr != after.Server.ListenAddr {
		return true
	}
	if before.Auth.Username != after.Auth.Username {
		return true
	}
	return false
}

func normalizeSettings(req *settingsPayload) {
	req.Server.ListenAddr = strings.TrimSpace(req.Server.ListenAddr)
	req.Auth.Username = strings.TrimSpace(req.Auth.Username)
	req.Paths.ZFS = strings.TrimSpace(req.Paths.ZFS)
	req.Paths.ZPool = strings.TrimSpace(req.Paths.ZPool)
	req.Paths.Geom = strings.TrimSpace(req.Paths.Geom)
	req.Paths.Service = strings.TrimSpace(req.Paths.Service)
	req.Paths.SMBPasswd = strings.TrimSpace(req.Paths.SMBPasswd)
	req.Paths.PDBEdit = strings.TrimSpace(req.Paths.PDBEdit)
	req.Paths.TestParm = strings.TrimSpace(req.Paths.TestParm)
	req.Paths.Rsync = strings.TrimSpace(req.Paths.Rsync)
	req.Paths.Sysctl = strings.TrimSpace(req.Paths.Sysctl)
	req.Paths.Sysrc = strings.TrimSpace(req.Paths.Sysrc)
	req.Paths.Shutdown = strings.TrimSpace(req.Paths.Shutdown)
	req.Samba.IncludeFile = strings.TrimSpace(req.Samba.IncludeFile)
	req.Samba.ReloadArgs = cleanList(req.Samba.ReloadArgs)
	req.Samba.TestparmArgs = cleanList(req.Samba.TestparmArgs)
	req.ZFS.AllowedPrefixes = cleanList(req.ZFS.AllowedPrefixes)
	req.ZFS.SnapshotPrefix = strings.TrimSpace(req.ZFS.SnapshotPrefix)
	req.Cron.CronFile = strings.TrimSpace(req.Cron.CronFile)
	req.Cron.CronUser = strings.TrimSpace(req.Cron.CronUser)
	req.Terminal.Aliases = cleanMap(req.Terminal.Aliases)
	req.Terminal.Favorites = cleanList(req.Terminal.Favorites)
	req.Terminal.HistoryLimit = intMax(req.Terminal.HistoryLimit, 0)
	req.Limits.MaxRequestBytes = int64Max(req.Limits.MaxRequestBytes, 0)
	req.Limits.MaxOutputBytes = int64Max(req.Limits.MaxOutputBytes, 0)
	req.Limits.MaxRuntimeSeconds = int64Max(req.Limits.MaxRuntimeSeconds, 0)
	req.Audit.LogFile = strings.TrimSpace(req.Audit.LogFile)
	req.AllowedCmds = cleanList(req.AllowedCmds)
	req.BinaryPath = strings.TrimSpace(req.BinaryPath)
}

func validateSettings(req settingsPayload) error {
	if req.Auth.Username == "" {
		return errors.New("auth.username required")
	}
	if err := validateAbsPath("paths.zfs", req.Paths.ZFS); err != nil {
		return err
	}
	if err := validateAbsPath("paths.zpool", req.Paths.ZPool); err != nil {
		return err
	}
	if err := validateAbsPath("paths.geom", req.Paths.Geom); err != nil {
		return err
	}
	if err := validateAbsPath("paths.service", req.Paths.Service); err != nil {
		return err
	}
	if err := validateAbsPath("paths.smbpasswd", req.Paths.SMBPasswd); err != nil {
		return err
	}
	if err := validateAbsPath("paths.pdbedit", req.Paths.PDBEdit); err != nil {
		return err
	}
	if err := validateAbsPath("paths.testparm", req.Paths.TestParm); err != nil {
		return err
	}
	if err := validateAbsPath("paths.rsync", req.Paths.Rsync); err != nil {
		return err
	}
	if err := validateAbsPath("paths.sysctl", req.Paths.Sysctl); err != nil {
		return err
	}
	if err := validateAbsPath("paths.sysrc", req.Paths.Sysrc); err != nil {
		return err
	}
	if err := validateAbsPath("paths.shutdown", req.Paths.Shutdown); err != nil {
		return err
	}
	if req.Samba.IncludeFile == "" {
		return errors.New("samba.include_file required")
	}
	if len(req.Samba.ReloadArgs) == 0 {
		return errors.New("samba.reload_args required")
	}
	if len(req.Samba.TestparmArgs) == 0 {
		return errors.New("samba.testparm_args required")
	}
	if req.ZFS.SnapshotPrefix == "" {
		return errors.New("zfs.snapshot_prefix required")
	}
	if err := validateAbsPath("cron.cron_file", req.Cron.CronFile); err != nil {
		return err
	}
	if req.Terminal.HistoryLimit <= 0 {
		return errors.New("terminal.history_limit must be > 0")
	}
	if req.Limits.MaxRequestBytes <= 0 {
		return errors.New("limits.max_request_bytes must be > 0")
	}
	if req.Limits.MaxOutputBytes <= 0 {
		return errors.New("limits.max_output_bytes must be > 0")
	}
	if req.Limits.MaxRuntimeSeconds <= 0 {
		return errors.New("limits.max_runtime_seconds must be > 0")
	}
	if err := validateAbsPath("audit.log_file", req.Audit.LogFile); err != nil {
		return err
	}
	if len(req.AllowedCmds) == 0 {
		return errors.New("allowed_cmds must include at least one command")
	}
	for _, cmd := range req.AllowedCmds {
		if err := validateAbsPath("allowed_cmds", cmd); err != nil {
			return err
		}
	}
	for key, value := range req.Terminal.Aliases {
		if key == "" || value == "" {
			return errors.New("terminal.aliases entries must be name=path")
		}
		if err := validateAbsPath("terminal.aliases", value); err != nil {
			return err
		}
	}
	if req.BinaryPath != "" {
		if err := validateAbsPath("binary_path", req.BinaryPath); err != nil {
			return err
		}
	}
	return nil
}

func validateAbsPath(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s required", label)
	}
	if !strings.HasPrefix(value, "/") {
		return fmt.Errorf("%s must be absolute", label)
	}
	return nil
}

func cleanList(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func cleanMap(items map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range items {
		k := strings.TrimSpace(key)
		v := strings.TrimSpace(value)
		if k == "" || v == "" {
			continue
		}
		out[k] = v
	}
	return out
}

func intMax(value, min int) int {
	if value < min {
		return min
	}
	return value
}

func int64Max(value, min int64) int64 {
	if value < min {
		return min
	}
	return value
}
