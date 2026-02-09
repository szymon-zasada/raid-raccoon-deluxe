// Package httpd wires HTTP routes, API handlers, and HTML rendering.
package httpd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"raidraccoon/internal/audit"
	"raidraccoon/internal/auth"
	"raidraccoon/internal/config"
	"raidraccoon/internal/cron"
	"raidraccoon/internal/drives"
	"raidraccoon/internal/execwrap"
	"raidraccoon/internal/samba"
	"raidraccoon/internal/ui"
	"raidraccoon/internal/zfs"
)

type Server struct {
	cfg               config.Config
	mux               *http.ServeMux
	jobs              *JobManager
	audit             *audit.Logger
	terminal          *TerminalState
	cfgMu             sync.Mutex
	importMu          sync.Mutex
	importablePools   []zfs.ImportablePool
	importableErr     string
	importableChecked time.Time
}

type pageData struct {
	Title  string
	Active string
}

// apiEnvelope is the uniform JSON response shape used by all API handlers.
type apiEnvelope struct {
	Ok      bool   `json:"ok"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	Details string `json:"details,omitempty"`
}

type scheduleUpdateRequest struct {
	Toggle    bool          `json:"toggle"`
	Dataset   string        `json:"dataset"`
	Retention int           `json:"retention"`
	Prefix    string        `json:"prefix"`
	Enabled   *bool         `json:"enabled"`
	Schedule  cron.CronSpec `json:"schedule"`
}

type replicationRequest struct {
	Source    string        `json:"source"`
	Target    string        `json:"target"`
	Retention int           `json:"retention"`
	Prefix    string        `json:"prefix"`
	Recursive bool          `json:"recursive"`
	Force     bool          `json:"force"`
	Enabled   bool          `json:"enabled"`
	Schedule  cron.CronSpec `json:"schedule"`
}

type replicationUpdateRequest struct {
	Toggle    bool          `json:"toggle"`
	Source    string        `json:"source"`
	Target    string        `json:"target"`
	Retention *int          `json:"retention"`
	Prefix    string        `json:"prefix"`
	Recursive *bool         `json:"recursive"`
	Force     *bool         `json:"force"`
	Enabled   *bool         `json:"enabled"`
	Schedule  cron.CronSpec `json:"schedule"`
}

type rsyncRequest struct {
	Source   string        `json:"source"`
	Target   string        `json:"target"`
	Mode     string        `json:"mode"`
	Flags    string        `json:"flags"`
	Enabled  bool          `json:"enabled"`
	Schedule cron.CronSpec `json:"schedule"`
}

type rsyncUpdateRequest struct {
	Toggle   bool          `json:"toggle"`
	Source   string        `json:"source"`
	Target   string        `json:"target"`
	Mode     string        `json:"mode"`
	Flags    string        `json:"flags"`
	Enabled  *bool         `json:"enabled"`
	Schedule cron.CronSpec `json:"schedule"`
}

func New(cfg config.Config) *Server {
	logger := audit.New(cfg.Audit.LogFile)
	s := &Server{
		cfg:      cfg,
		mux:      http.NewServeMux(),
		jobs:     NewJobManager(cfg, logger.Log),
		audit:    logger,
		terminal: NewTerminalState(cfg),
	}
	s.routes()
	s.startImportWatcher()
	return s
}

// Handler returns the HTTP handler with authentication middleware applied.
func (s *Server) Handler() http.Handler {
	return auth.Middleware(s.cfg.Auth, s.mux)
}

func (s *Server) routes() {
	s.mux.Handle("/static/", http.StripPrefix("/static/", ui.StaticHandler()))

	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard", http.StatusFound)
	})
	s.mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "dashboard", pageData{Title: "Dashboard", Active: "dashboard"})
	})
	s.mux.HandleFunc("/terminal", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "terminal", pageData{Title: "Terminal", Active: "terminal"})
	})
	s.mux.HandleFunc("/samba/users", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "samba_users", pageData{Title: "Samba Settings: Users", Active: "samba-users"})
	})
	s.mux.HandleFunc("/samba/shares", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "samba_shares", pageData{Title: "Samba Settings: Shares", Active: "samba-shares"})
	})
	s.mux.HandleFunc("/zfs/pools", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_pools", pageData{Title: "ZFS Pools", Active: "zfs-pools"})
	})
	s.mux.HandleFunc("/zfs/mounts", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_mounts", pageData{Title: "ZFS Mounts", Active: "zfs-mounts"})
	})
	s.mux.HandleFunc("/zfs/datasets", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_datasets", pageData{Title: "ZFS Datasets", Active: "zfs-datasets"})
	})
	s.mux.HandleFunc("/zfs/snapshots", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_snapshots", pageData{Title: "ZFS Snapshots: Snapshots", Active: "zfs-snapshots"})
	})
	s.mux.HandleFunc("/zfs/schedules", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_schedules", pageData{Title: "ZFS Snapshots: Snapshot Schedules", Active: "zfs-schedules"})
	})
	s.mux.HandleFunc("/zfs/replication", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "zfs_replication", pageData{Title: "ZFS Snapshots: Replication", Active: "zfs-replication"})
	})
	s.mux.HandleFunc("/settings", func(w http.ResponseWriter, r *http.Request) {
		ui.Render(w, "settings", pageData{Title: "System Settings", Active: "settings"})
	})

	s.mux.HandleFunc("/api/cmd/run", s.handleCmdRun)
	s.mux.HandleFunc("/api/jobs/", s.handleJobs)
	s.mux.HandleFunc("/api/terminal/meta", s.handleTerminalMeta)
	s.mux.HandleFunc("/api/terminal/favorites", s.handleTerminalFavorites)
	s.mux.HandleFunc("/api/dashboard", s.handleDashboard)

	s.mux.HandleFunc("/api/samba/users", s.handleSambaUsers)
	s.mux.HandleFunc("/api/samba/users/", s.handleSambaUserAction)
	s.mux.HandleFunc("/api/samba/shares", s.handleSambaShares)
	s.mux.HandleFunc("/api/samba/shares/", s.handleSambaShare)
	s.mux.HandleFunc("/api/samba/testparm", s.handleSambaTest)
	s.mux.HandleFunc("/api/samba/reload", s.handleSambaReload)

	s.mux.HandleFunc("/api/zfs/pools", s.handleZFSPools)
	s.mux.HandleFunc("/api/zfs/importable", s.handleZFSImportable)
	s.mux.HandleFunc("/api/zfs/import", s.handleZFSImport)
	s.mux.HandleFunc("/api/zfs/pools/", s.handleZFSPoolItem)
	s.mux.HandleFunc("/api/zfs/pools/status", s.handleZFSPoolStatus)
	s.mux.HandleFunc("/api/zfs/datasets", s.handleZFSDatasets)
	s.mux.HandleFunc("/api/zfs/datasets/", s.handleZFSDatasetItem)
	s.mux.HandleFunc("/api/zfs/drives", s.handleZFSDrives)
	s.mux.HandleFunc("/api/zfs/mounts", s.handleZFSMounts)
	s.mux.HandleFunc("/api/zfs/snapshots", s.handleZFSSnapshots)

	s.mux.HandleFunc("/api/zfs/schedules", s.handleSchedules)
	s.mux.HandleFunc("/api/zfs/schedules/", s.handleScheduleItem)
	s.mux.HandleFunc("/api/zfs/replication", s.handleZFSReplication)
	s.mux.HandleFunc("/api/zfs/replication/", s.handleZFSReplicationItem)
	s.mux.HandleFunc("/api/rsync", s.handleRsyncJobs)
	s.mux.HandleFunc("/api/rsync/", s.handleRsyncJobItem)
	s.mux.HandleFunc("/api/zfs/labels", s.handleZFSLabels)

	s.mux.HandleFunc("/api/settings", s.handleSettings)
	s.mux.HandleFunc("/api/settings/password", s.handleSettingsPassword)
	s.mux.HandleFunc("/api/system/autostart", s.handleSystemAutostart)
	s.mux.HandleFunc("/api/system/reboot", s.handleSystemReboot)
	s.mux.HandleFunc("/api/system/shutdown", s.handleSystemShutdown)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, env apiEnvelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

func (s *Server) startImportWatcher() {
	interval := 20 * time.Second
	s.refreshImportableCache()
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			s.refreshImportableCache()
		}
	}()
}

func (s *Server) importableSnapshot() []zfs.ImportablePool {
	s.importMu.Lock()
	defer s.importMu.Unlock()
	out := make([]zfs.ImportablePool, len(s.importablePools))
	copy(out, s.importablePools)
	return out
}

func (s *Server) refreshImportableCache() {
	pools, err := zfs.ListImportablePools(context.Background(), s.cfg)
	filtered := pools
	if err == nil {
		existing, listErr := zfs.ListPools(context.Background(), s.cfg)
		if listErr == nil {
			existingNames := map[string]bool{}
			for _, pool := range existing {
				existingNames[pool.Name] = true
			}
			filtered = make([]zfs.ImportablePool, 0, len(pools))
			for _, pool := range pools {
				if pool.Name == "" || existingNames[pool.Name] {
					continue
				}
				filtered = append(filtered, pool)
			}
		}
	}
	s.importMu.Lock()
	defer s.importMu.Unlock()
	s.importableChecked = time.Now()
	if err != nil {
		s.importablePools = nil
		s.importableErr = err.Error()
		return
	}
	s.importablePools = filtered
	s.importableErr = ""
}

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.Limits.MaxRequestBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(out); err != nil {
		if err == io.EOF {
			return true
		}
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid json", Details: err.Error()})
		return false
	}
	return true
}

func (s *Server) handleCmdRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Cmd string `json:"cmd"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	// Do not bind command execution to the request context; the POST handler returns
	// immediately after issuing a job ID, which would cancel the context and kill the job.
	job, err := s.jobs.Start(context.Background(), auth.UserFromContext(r.Context()), strings.TrimSpace(req.Cmd))
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "command rejected", Details: err.Error()})
		return
	}
	s.terminal.AddHistory(strings.TrimSpace(req.Cmd))
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"job_id": job.ID}})
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing job id"})
		return
	}
	id := parts[0]
	job, ok := s.jobs.Get(id)
	if !ok {
		s.writeJSON(w, http.StatusNotFound, apiEnvelope{Ok: false, Error: "job not found"})
		return
	}
	if len(parts) > 1 && parts[1] == "stream" {
		s.streamJob(w, r, job)
		return
	}
	job.mu.Lock()
	data := map[string]any{
		"id":        job.ID,
		"cmd":       job.Cmd,
		"args":      job.Args,
		"start":     job.Start.UTC().Format(time.RFC3339),
		"end":       job.End.UTC().Format(time.RFC3339),
		"done":      job.Done,
		"exit_code": job.ExitCode,
		"output":    job.Output,
		"truncated": job.Truncated,
		"duration":  duration(job.Start, job.End, job.Done),
	}
	job.mu.Unlock()
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: data})
}

func (s *Server) streamJob(w http.ResponseWriter, r *http.Request, job *Job) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	job.mu.Lock()
	initial := job.Output
	job.mu.Unlock()

	sendSSE(w, initial)
	flusher.Flush()

	ch := job.Subscribe()
	defer job.Unsubscribe(ch)

	notify := r.Context().Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-notify:
			return
		case chunk := <-ch:
			sendSSE(w, chunk)
			flusher.Flush()
		case <-ticker.C:
			job.mu.Lock()
			done := job.Done
			job.mu.Unlock()
			if done {
				return
			}
		}
	}
}

func sendSSE(w io.Writer, data string) {
	if data == "" {
		return
	}
	for _, line := range strings.Split(data, "\n") {
		fmt.Fprintf(w, "data: %s\n", line)
	}
	fmt.Fprint(w, "\n")
}

func duration(start, end time.Time, done bool) string {
	if !done {
		return "running"
	}
	if end.Before(start) {
		return "unknown"
	}
	return end.Sub(start).Truncate(time.Second).String()
}

func (s *Server) handleSambaUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		users, err := samba.ListUsers(r.Context(), s.cfg)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list users failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: users})
	case http.MethodPost:
		var req struct {
			Username        string `json:"username"`
			Password        string `json:"password"`
			PasswordConfirm string `json:"password_confirm"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Password == "" || req.Password != req.PasswordConfirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "passwords do not match"})
			return
		}
		res, err := samba.AddUser(r.Context(), s.cfg, req.Username, req.Password)
		s.audit.Log(auth.UserFromContext(r.Context()), "samba.add_user", fmt.Sprintf("%s -a %s", s.cfg.Paths.SMBPasswd, req.Username), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "add user failed", Details: res.Stderr})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"user": req.Username}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleSambaUserAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/samba/users/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing action"})
		return
	}
	username := parts[0]
	action := parts[1]

	var req struct {
		Password string `json:"password"`
		Confirm  bool   `json:"confirm"`
	}
	if r.Method == http.MethodPost {
		_ = s.decodeJSON(w, r, &req)
	}

	switch action {
	case "enable":
		res, err := samba.EnableUser(r.Context(), s.cfg, username)
		s.audit.Log(auth.UserFromContext(r.Context()), "samba.enable_user", fmt.Sprintf("%s -e %s", s.cfg.Paths.SMBPasswd, username), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "enable failed", Details: res.Stderr})
			return
		}
	case "disable":
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		res, err := samba.DisableUser(r.Context(), s.cfg, username)
		s.audit.Log(auth.UserFromContext(r.Context()), "samba.disable_user", fmt.Sprintf("%s -d %s", s.cfg.Paths.SMBPasswd, username), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "disable failed", Details: res.Stderr})
			return
		}
	case "delete":
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		res, err := samba.DeleteUser(r.Context(), s.cfg, username)
		s.audit.Log(auth.UserFromContext(r.Context()), "samba.delete_user", fmt.Sprintf("%s -x %s", s.cfg.Paths.SMBPasswd, username), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "delete failed", Details: res.Stderr})
			return
		}
	case "passwd":
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		if req.Password == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "password required"})
			return
		}
		res, err := samba.PasswdUser(r.Context(), s.cfg, username, req.Password)
		s.audit.Log(auth.UserFromContext(r.Context()), "samba.passwd_user", fmt.Sprintf("%s -s %s", s.cfg.Paths.SMBPasswd, username), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "password change failed", Details: res.Stderr})
			return
		}
	default:
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "unknown action"})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"user": username}})
}

func (s *Server) handleSambaShares(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		shares, err := samba.ListShares(s.cfg.Samba.IncludeFile)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list shares failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: shares})
	case http.MethodPost:
		var req samba.Share
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Name == "" || req.Path == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "name and path required"})
			return
		}
		shares, err := samba.ListShares(s.cfg.Samba.IncludeFile)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list shares failed", Details: err.Error()})
			return
		}
		shares = samba.UpsertShare(shares, req)
		if err := samba.SaveShares(s.cfg.Samba.IncludeFile, shares); err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save shares failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: req})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleSambaShare(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/samba/shares/")
	if name == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing share name"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req samba.Share
		if !s.decodeJSON(w, r, &req) {
			return
		}
		req.Name = name
		shares, err := samba.ListShares(s.cfg.Samba.IncludeFile)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list shares failed", Details: err.Error()})
			return
		}
		shares = samba.UpsertShare(shares, req)
		if err := samba.SaveShares(s.cfg.Samba.IncludeFile, shares); err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save shares failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: req})
	case http.MethodDelete:
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
		shares, err := samba.ListShares(s.cfg.Samba.IncludeFile)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list shares failed", Details: err.Error()})
			return
		}
		shares = samba.DeleteShare(shares, name)
		if err := samba.SaveShares(s.cfg.Samba.IncludeFile, shares); err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save shares failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"name": name}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleSambaTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	res, err := samba.TestConfig(r.Context(), s.cfg)
	s.audit.Log(auth.UserFromContext(r.Context()), "samba.testparm", fmt.Sprintf("%s %s", s.cfg.Paths.TestParm, strings.Join(s.cfg.Samba.TestparmArgs, " ")), res.ExitCode)
	if err != nil || res.ExitCode != 0 {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "testparm failed", Details: res.Stderr})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"output": res.Stdout}})
}

func (s *Server) handleSambaReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	res, err := samba.Reload(r.Context(), s.cfg)
	s.audit.Log(auth.UserFromContext(r.Context()), "samba.reload", fmt.Sprintf("%s %s", s.cfg.Paths.Service, strings.Join(s.cfg.Samba.ReloadArgs, " ")), res.ExitCode)
	if err != nil || res.ExitCode != 0 {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "reload failed", Details: res.Stderr})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"output": res.Stdout}})
}

func (s *Server) handleZFSDrives(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	geomDrives, err := drives.ListDrives(r.Context(), s.cfg)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list drives failed", Details: err.Error()})
		return
	}
	poolDevices, poolErr := zfs.ListPoolDevices(r.Context(), s.cfg)
	l2size, l2Err := zfs.L2ARCSize(r.Context(), s.cfg)
	labelMap, labelErr := drives.ListLabels(r.Context(), s.cfg)
	pools, poolsErr := zfs.ListPools(r.Context(), s.cfg)
	errors := map[string]string{}
	if poolErr != nil {
		errors["pool_devices"] = poolErr.Error()
	}
	if l2Err != nil {
		errors["cache"] = l2Err.Error()
	}
	if labelErr != nil {
		errors["labels"] = labelErr.Error()
	}
	if poolsErr != nil {
		errors["pools"] = poolsErr.Error()
	}

	type driveView struct {
		Name        string `json:"name"`
		Mediasize   string `json:"mediasize"`
		Description string `json:"description"`
		Ident       string `json:"ident"`
		Pool        string `json:"pool"`
		Role        string `json:"role"`
		Alloc       string `json:"alloc"`
		Free        string `json:"free"`
		Size        string `json:"size"`
	}

	usageByName := map[string]zfs.PoolDevice{}
	usageByBase := map[string]zfs.PoolDevice{}
	for _, dev := range poolDevices {
		key := strings.ToLower(dev.Name)
		usageByName[key] = dev
		base := strings.ToLower(baseDeviceName(dev.Name))
		if base != key {
			if _, ok := usageByBase[base]; !ok {
				usageByBase[base] = dev
			}
		}
	}

	mapped := map[string]struct{}{}
	driveSizeByName := map[string]string{}
	for _, drive := range geomDrives {
		if drive.Name == "" {
			continue
		}
		driveSizeByName[strings.ToLower(drive.Name)] = drive.Mediasize
	}
	views := make([]driveView, 0, len(geomDrives))
	for _, drive := range geomDrives {
		key := strings.ToLower(drive.Name)
		usage, ok := usageByName[key]
		if !ok {
			base := strings.ToLower(baseDeviceName(drive.Name))
			if candidate, found := usageByBase[base]; found {
				usage = candidate
				ok = true
			}
		}
		view := driveView{
			Name:        drive.Name,
			Mediasize:   drive.Mediasize,
			Description: drive.Description,
			Ident:       drive.Ident,
		}
		if ok {
			view.Pool = usage.Pool
			view.Role = usage.Role
			view.Alloc = usage.Alloc
			view.Free = usage.Free
			view.Size = usage.Size
			mapped[strings.ToLower(usage.Name)] = struct{}{}
		}
		if view.Size == "" {
			view.Size = drive.Mediasize
		}
		if view.Free == "" && view.Size != "" && view.Pool == "" {
			view.Free = view.Size
		}
		views = append(views, view)
	}

	for _, usage := range poolDevices {
		key := strings.ToLower(usage.Name)
		if _, ok := mapped[key]; ok {
			continue
		}
		view := driveView{
			Name:  usage.Name,
			Pool:  usage.Pool,
			Role:  usage.Role,
			Alloc: usage.Alloc,
			Free:  usage.Free,
			Size:  usage.Size,
		}
		views = append(views, view)
	}

	type cacheDeviceView struct {
		Name  string `json:"name"`
		Size  string `json:"size"`
		Alloc string `json:"alloc"`
		Free  string `json:"free"`
	}
	cacheDevices := []cacheDeviceView{}
	var cacheTotal int64
	cacheSeen := map[string]struct{}{}
	if poolsErr == nil {
		for _, pool := range pools {
			devs, err := zfs.PoolCacheDevices(r.Context(), s.cfg, pool.Name)
			if err != nil {
				continue
			}
			for _, dev := range devs {
				if dev == "" {
					continue
				}
				if _, ok := cacheSeen[dev]; ok {
					continue
				}
				cacheSeen[dev] = struct{}{}
				size := ""
				if labelMap != nil {
					if provider, ok := labelMap[dev]; ok {
						size = lookupDriveSize(provider, driveSizeByName)
					}
				}
				if size == "" {
					size = lookupDriveSize(dev, driveSizeByName)
				}
				cacheDevices = append(cacheDevices, cacheDeviceView{
					Name: dev,
					Size: size,
				})
				if bytes, ok := parseGeomBytes(size); ok {
					cacheTotal += bytes
				}
			}
		}
	}

	data := map[string]any{
		"drives": views,
		"cache": map[string]any{
			"used_bytes":  l2size,
			"total_bytes": cacheTotal,
			"devices":     cacheDevices,
		},
	}
	if len(errors) > 0 {
		data["errors"] = errors
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: data})
}

func (s *Server) handleZFSMounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		data, err := zfs.ListMounts(r.Context(), s.cfg)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list mounts failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: data})
	case http.MethodPost:
		var req struct {
			Dataset string `json:"dataset"`
			Action  string `json:"action"`
			Confirm bool   `json:"confirm"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		req.Dataset = strings.TrimSpace(req.Dataset)
		req.Action = strings.ToLower(strings.TrimSpace(req.Action))
		if req.Dataset == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dataset required"})
			return
		}
		if !zfs.ValidateDataset(s.cfg, req.Dataset) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		if req.Action == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "action required"})
			return
		}
		switch req.Action {
		case "mount":
			res, err := zfs.MountDataset(r.Context(), s.cfg, req.Dataset)
			s.audit.Log(auth.UserFromContext(r.Context()), "zfs.mount", fmt.Sprintf("%s mount %s", s.cfg.Paths.ZFS, req.Dataset), res.ExitCode)
			if err != nil || res.ExitCode != 0 {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "mount failed", Details: res.Stderr})
				return
			}
			s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"dataset": req.Dataset}})
		case "unmount":
			if !req.Confirm {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
				return
			}
			res, err := zfs.UnmountDataset(r.Context(), s.cfg, req.Dataset)
			s.audit.Log(auth.UserFromContext(r.Context()), "zfs.unmount", fmt.Sprintf("%s unmount %s", s.cfg.Paths.ZFS, req.Dataset), res.ExitCode)
			if err != nil || res.ExitCode != 0 {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "unmount failed", Details: res.Stderr})
				return
			}
			s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"dataset": req.Dataset}})
		default:
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "unknown action"})
		}
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSPools(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pools, err := zfs.ListPools(r.Context(), s.cfg)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list pools failed", Details: err.Error()})
			return
		}
		cacheByPool := map[string][]string{}
		for _, pool := range pools {
			devices, err := zfs.PoolCacheDevices(r.Context(), s.cfg, pool.Name)
			if err != nil {
				continue
			}
			if len(devices) > 0 {
				cacheByPool[pool.Name] = devices
			}
		}
		type poolView struct {
			Name         string   `json:"name"`
			Size         string   `json:"size"`
			Alloc        string   `json:"alloc"`
			Free         string   `json:"free"`
			Health       string   `json:"health"`
			Cached       bool     `json:"cached"`
			CacheDevices []string `json:"cache_devices"`
		}
		views := make([]poolView, 0, len(pools))
		for _, pool := range pools {
			cacheDevices := cacheByPool[pool.Name]
			views = append(views, poolView{
				Name:         pool.Name,
				Size:         pool.Size,
				Alloc:        pool.Alloc,
				Free:         pool.Free,
				Health:       pool.Health,
				Cached:       len(cacheDevices) > 0,
				CacheDevices: cacheDevices,
			})
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: views})
	case http.MethodPost:
		var req struct {
			Name    string   `json:"name"`
			Vdevs   []string `json:"vdevs"`
			Cache   []string `json:"cache"`
			Confirm bool     `json:"confirm"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.Vdevs = cleanList(req.Vdevs)
		req.Cache = cleanList(req.Cache)
		if !zfs.ValidPoolName(req.Name) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid pool name"})
			return
		}
		if len(req.Vdevs) == 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "at least one device required"})
			return
		}
		res, err := zfs.CreatePool(r.Context(), s.cfg, req.Name, req.Vdevs, req.Cache)
		command := fmt.Sprintf("%s create %s %s", s.cfg.Paths.ZPool, req.Name, strings.Join(req.Vdevs, " "))
		if len(req.Cache) > 0 {
			command = fmt.Sprintf("%s create %s %s cache %s", s.cfg.Paths.ZPool, req.Name, strings.Join(req.Vdevs, " "), strings.Join(req.Cache, " "))
		}
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.create_pool", command, res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			details := ""
			if err != nil {
				details = err.Error()
			} else {
				details = res.Stderr
			}
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool create failed", Details: details})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"pool": req.Name}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSImportable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	pools := s.importableSnapshot()
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: pools})
}

func (s *Server) handleZFSImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Pool    string   `json:"pool"`
		PoolID  string   `json:"pool_id"`
		Pools   []string `json:"pools"`
		PoolIDs []string `json:"pool_ids"`
		Confirm bool     `json:"confirm"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	if !req.Confirm {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
		return
	}
	poolIDs := cleanList(req.PoolIDs)
	if req.PoolID != "" {
		poolIDs = append(poolIDs, strings.TrimSpace(req.PoolID))
	}
	poolIDs = cleanList(poolIDs)
	pools := cleanList(req.Pools)
	if req.Pool != "" {
		pools = append(pools, strings.TrimSpace(req.Pool))
	}
	pools = cleanList(pools)
	identifiers := append([]string{}, poolIDs...)
	identifiers = append(identifiers, pools...)
	identifiers = cleanList(identifiers)
	if len(identifiers) == 0 {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool name required"})
		return
	}
	for _, id := range identifiers {
		if !zfs.ValidPoolName(id) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid pool identifier", Details: id})
			return
		}
	}
	for _, id := range identifiers {
		res, err := zfs.ImportPool(r.Context(), s.cfg, id)
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.pool_import", fmt.Sprintf("%s import %s", s.cfg.Paths.ZPool, id), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			details := ""
			if err != nil {
				details = err.Error()
			} else {
				details = res.Stderr
			}
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool import failed", Details: details})
			return
		}
	}
	s.refreshImportableCache()
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string][]string{"pools": identifiers}})
}

func (s *Server) handleZFSPoolStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	pool := r.URL.Query().Get("pool")
	if pool == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool required"})
		return
	}
	res, err := zfs.PoolStatus(r.Context(), s.cfg, pool)
	s.audit.Log(auth.UserFromContext(r.Context()), "zfs.pool_status", fmt.Sprintf("%s status -v %s", s.cfg.Paths.ZPool, pool), res.ExitCode)
	if err != nil || res.ExitCode != 0 {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "status failed", Details: res.Stderr})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"output": res.Stdout}})
}

func (s *Server) handleZFSPoolItem(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/zfs/pools/")
	name = strings.TrimSpace(name)
	if name == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool name required"})
		return
	}
	if !zfs.ValidPoolName(name) {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid pool name"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req struct {
			Property string `json:"property"`
			Value    string `json:"value"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		prop := strings.TrimSpace(req.Property)
		val := strings.TrimSpace(req.Value)
		if prop == "" || val == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "property and value required"})
			return
		}
		res, err := zfs.SetPoolProperty(r.Context(), s.cfg, name, prop, val)
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.pool_set", fmt.Sprintf("%s set %s=%s %s", s.cfg.Paths.ZPool, prop, val, name), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			details := ""
			if err != nil {
				details = err.Error()
			} else {
				details = res.Stderr
			}
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "pool update failed", Details: details})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"pool": name, "property": prop, "value": val}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSDatasets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		data, err := zfs.ListDatasets(r.Context(), s.cfg)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list datasets failed", Details: err.Error()})
			return
		}
		type datasetView struct {
			Name       string `json:"name"`
			Type       string `json:"type"`
			Used       string `json:"used"`
			Available  string `json:"available"`
			Referenced string `json:"referenced"`
			Mountpoint string `json:"mountpoint"`
		}
		views := make([]datasetView, 0, len(data))
		for _, ds := range data {
			views = append(views, datasetView{
				Name:       ds.Name,
				Type:       ds.Type,
				Used:       ds.Used,
				Available:  ds.Available,
				Referenced: ds.Referenced,
				Mountpoint: ds.Mountpoint,
			})
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: views})
	case http.MethodPost:
		var req struct {
			Name       string            `json:"name"`
			Kind       string            `json:"kind"`
			Size       string            `json:"size"`
			Properties map[string]string `json:"properties"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Name == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dataset name required"})
			return
		}
		if !zfs.ValidDatasetName(req.Name) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		if !zfs.ValidateDataset(s.cfg, req.Name) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		kind := strings.ToLower(strings.TrimSpace(req.Kind))
		if kind == "" {
			kind = "filesystem"
		}
		if kind != "filesystem" && kind != "volume" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset kind"})
			return
		}
		if kind == "volume" && strings.TrimSpace(req.Size) == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "volume size required"})
			return
		}
		props := filterDatasetProps(req.Properties)
		res, err := zfs.CreateDataset(r.Context(), s.cfg, req.Name, kind, strings.TrimSpace(req.Size), props)
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.create_dataset", fmt.Sprintf("%s create %s", s.cfg.Paths.ZFS, req.Name), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			details := ""
			if err != nil {
				details = err.Error()
			} else {
				details = res.Stderr
			}
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "create dataset failed", Details: details})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"dataset": req.Name}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSDatasetItem(w http.ResponseWriter, r *http.Request) {
	rawName := strings.TrimPrefix(r.URL.Path, "/api/zfs/datasets/")
	if rawName == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing dataset name"})
		return
	}
	name, err := url.PathUnescape(rawName)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
		return
	}
	if !zfs.ValidDatasetName(name) {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
		return
	}
	if !zfs.ValidateDataset(s.cfg, name) {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req struct {
			NewName    string            `json:"new_name"`
			Properties map[string]string `json:"properties"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		newName := strings.TrimSpace(req.NewName)
		if newName != "" {
			if !zfs.ValidDatasetName(newName) {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
				return
			}
			if !zfs.ValidateDataset(s.cfg, newName) {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
				return
			}
		}
		props := filterDatasetProps(req.Properties)
		if newName == "" && len(props) == 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "no updates provided"})
			return
		}
		if newName != "" && newName != name {
			res, err := zfs.RenameDataset(r.Context(), s.cfg, name, newName)
			s.audit.Log(auth.UserFromContext(r.Context()), "zfs.rename_dataset", fmt.Sprintf("%s rename %s %s", s.cfg.Paths.ZFS, name, newName), res.ExitCode)
			if err != nil || res.ExitCode != 0 {
				details := ""
				if err != nil {
					details = err.Error()
				} else {
					details = res.Stderr
				}
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "rename dataset failed", Details: details})
				return
			}
			name = newName
		}
		if len(props) > 0 {
			res, err := zfs.SetDatasetProperties(r.Context(), s.cfg, name, props)
			s.audit.Log(auth.UserFromContext(r.Context()), "zfs.set_properties", fmt.Sprintf("%s set %s", s.cfg.Paths.ZFS, name), res.ExitCode)
			if err != nil || res.ExitCode != 0 {
				details := ""
				if err != nil {
					details = err.Error()
				} else {
					details = res.Stderr
				}
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "update dataset failed", Details: details})
				return
			}
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"dataset": name}})
	case http.MethodDelete:
		var req struct {
			Confirm   bool `json:"confirm"`
			Recursive bool `json:"recursive"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		res, err := zfs.DestroyDataset(r.Context(), s.cfg, name, req.Recursive)
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.destroy_dataset", fmt.Sprintf("%s destroy %s", s.cfg.Paths.ZFS, name), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			details := ""
			if err != nil {
				details = err.Error()
			} else {
				details = res.Stderr
			}
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "destroy dataset failed", Details: details})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"dataset": name}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSSnapshots(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		dataset := r.URL.Query().Get("dataset")
		if dataset == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dataset required"})
			return
		}
		if !zfs.ValidateDataset(s.cfg, dataset) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		snaps, err := zfs.ListSnapshots(r.Context(), s.cfg, dataset)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list snapshots failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: snaps})
	case http.MethodPost:
		var req struct {
			Dataset   string `json:"dataset"`
			Prefix    string `json:"prefix"`
			Name      string `json:"name"`
			Recursive bool   `json:"recursive"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Dataset == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dataset required"})
			return
		}
		if !zfs.ValidateDataset(s.cfg, req.Dataset) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		name := req.Name
		if name == "" || name == "auto" {
			prefix := req.Prefix
			if prefix == "" {
				prefix = s.cfg.ZFS.SnapshotPrefix
			}
			if !zfs.ValidSnapshotToken(prefix) {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid snapshot prefix"})
				return
			}
			name = zfs.BuildSnapshotName(prefix, time.Now())
		} else if !zfs.ValidSnapshotName(name) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid snapshot name"})
			return
		}
		res, err := zfs.CreateSnapshot(r.Context(), s.cfg, req.Dataset, name, req.Recursive)
		command := fmt.Sprintf("%s snapshot %s@%s", s.cfg.Paths.ZFS, req.Dataset, name)
		if req.Recursive {
			command = fmt.Sprintf("%s snapshot -r %s@%s", s.cfg.Paths.ZFS, req.Dataset, name)
		}
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.create_snapshot", command, res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "snapshot create failed", Details: res.Stderr})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"snapshot": req.Dataset + "@" + name}})
	case http.MethodDelete:
		var req struct {
			Name    string `json:"name"`
			Confirm bool   `json:"confirm"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		if req.Name == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "snapshot name required"})
			return
		}
		res, err := zfs.DestroySnapshot(r.Context(), s.cfg, req.Name)
		s.audit.Log(auth.UserFromContext(r.Context()), "zfs.destroy_snapshot", fmt.Sprintf("%s destroy %s", s.cfg.Paths.ZFS, req.Name), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "snapshot destroy failed", Details: res.Stderr})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"snapshot": req.Name}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		items := []cron.Schedule{}
		for _, item := range file.Items {
			if scheduleKind(item) == "snapshot" {
				items = append(items, item)
			}
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"items": items, "updated": file.Updated}})
	case http.MethodPost:
		var req struct {
			Dataset   string        `json:"dataset"`
			Retention int           `json:"retention"`
			Prefix    string        `json:"prefix"`
			Enabled   bool          `json:"enabled"`
			Schedule  cron.CronSpec `json:"schedule"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Dataset == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dataset required"})
			return
		}
		if !zfs.ValidateDataset(s.cfg, req.Dataset) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		item := cron.Schedule{
			Type:      "snapshot",
			Dataset:   req.Dataset,
			Retention: req.Retention,
			Prefix:    req.Prefix,
			Enabled:   req.Enabled,
			Cron:      normalizeCron(req.Schedule),
		}
		file.Items = cron.Upsert(file.Items, item)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleScheduleItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/zfs/schedules/")
	if id == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing id"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req scheduleUpdateRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if req.Dataset != "" && !zfs.ValidateDataset(s.cfg, req.Dataset) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid dataset name"})
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		if req.Toggle {
			file.Items = cron.Toggle(file.Items, id)
		} else {
			file.Items = updateSchedule(file.Items, id, req, s.cfg)
		}
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	case http.MethodDelete:
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
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		file.Items = cron.Delete(file.Items, id)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSReplication(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		type replicationView struct {
			ID        string        `json:"id"`
			Source    string        `json:"source"`
			Target    string        `json:"target"`
			Retention int           `json:"retention"`
			Prefix    string        `json:"prefix"`
			Recursive bool          `json:"recursive"`
			Force     bool          `json:"force"`
			Enabled   bool          `json:"enabled"`
			Schedule  cron.CronSpec `json:"schedule"`
			Cron      string        `json:"cron"`
		}
		views := []replicationView{}
		for _, item := range file.Items {
			if scheduleKind(item) != "replication" {
				continue
			}
			meta := item.Meta
			if meta == nil {
				meta = map[string]string{}
			}
			views = append(views, replicationView{
				ID:        item.ID,
				Source:    meta["source"],
				Target:    meta["target"],
				Retention: metaInt(meta, "retention", item.Retention),
				Prefix:    metaValue(meta, "prefix", item.Prefix),
				Recursive: metaBool(meta, "recursive"),
				Force:     metaBool(meta, "force"),
				Enabled:   item.Enabled,
				Schedule:  item.Cron,
				Cron:      item.RawCron,
			})
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"items": views, "updated": file.Updated}})
	case http.MethodPost:
		var req replicationRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		req.Source = strings.TrimSpace(req.Source)
		req.Target = strings.TrimSpace(req.Target)
		if req.Source == "" || req.Target == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "source and target required"})
			return
		}
		if req.Source == req.Target {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "source and target must differ"})
			return
		}
		if !zfs.ValidDatasetName(req.Source) || !zfs.ValidateDataset(s.cfg, req.Source) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid source dataset"})
			return
		}
		if !zfs.ValidDatasetName(req.Target) || !zfs.ValidateDataset(s.cfg, req.Target) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid target dataset"})
			return
		}
		prefix := strings.TrimSpace(req.Prefix)
		if prefix == "" {
			if s.cfg.ZFS.SnapshotPrefix != "" {
				prefix = s.cfg.ZFS.SnapshotPrefix + "-repl"
			} else {
				prefix = "replication"
			}
		}
		if !zfs.ValidSnapshotToken(prefix) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid prefix"})
			return
		}
		if req.Retention < 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "retention must be >= 0"})
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		item := cron.Schedule{
			Type:      "replication",
			Enabled:   req.Enabled,
			Cron:      normalizeCron(req.Schedule),
			Retention: req.Retention,
			Prefix:    prefix,
			Meta: map[string]string{
				"type":      "replication",
				"source":    req.Source,
				"target":    req.Target,
				"prefix":    prefix,
				"retention": strconv.Itoa(req.Retention),
				"recursive": boolToIntString(req.Recursive),
				"force":     boolToIntString(req.Force),
			},
		}
		file.Items = cron.Upsert(file.Items, item)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSReplicationItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/zfs/replication/")
	if id == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing id"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req replicationUpdateRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		if req.Toggle {
			file.Items = cron.Toggle(file.Items, id)
		} else {
			updatedItems, err := updateReplication(file.Items, id, req, s.cfg)
			if err != nil {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "update failed", Details: err.Error()})
				return
			}
			file.Items = updatedItems
		}
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	case http.MethodDelete:
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
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		file.Items = cron.Delete(file.Items, id)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleRsyncJobs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		type rsyncView struct {
			ID       string        `json:"id"`
			Source   string        `json:"source"`
			Target   string        `json:"target"`
			Mode     string        `json:"mode"`
			Flags    string        `json:"flags"`
			Enabled  bool          `json:"enabled"`
			Schedule cron.CronSpec `json:"schedule"`
			Cron     string        `json:"cron"`
		}
		views := []rsyncView{}
		for _, item := range file.Items {
			if scheduleKind(item) != "rsync" {
				continue
			}
			meta := item.Meta
			if meta == nil {
				meta = map[string]string{}
			}
			views = append(views, rsyncView{
				ID:       item.ID,
				Source:   meta["source"],
				Target:   meta["target"],
				Mode:     metaValue(meta, "mode", "mirror"),
				Flags:    meta["flags"],
				Enabled:  item.Enabled,
				Schedule: item.Cron,
				Cron:     item.RawCron,
			})
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"items": views, "updated": file.Updated}})
	case http.MethodPost:
		var req rsyncRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		req.Source = strings.TrimSpace(req.Source)
		req.Target = strings.TrimSpace(req.Target)
		req.Mode = strings.ToLower(strings.TrimSpace(req.Mode))
		req.Flags = strings.TrimSpace(req.Flags)
		if req.Source == "" || req.Target == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "source and target required"})
			return
		}
		if req.Source == req.Target {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "source and target must differ"})
			return
		}
		if !validRsyncPath(req.Source) || !validRsyncPath(req.Target) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid source or target path"})
			return
		}
		mode := req.Mode
		if mode == "" {
			mode = "mirror"
		}
		flags := req.Flags
		if flags == "" {
			flags = rsyncFlagsForMode(mode)
		}
		if mode == "custom" && flags == "" {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "custom mode requires flags"})
			return
		}
		if strings.ContainsAny(flags, " \t") {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "flags must be comma-separated without spaces"})
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		item := cron.Schedule{
			Type:    "rsync",
			Enabled: req.Enabled,
			Cron:    normalizeCron(req.Schedule),
			Meta: map[string]string{
				"type":   "rsync",
				"source": req.Source,
				"target": req.Target,
				"mode":   mode,
				"flags":  flags,
			},
		}
		file.Items = cron.Upsert(file.Items, item)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleRsyncJobItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/rsync/")
	if id == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing id"})
		return
	}
	switch r.Method {
	case http.MethodPut:
		var req rsyncUpdateRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		if req.Toggle {
			file.Items = cron.Toggle(file.Items, id)
		} else {
			updatedItems, err := updateRsync(file.Items, id, req)
			if err != nil {
				s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "update failed", Details: err.Error()})
				return
			}
			file.Items = updatedItems
		}
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	case http.MethodDelete:
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
		file, err := cron.Load(s.cfg.Cron.CronFile, s.cfg.Cron.CronUser)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "read cron failed", Details: err.Error()})
			return
		}
		file.Items = cron.Delete(file.Items, id)
		updated, err := s.saveCronFile(file)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "save cron failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"updated": updated}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) handleZFSLabels(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		labels, err := drives.ListLabels(r.Context(), s.cfg)
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "list labels failed", Details: err.Error()})
			return
		}
		type labelView struct {
			Label    string `json:"label"`
			Provider string `json:"provider"`
		}
		views := make([]labelView, 0, len(labels))
		for label, provider := range labels {
			views = append(views, labelView{Label: label, Provider: provider})
		}
		sort.Slice(views, func(i, j int) bool {
			return views[i].Label < views[j].Label
		})
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: views})
	case http.MethodPost:
		var req struct {
			Label    string `json:"label"`
			Provider string `json:"provider"`
			Confirm  bool   `json:"confirm"`
		}
		if !s.decodeJSON(w, r, &req) {
			return
		}
		if !req.Confirm {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "confirmation required"})
			return
		}
		label := strings.TrimSpace(req.Label)
		label = strings.TrimPrefix(label, "gpt/")
		provider := strings.TrimSpace(req.Provider)
		if !validGeomLabel(label) {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid label"})
			return
		}
		if provider == "" || strings.ContainsAny(provider, " \t") {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "invalid provider"})
			return
		}
		res, err := drives.CreateGPTLabel(r.Context(), s.cfg, label, provider)
		s.audit.Log(auth.UserFromContext(r.Context()), "geom.label", fmt.Sprintf("%s label label gpt/%s %s", s.cfg.Paths.Geom, label, provider), res.ExitCode)
		if err != nil || res.ExitCode != 0 {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "label create failed", Details: res.Stderr})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]string{"label": "gpt/" + label}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) binaryPath() string {
	if s.cfg.BinaryPath != "" {
		return s.cfg.BinaryPath
	}
	return "/usr/local/bin/raidraccoon"
}

func (s *Server) runCommand(ctx context.Context, absCmd string, args []string, stdin []byte) (execwrap.Result, error) {
	return execwrap.Run(ctx, absCmd, args, stdin, s.cfg.Limits)
}

func (s *Server) saveCronFile(file cron.File) (string, error) {
	updated, err := cron.Save(s.cfg.Cron.CronFile, file, s.binaryPath(), s.cfg.Cron.CronUser)
	if err == nil {
		return updated, nil
	}
	if !errors.Is(err, os.ErrPermission) {
		return "", err
	}

	tmpDir := os.TempDir()
	tmpPath := filepath.Join(tmpDir, fmt.Sprintf("raidraccoon-cron-%d.tmp", time.Now().UnixNano()))
	defer os.Remove(tmpPath)
	if _, tmpErr := cron.Save(tmpPath, file, s.binaryPath(), s.cfg.Cron.CronUser); tmpErr != nil {
		return "", tmpErr
	}

	res, runErr := s.runCommand(context.Background(), "/usr/bin/install", []string{"-m", "0644", tmpPath, s.cfg.Cron.CronFile}, nil)
	if runErr != nil {
		return "", runErr
	}
	if res.ExitCode != 0 {
		details := strings.TrimSpace(res.Stderr)
		if details == "" {
			details = "sudo install failed; ensure /usr/bin/install is allowed for the service user"
		}
		return "", fmt.Errorf("%s", details)
	}
	return time.Now().UTC().Format(time.RFC3339), nil
}

func normalizeCron(spec cron.CronSpec) cron.CronSpec {
	if spec.Minute == "" {
		spec.Minute = "0"
	}
	if spec.Hour == "" {
		spec.Hour = "0"
	}
	if spec.Dom == "" {
		spec.Dom = "*"
	}
	if spec.Month == "" {
		spec.Month = "*"
	}
	if spec.Dow == "" {
		spec.Dow = "*"
	}
	return spec
}

func updateSchedule(items []cron.Schedule, id string, req scheduleUpdateRequest, cfg config.Config) []cron.Schedule {
	for i := range items {
		if items[i].ID != id {
			continue
		}
		if items[i].Type == "" {
			items[i].Type = "snapshot"
		}
		if req.Dataset != "" && zfs.ValidateDataset(cfg, req.Dataset) {
			items[i].Dataset = req.Dataset
		}
		if req.Retention > 0 {
			items[i].Retention = req.Retention
		}
		if req.Prefix != "" {
			items[i].Prefix = req.Prefix
		}
		if req.Enabled != nil {
			items[i].Enabled = *req.Enabled
		}
		if req.Schedule.Minute != "" || req.Schedule.Hour != "" || req.Schedule.Dom != "" || req.Schedule.Month != "" || req.Schedule.Dow != "" {
			items[i].Cron = normalizeCron(req.Schedule)
		}
		break
	}
	return items
}

func updateReplication(items []cron.Schedule, id string, req replicationUpdateRequest, cfg config.Config) ([]cron.Schedule, error) {
	for i := range items {
		if items[i].ID != id {
			continue
		}
		if scheduleKind(items[i]) != "replication" {
			return items, fmt.Errorf("job type mismatch")
		}
		meta := items[i].Meta
		if meta == nil {
			meta = map[string]string{}
		}
		if req.Source != "" {
			source := strings.TrimSpace(req.Source)
			if !zfs.ValidDatasetName(source) || !zfs.ValidateDataset(cfg, source) {
				return items, fmt.Errorf("invalid source dataset")
			}
			meta["source"] = source
		}
		if req.Target != "" {
			target := strings.TrimSpace(req.Target)
			if !zfs.ValidDatasetName(target) || !zfs.ValidateDataset(cfg, target) {
				return items, fmt.Errorf("invalid target dataset")
			}
			meta["target"] = target
		}
		if req.Prefix != "" {
			prefix := strings.TrimSpace(req.Prefix)
			if !zfs.ValidSnapshotToken(prefix) {
				return items, fmt.Errorf("invalid prefix")
			}
			meta["prefix"] = prefix
			items[i].Prefix = prefix
		}
		if req.Retention != nil {
			if *req.Retention < 0 {
				return items, fmt.Errorf("retention must be >= 0")
			}
			meta["retention"] = strconv.Itoa(*req.Retention)
			items[i].Retention = *req.Retention
		}
		if req.Recursive != nil {
			meta["recursive"] = boolToIntString(*req.Recursive)
		}
		if req.Force != nil {
			meta["force"] = boolToIntString(*req.Force)
		}
		if req.Enabled != nil {
			items[i].Enabled = *req.Enabled
		}
		if req.Schedule.Minute != "" || req.Schedule.Hour != "" || req.Schedule.Dom != "" || req.Schedule.Month != "" || req.Schedule.Dow != "" {
			items[i].Cron = normalizeCron(req.Schedule)
		}
		meta["type"] = "replication"
		items[i].Meta = meta
		items[i].Type = "replication"
		return items, nil
	}
	return items, fmt.Errorf("job not found")
}

func updateRsync(items []cron.Schedule, id string, req rsyncUpdateRequest) ([]cron.Schedule, error) {
	for i := range items {
		if items[i].ID != id {
			continue
		}
		if scheduleKind(items[i]) != "rsync" {
			return items, fmt.Errorf("job type mismatch")
		}
		meta := items[i].Meta
		if meta == nil {
			meta = map[string]string{}
		}
		if req.Source != "" {
			source := strings.TrimSpace(req.Source)
			if !validRsyncPath(source) {
				return items, fmt.Errorf("invalid source path")
			}
			meta["source"] = source
		}
		if req.Target != "" {
			target := strings.TrimSpace(req.Target)
			if !validRsyncPath(target) {
				return items, fmt.Errorf("invalid target path")
			}
			meta["target"] = target
		}
		if req.Mode != "" {
			mode := strings.ToLower(strings.TrimSpace(req.Mode))
			meta["mode"] = mode
			if req.Flags == "" {
				flags := rsyncFlagsForMode(mode)
				if mode == "custom" && flags == "" {
					return items, fmt.Errorf("custom mode requires flags")
				}
				meta["flags"] = flags
			}
		}
		if req.Flags != "" {
			flags := strings.TrimSpace(req.Flags)
			if strings.ContainsAny(flags, " \t") {
				return items, fmt.Errorf("flags must be comma-separated without spaces")
			}
			meta["flags"] = flags
		}
		if req.Enabled != nil {
			items[i].Enabled = *req.Enabled
		}
		if req.Schedule.Minute != "" || req.Schedule.Hour != "" || req.Schedule.Dom != "" || req.Schedule.Month != "" || req.Schedule.Dow != "" {
			items[i].Cron = normalizeCron(req.Schedule)
		}
		meta["type"] = "rsync"
		items[i].Meta = meta
		items[i].Type = "rsync"
		return items, nil
	}
	return items, fmt.Errorf("job not found")
}

func filterDatasetProps(props map[string]string) map[string]string {
	if len(props) == 0 {
		return map[string]string{}
	}
	allowed := map[string]bool{
		"compression": true,
		"atime":       true,
		"mountpoint":  true,
		"canmount":    true,
		"readonly":    true,
		"quota":       true,
		"reservation": true,
		"volsize":     true,
	}
	out := map[string]string{}
	for key, val := range props {
		k := strings.ToLower(strings.TrimSpace(key))
		if k == "" || !allowed[k] {
			continue
		}
		v := strings.TrimSpace(val)
		if v == "" {
			continue
		}
		out[k] = v
	}
	return out
}

func scheduleKind(item cron.Schedule) string {
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

func metaBool(meta map[string]string, key string) bool {
	if meta == nil {
		return false
	}
	return meta[key] == "1" || strings.EqualFold(meta[key], "true")
}

func metaInt(meta map[string]string, key string, def int) int {
	if meta == nil {
		return def
	}
	val := strings.TrimSpace(meta[key])
	if val == "" {
		return def
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return n
}

func metaValue(meta map[string]string, key, def string) string {
	if meta == nil {
		return def
	}
	val := strings.TrimSpace(meta[key])
	if val == "" {
		return def
	}
	return val
}

func boolToIntString(val bool) string {
	if val {
		return "1"
	}
	return "0"
}

var geomLabelPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validGeomLabel(label string) bool {
	return label != "" && geomLabelPattern.MatchString(label)
}

func validRsyncPath(value string) bool {
	if value == "" {
		return false
	}
	if strings.ContainsAny(value, " \t") {
		return false
	}
	if strings.Contains(value, ":") {
		return true
	}
	return strings.HasPrefix(value, "/")
}

func rsyncFlagsForMode(mode string) string {
	switch mode {
	case "sync":
		return "-a,--stats"
	case "custom":
		return ""
	default:
		return "-a,--delete,--stats"
	}
}

var devicePartitionSuffix = regexp.MustCompile(`^(.*?)(p[0-9]+|s[0-9]+)$`)

func baseDeviceName(name string) string {
	value := strings.TrimSpace(strings.TrimPrefix(name, "/dev/"))
	if value == "" {
		return value
	}
	if strings.Contains(value, "/") {
		return value
	}
	if match := devicePartitionSuffix.FindStringSubmatch(value); len(match) == 3 {
		if match[1] != "" {
			return match[1]
		}
	}
	return value
}

func lookupDriveSize(name string, sizes map[string]string) string {
	if name == "" || len(sizes) == 0 {
		return ""
	}
	key := strings.ToLower(strings.TrimPrefix(name, "/dev/"))
	if val, ok := sizes[key]; ok {
		return val
	}
	base := strings.ToLower(baseDeviceName(name))
	if val, ok := sizes[base]; ok {
		return val
	}
	return ""
}

var geomSizePattern = regexp.MustCompile(`^\s*([0-9]+)`)

func parseGeomBytes(value string) (int64, bool) {
	if value == "" {
		return 0, false
	}
	match := geomSizePattern.FindStringSubmatch(strings.TrimSpace(value))
	if len(match) != 2 {
		return 0, false
	}
	out, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return out, true
}
