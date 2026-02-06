// Package httpd contains job tracking for streamed command execution.
package httpd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"raidraccoon/internal/config"
)

type JobManager struct {
	cfg   config.Config
	cfgMu sync.RWMutex
	mu    sync.Mutex
	jobs  map[string]*Job
	ttl   time.Duration
	audit func(user, action, command string, exitCode int)
}

// Job represents one privileged command execution request.
// Output is kept in-memory and streamed to clients via SSE.
type Job struct {
	ID        string    `json:"id"`
	Cmd       string    `json:"cmd"`
	Args      []string  `json:"args"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	ExitCode  int       `json:"exit_code"`
	Done      bool      `json:"done"`
	Output    string    `json:"output"`
	Truncated bool      `json:"truncated"`
	Limit     int64     `json:"-"`
	User      string    `json:"-"`

	mu     sync.Mutex
	buffer strings.Builder
	subs   map[chan string]struct{}
	cancel context.CancelFunc
}

// NewJobManager constructs a manager with an internal cleanup loop.
func NewJobManager(cfg config.Config, auditFn func(user, action, command string, exitCode int)) *JobManager {
	jm := &JobManager{cfg: cfg, jobs: map[string]*Job{}, ttl: 15 * time.Minute, audit: auditFn}
	go jm.cleanupLoop()
	return jm
}

// UpdateConfig refreshes command/runtime/output limits for newly started jobs.
func (jm *JobManager) UpdateConfig(cfg config.Config) {
	jm.cfgMu.Lock()
	jm.cfg = cfg
	jm.cfgMu.Unlock()
}

func (jm *JobManager) configSnapshot() config.Config {
	jm.cfgMu.RLock()
	defer jm.cfgMu.RUnlock()
	return jm.cfg
}

func (jm *JobManager) Start(ctx context.Context, user, command string) (*Job, error) {
	cfg := jm.configSnapshot()
	cmdPath, args, err := parseCommand(command, cfg)
	if err != nil {
		return nil, err
	}
	if !cfg.Unsafe && !isAllowed(cfg.AllowedCmds, cmdPath) {
		return nil, fmt.Errorf("command not in allowlist")
	}

	id := newID()
	job := &Job{ID: id, Cmd: cmdPath, Args: args, Start: time.Now(), subs: map[chan string]struct{}{}, Limit: cfg.Limits.MaxOutputBytes, User: user}
	jm.mu.Lock()
	jm.jobs[id] = job
	jm.mu.Unlock()

	go jm.runJob(ctx, job)
	return job, nil
}

// Get returns the current job record, if present.
func (jm *JobManager) Get(id string) (*Job, bool) {
	jm.mu.Lock()
	defer jm.mu.Unlock()
	job, ok := jm.jobs[id]
	return job, ok
}

func (jm *JobManager) runJob(ctx context.Context, job *Job) {
	cfg := jm.configSnapshot()
	execCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Limits.MaxRuntimeSeconds)*time.Second)
	if cfg.Limits.MaxRuntimeSeconds <= 0 {
		execCtx, cancel = context.WithTimeout(ctx, 120*time.Second)
	}
	job.cancel = cancel
	defer cancel()

	cmd := exec.CommandContext(execCtx, "sudo", append([]string{"-n", job.Cmd}, job.Args...)...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		job.finishError(err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		job.finishError(err)
		return
	}
	if err := cmd.Start(); err != nil {
		job.finishError(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go job.readStream(&wg, stdout)
	go job.readStream(&wg, stderr)

	err = cmd.Wait()
	wg.Wait()

	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if errors.Is(err, context.DeadlineExceeded) {
			exitCode = 124
		} else {
			exitCode = 1
		}
	}

	job.mu.Lock()
	job.Done = true
	job.End = time.Now()
	job.ExitCode = exitCode
	job.Output = job.buffer.String()
	job.mu.Unlock()

	if jm.audit != nil {
		jm.audit(job.User, "cmd.run", job.CommandString(), exitCode)
	}
}

func (job *Job) readStream(wg *sync.WaitGroup, r io.Reader) {
	defer wg.Done()
	reader := bufio.NewReader(r)
	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			chunk := string(buf[:n])
			job.append(chunk)
		}
		if err != nil {
			return
		}
	}
}

func (job *Job) append(chunk string) {
	job.mu.Lock()
	defer job.mu.Unlock()
	if int64(job.buffer.Len()+len(chunk)) > jobLimit(job) {
		job.Truncated = true
		remaining := int(jobLimit(job)) - job.buffer.Len()
		if remaining > 0 {
			job.buffer.WriteString(chunk[:remaining])
			job.Output = job.buffer.String()
			job.broadcast(chunk[:remaining])
		}
		return
	}
	job.buffer.WriteString(chunk)
	job.Output = job.buffer.String()
	job.broadcast(chunk)
}

func jobLimit(job *Job) int64 {
	if job.Limit <= 0 {
		return 1 << 20
	}
	return job.Limit
}

func (job *Job) broadcast(chunk string) {
	for ch := range job.subs {
		select {
		case ch <- chunk:
		default:
		}
	}
}

func (job *Job) Subscribe() chan string {
	job.mu.Lock()
	defer job.mu.Unlock()
	ch := make(chan string, 16)
	job.subs[ch] = struct{}{}
	return ch
}

// Unsubscribe stops streaming output to ch and closes it.
func (job *Job) Unsubscribe(ch chan string) {
	job.mu.Lock()
	defer job.mu.Unlock()
	delete(job.subs, ch)
	close(ch)
}

func (job *Job) finishError(err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	job.Done = true
	job.End = time.Now()
	job.ExitCode = 1
	job.buffer.WriteString(err.Error())
	job.Output = job.buffer.String()
	job.broadcast(err.Error())
}

func (job *Job) CommandString() string {
	return strings.TrimSpace(strings.Join(append([]string{job.Cmd}, job.Args...), " "))
}

func (jm *JobManager) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		jm.mu.Lock()
		for id, job := range jm.jobs {
			job.mu.Lock()
			done := job.Done
			end := job.End
			job.mu.Unlock()
			if done && time.Since(end) > jm.ttl {
				delete(jm.jobs, id)
			}
		}
		jm.mu.Unlock()
	}
}

func parseCommand(cmd string, cfg config.Config) (string, []string, error) {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return "", nil, fmt.Errorf("empty command")
	}
	if strings.HasPrefix(fields[0], "/") {
		return fields[0], fields[1:], nil
	}
	if target, ok := cfg.Terminal.Aliases[fields[0]]; ok {
		if !strings.HasPrefix(target, "/") {
			return "", nil, fmt.Errorf("alias must be absolute")
		}
		return target, fields[1:], nil
	}
	resolved := ""
	for _, allowed := range cfg.AllowedCmds {
		if filepath.Base(allowed) == fields[0] {
			if resolved != "" && resolved != allowed {
				return "", nil, fmt.Errorf("command alias is ambiguous")
			}
			resolved = allowed
		}
	}
	if resolved != "" {
		return resolved, fields[1:], nil
	}
	return "", nil, fmt.Errorf("command must be absolute or known alias")
}

func isAllowed(allowed []string, cmd string) bool {
	for _, a := range allowed {
		if a == cmd {
			return true
		}
	}
	return false
}

func newID() string {
	return fmt.Sprintf("job-%d", time.Now().UnixNano())
}
