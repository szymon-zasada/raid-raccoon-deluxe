// Package audit appends structured audit events to a log file.
package audit

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Logger struct {
	path string
	mu   sync.Mutex
}

// New returns a logger that appends audit events to path.
func New(path string) *Logger {
	return &Logger{path: path}
}

// SetPath switches the audit log destination.
func (l *Logger) SetPath(path string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.path = path
}

// Log appends a single line describing a security-relevant action.
// It is best-effort by design: failures to write the log should not crash the service.
func (l *Logger) Log(user, action, command string, exitCode int) {
	if l == nil || l.path == "" {
		return
	}
	line := fmt.Sprintf("%s user=%q action=%q command=%q exit=%d\n", time.Now().UTC().Format(time.RFC3339), user, action, command, exitCode)
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	_, _ = f.WriteString(line)
	_ = f.Close()
}
