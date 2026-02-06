// Package httpd contains terminal state and API handlers.
package httpd

import (
	"errors"
	"net/http"
	"strings"
	"sync"

	"raidraccoon/internal/config"
)

type TerminalState struct {
	mu           sync.Mutex
	history      []string
	historyLimit int
}

func NewTerminalState(cfg config.Config) *TerminalState {
	limit := cfg.Terminal.HistoryLimit
	if limit <= 0 {
		limit = 20
	}
	return &TerminalState{historyLimit: limit}
}

func (t *TerminalState) AddHistory(cmd string) {
	cmd = strings.TrimSpace(cmd)
	if cmd == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.history = removeString(t.history, cmd)
	t.history = append([]string{cmd}, t.history...)
	if len(t.history) > t.historyLimit {
		t.history = t.history[:t.historyLimit]
	}
}

func (t *TerminalState) History() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string{}, t.history...)
}

func (t *TerminalState) HistoryLimit() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.historyLimit
}

func (t *TerminalState) SetHistoryLimit(limit int) {
	if limit <= 0 {
		limit = 20
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.historyLimit = limit
	if len(t.history) > t.historyLimit {
		t.history = t.history[:t.historyLimit]
	}
}

func (s *Server) handleTerminalMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	aliases, favorites := s.terminalConfig()
	data := map[string]any{
		"aliases":       aliases,
		"favorites":     favorites,
		"history":       s.terminal.History(),
		"history_limit": s.terminal.HistoryLimit(),
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: data})
}

func (s *Server) handleTerminalFavorites(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
		return
	}
	var req struct {
		Cmd      string `json:"cmd"`
		Favorite bool   `json:"favorite"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}
	cmd := strings.TrimSpace(req.Cmd)
	if cmd == "" {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "missing command"})
		return
	}
	favorites, err := s.updateFavorites(cmd, req.Favorite)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "favorite update failed", Details: err.Error()})
		return
	}
	s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"favorites": favorites}})
}

func (s *Server) terminalConfig() (map[string]string, []string) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	aliases := map[string]string{}
	for key, value := range s.cfg.Terminal.Aliases {
		aliases[key] = value
	}
	favorites := append([]string{}, s.cfg.Terminal.Favorites...)
	return aliases, favorites
}

func (s *Server) updateFavorites(cmd string, favorite bool) ([]string, error) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.cfg.ConfigPath == "" {
		return nil, errors.New("config path not set")
	}
	previous := append([]string{}, s.cfg.Terminal.Favorites...)
	next := previous
	if favorite {
		next = prependUnique(previous, cmd)
	} else {
		next = removeString(previous, cmd)
	}
	s.cfg.Terminal.Favorites = next
	if err := config.Save(s.cfg.ConfigPath, s.cfg); err != nil {
		s.cfg.Terminal.Favorites = previous
		return previous, err
	}
	return next, nil
}

func prependUnique(items []string, item string) []string {
	items = removeString(items, item)
	return append([]string{item}, items...)
}

func removeString(items []string, target string) []string {
	if len(items) == 0 {
		return items
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == target {
			continue
		}
		out = append(out, item)
	}
	return out
}
