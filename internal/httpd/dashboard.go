// Package httpd handles dashboard aggregation.
package httpd

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"raidraccoon/internal/config"
	"raidraccoon/internal/cron"
	"raidraccoon/internal/drives"
	"raidraccoon/internal/samba"
	"raidraccoon/internal/zfs"
)

type dashboardPoolsSummary struct {
	Count      int   `json:"count"`
	Healthy    int   `json:"healthy"`
	Degraded   int   `json:"degraded"`
	AllocBytes int64 `json:"alloc_bytes"`
	SizeBytes  int64 `json:"size_bytes"`
}

type dashboardDatasetsSummary struct {
	Count          int   `json:"count"`
	UsedBytes      int64 `json:"used_bytes"`
	AvailableBytes int64 `json:"available_bytes"`
}

type dashboardSnapshotsSummary struct {
	Count int `json:"count"`
}

type dashboardCacheSummary struct {
	UsedBytes  int64    `json:"used_bytes"`
	TotalBytes int64    `json:"total_bytes"`
	Devices    []string `json:"devices"`
	Present    bool     `json:"present"`
}

type dashboardSchedulesSummary struct {
	Count    int `json:"count"`
	Enabled  int `json:"enabled"`
	Disabled int `json:"disabled"`
}

type dashboardSambaSummary struct {
	Users  int `json:"users"`
	Shares int `json:"shares"`
}

type dashboardSettingsSummary struct {
	AutostartEnabled bool `json:"autostart_enabled"`
}

type dashboardSummary struct {
	Pools     dashboardPoolsSummary     `json:"pools"`
	Datasets  dashboardDatasetsSummary  `json:"datasets"`
	Snapshots dashboardSnapshotsSummary `json:"snapshots"`
	Cache     dashboardCacheSummary     `json:"cache"`
	Schedules dashboardSchedulesSummary `json:"schedules"`
	Samba     dashboardSambaSummary     `json:"samba"`
	Settings  dashboardSettingsSummary  `json:"settings"`
	Updated   string                    `json:"updated"`
}

type dashboardUpdateRequest struct {
	Widgets []config.DashboardWidget `json:"widgets"`
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfg := s.snapshotConfig()
		layout := normalizeDashboardWidgets(cfg.Dashboard.Widgets)
		summary, errs := s.buildDashboardSummary(r.Context(), cfg)
		data := map[string]any{
			"layout":  layout,
			"summary": summary,
		}
		if len(errs) > 0 {
			data["errors"] = errs
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: data})
	case http.MethodPut:
		var req dashboardUpdateRequest
		if !s.decodeJSON(w, r, &req) {
			return
		}
		layout := normalizeDashboardWidgets(req.Widgets)
		if err := s.saveDashboardLayout(layout); err != nil {
			s.writeJSON(w, http.StatusBadRequest, apiEnvelope{Ok: false, Error: "dashboard update failed", Details: err.Error()})
			return
		}
		s.writeJSON(w, http.StatusOK, apiEnvelope{Ok: true, Data: map[string]any{"layout": layout}})
	default:
		s.writeJSON(w, http.StatusMethodNotAllowed, apiEnvelope{Ok: false, Error: "method not allowed"})
	}
}

func (s *Server) saveDashboardLayout(layout []config.DashboardWidget) error {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	if s.cfg.ConfigPath == "" {
		return errConfigPathMissing()
	}
	previous := append([]config.DashboardWidget{}, s.cfg.Dashboard.Widgets...)
	s.cfg.Dashboard.Widgets = append([]config.DashboardWidget{}, layout...)
	if err := config.Save(s.cfg.ConfigPath, s.cfg); err != nil {
		s.cfg.Dashboard.Widgets = previous
		return err
	}
	return nil
}

func (s *Server) buildDashboardSummary(ctx context.Context, cfg config.Config) (dashboardSummary, map[string]string) {
	summary := dashboardSummary{Updated: time.Now().UTC().Format(time.RFC3339)}
	errs := map[string]string{}

	pools, err := zfs.ListPools(ctx, cfg)
	if err != nil {
		errs["pools"] = err.Error()
	} else {
		var allocTotal int64
		var sizeTotal int64
		healthy := 0
		degraded := 0
		for _, pool := range pools {
			if strings.EqualFold(pool.Health, "online") {
				healthy += 1
			} else {
				degraded += 1
			}
			if bytes, ok := parseSizeBytes(pool.Alloc); ok {
				allocTotal += bytes
			}
			if bytes, ok := parseSizeBytes(pool.Size); ok {
				sizeTotal += bytes
			}
		}
		summary.Pools = dashboardPoolsSummary{
			Count:      len(pools),
			Healthy:    healthy,
			Degraded:   degraded,
			AllocBytes: allocTotal,
			SizeBytes:  sizeTotal,
		}
	}

	datasets, err := zfs.ListDatasets(ctx, cfg)
	if err != nil {
		errs["datasets"] = err.Error()
	} else {
		var usedTotal int64
		var availTotal int64
		for _, ds := range datasets {
			if bytes, ok := parseSizeBytes(ds.Used); ok {
				usedTotal += bytes
			}
			if bytes, ok := parseSizeBytes(ds.Available); ok {
				availTotal += bytes
			}
		}
		summary.Datasets = dashboardDatasetsSummary{
			Count:          len(datasets),
			UsedBytes:      usedTotal,
			AvailableBytes: availTotal,
		}
	}

	snaps, err := zfs.ListSnapshots(ctx, cfg, "")
	if err != nil {
		errs["snapshots"] = err.Error()
	} else {
		summary.Snapshots = dashboardSnapshotsSummary{Count: len(snaps)}
	}

	cacheDevices := []string{}
	cacheTotal := int64(0)
	cacheUsed := int64(0)
	if pools != nil {
		labelMap, _ := drives.ListLabels(ctx, cfg)
		geomDrives, _ := drives.ListDrives(ctx, cfg)
		driveSizes := map[string]string{}
		for _, drive := range geomDrives {
			if drive.Name == "" {
				continue
			}
			driveSizes[strings.ToLower(drive.Name)] = drive.Mediasize
		}
		seen := map[string]struct{}{}
		for _, pool := range pools {
			devs, err := zfs.PoolCacheDevices(ctx, cfg, pool.Name)
			if err != nil {
				continue
			}
			for _, dev := range devs {
				if dev == "" {
					continue
				}
				if _, ok := seen[dev]; ok {
					continue
				}
				seen[dev] = struct{}{}
				cacheDevices = append(cacheDevices, dev)
				size := ""
				if labelMap != nil {
					if provider, ok := labelMap[dev]; ok {
						size = lookupDriveSize(provider, driveSizes)
					}
				}
				if size == "" {
					size = lookupDriveSize(dev, driveSizes)
				}
				if bytes, ok := parseGeomBytes(size); ok {
					cacheTotal += bytes
				}
			}
		}
	}
	if size, err := zfs.L2ARCSize(ctx, cfg); err == nil {
		cacheUsed = size
	} else if len(cacheDevices) == 0 {
		errs["cache"] = err.Error()
	}
	summary.Cache = dashboardCacheSummary{
		UsedBytes:  cacheUsed,
		TotalBytes: cacheTotal,
		Devices:    cacheDevices,
		Present:    len(cacheDevices) > 0,
	}

	file, err := cron.Load(cfg.Cron.CronFile, cfg.Cron.CronUser)
	if err != nil {
		errs["schedules"] = err.Error()
	} else {
		enabled := 0
		for _, item := range file.Items {
			if item.Enabled {
				enabled += 1
			}
		}
		summary.Schedules = dashboardSchedulesSummary{
			Count:    len(file.Items),
			Enabled:  enabled,
			Disabled: len(file.Items) - enabled,
		}
	}

	var sambaErrors []string
	users, err := samba.ListUsers(ctx, cfg)
	if err != nil {
		sambaErrors = append(sambaErrors, err.Error())
	} else {
		summary.Samba.Users = len(users)
	}
	shares, err := samba.ListShares(cfg.Samba.IncludeFile)
	if err != nil {
		sambaErrors = append(sambaErrors, err.Error())
	} else {
		summary.Samba.Shares = len(shares)
	}
	if len(sambaErrors) > 0 {
		errs["samba"] = strings.Join(sambaErrors, "; ")
	}

	meta := s.buildSettingsMeta(cfg)
	summary.Settings = dashboardSettingsSummary{AutostartEnabled: meta.AutostartEnabled}
	if meta.AutostartError != "" {
		errs["settings"] = meta.AutostartError
	}

	if len(errs) == 0 {
		return summary, nil
	}
	return summary, errs
}

var sizePattern = regexp.MustCompile(`^([0-9]*\.?[0-9]+)\s*([kKmMgGtTpPeE]?)[bB]?$`)

func parseSizeBytes(value string) (int64, bool) {
	raw := strings.TrimSpace(value)
	if raw == "" || raw == "-" || strings.EqualFold(raw, "none") {
		return 0, false
	}
	if raw == "0" {
		return 0, true
	}
	match := sizePattern.FindStringSubmatch(raw)
	if len(match) != 3 {
		return 0, false
	}
	num, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, false
	}
	unit := strings.ToUpper(match[2])
	scale := float64(1)
	switch unit {
	case "K":
		scale = 1024
	case "M":
		scale = 1024 * 1024
	case "G":
		scale = 1024 * 1024 * 1024
	case "T":
		scale = 1024 * 1024 * 1024 * 1024
	case "P":
		scale = 1024 * 1024 * 1024 * 1024 * 1024
	case "E":
		scale = 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	}
	return int64(num * scale), true
}

func normalizeDashboardWidgets(input []config.DashboardWidget) []config.DashboardWidget {
	defaults := config.DefaultDashboardWidgets()
	known := map[string]config.DashboardWidget{}
	for _, def := range defaults {
		known[def.ID] = def
	}
	out := make([]config.DashboardWidget, 0, len(defaults))
	seen := map[string]struct{}{}
	for _, item := range input {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		if _, ok := known[id]; !ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		out = append(out, config.DashboardWidget{ID: id, Enabled: item.Enabled})
		seen[id] = struct{}{}
	}
	for _, def := range defaults {
		if _, ok := seen[def.ID]; ok {
			continue
		}
		out = append(out, def)
		seen[def.ID] = struct{}{}
	}
	return out
}

func errConfigPathMissing() error {
	return errString("config path not set")
}

type errString string

func (e errString) Error() string { return string(e) }
