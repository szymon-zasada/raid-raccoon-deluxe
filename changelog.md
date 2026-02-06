# Changelog

## 2026-02-06
- Grouped Samba Users/Shares under a single “Samba Settings” top nav entry.
- Grouped ZFS Snapshots/Schedules/Replication under a single “ZFS Snapshots” top nav entry.
- Switched ZFS snapshots subnav links to button-based navigation.
- Added automatic polling for importable ZFS pools with a confirmation popup to import them.
- Added server-side detection of importable ZFS pools to back the UI popup.
- Prefer ZFS pool import by numeric ID when available to avoid ambiguous pool name errors.
- Filter out already-imported pools from the importable list and refresh cache after import.
- Updated install.sh to download the latest GitHub release binary when a local build is unavailable.
- ZFS allowlist now defaults to allow all datasets when `zfs.allowed_prefixes` is empty.

## 2026-02-03
- Added `install.sh` to set up the FreeBSD service user, rc.d script, sudoers entry, config, and autostart/start flow.
- Standardized config path discovery with `RAIDRACCOON_CONFIG` and `/usr/local/etc/raidraccoon.json` defaults.
- Improved autostart guidance in Settings and refreshed the README install/sudoers docs.
- Added ZFS replication jobs with schedule builder, cron metadata typing, and CLI support (`replicate`).
- Added rsync jobs with presets/custom flags plus CLI support (`rsync`) and config path/allowlist updates.
- Added GPT label management in ZFS Mounts (create + list `gpt/<label>` via `geom label`).
- Added a shared subnav across Snapshots/Schedules/Replication and fixed replication source selection (dataset tree + dropdown sync).
- Filtered `/zfs/schedules` list to snapshot schedules only (replication/rsync have dedicated pages).

## 2026-01-30
- Switched default cron file to `/etc/crontab` and preserved non-managed lines when saving schedules.
- Added cron parsing for system crontab entries and imported existing `raidraccoon snapshot` lines without metadata.
- Allowed cron rendering without a user field when `cron_user` is empty.
- Updated README, reasoning, and example config to match the cron changes. 
- Tuned the ASCII logo sizing/contrast and sidebar layout so it fits cleanly.
- Added terminal upgrades: alias-based commands (no `/sbin` required), history + favorites (persisted in config), tab autocomplete, and a redesigned terminal UI.

## 2026-01-31
- Added a configurable dashboard landing page with donut widgets, drag-to-reorder layout, and overview summaries.
- Added ZFS datasets management tab with create/edit/destroy flows and dataset detail panel.
- Added dataset API support for create/update/destroy plus allowlisted property updates and clearer “dataset not allowed” messages.
- Switched snapshots + schedules to a shared expandable dataset picker, filtering to allowed datasets.
- Upgraded schedules UI with quick/interval presets, advanced cron editor, and cron preview.
- Added dataset size details (used/available/max approx) in the UI.
- Enabled dataset rename and volume size updates with a “Set Max” helper for zvols.
- Added dataset quota editing in the datasets UI.
- Added usage bars for datasets (used/max) and pools (allocated) in the UI.
- Added “Set Max” helper for dataset quotas.
- Quota “Set Max” now uses the pool's max size when available.
- Reworked dataset create/edit form layout to a fixed 3-column grid with reordered fields.
- Added a Settings tab to edit configuration, toggle autostart, and trigger reboot/shutdown actions.
- Added a FreeBSD rc.d script in `contrib/rc.d/raidraccoon` with install steps in the README.

## 2026-01-29
- Completed plan point 1: project skeleton, Go module, embed assets, CLI subcommands, config init/example.
- Completed plan point 2: sudo execution wrapper, absolute paths defaults, sudo banner, FreeBSD runtime notes.
- Completed plan point 3: Basic Auth, request/output/runtime limits, audit logger, error envelopes.
- Completed plan point 4: monochrome UI layout, checker background, menu/sidebar/window panels.
- Completed plan point 5: JS API helper, bindings, modals, toasts, keyboard handling.
- Completed plan point 6: command runner with jobs, polling, SSE streaming, terminal UI.
- Completed plan point 7: Samba users/shares CRUD, testparm/reload, include-file parser/writer.
- Completed plan point 8: ZFS pools/datasets/snapshots endpoints, dataset allow-prefix checks.
- Completed plan point 9: cron-backed schedules parsing/writing + UI builder.
- Completed plan point 10: README acceptance checklist and manual verification guidance.
- Added ZFS mounts page with drive inventory (geom) plus dataset mount/unmount actions.
- Added `serve --unsafe` flag to bypass command allowlist checks for terminal jobs.
