# RaidRaccoon Deluxe

Single-binary FreeBSD web UI. It manages ZFS, Samba, and scheduled snapshots. It also includes a safe terminal. The UI is embedded with Go stdlib only.

## Features
- Terminal page with SSE output streaming and allowlisted commands.
- Samba user and share management with `testparm` and service reload.
- ZFS pools, datasets, snapshots, and retention cleanup.
- Cron-backed snapshot schedules (app-managed block in cron file).
- HTTP Basic Auth with salted SHA-256 hash.
- Audit log with command and exit code.

## Build
```sh
go build -o raidraccoon ./cmd/raidraccoon
```
This produces a single executable with embedded UI assets.

## Install (FreeBSD service, recommended)
You can install from a release with one command. This pulls the newest GitHub release for your FreeBSD arch. It also sets up the service and config.
```sh
curl -fsSL https://github.com/szymon-zasada/raid-raccoon-deluxe/raw/main/install.sh | sh
```

You can also run it from the repo:
```sh
doas ./install.sh
```
Default behavior:
- Installs the binary to `/usr/local/bin/raidraccoon`.
- Creates `/usr/local/etc/raidraccoon.json`.
- Installs the rc.d script to `/usr/local/etc/rc.d/raidraccoon`.
- Creates the `raidraccoon` user and group.
- Installs a sudoers entry for required system commands.
- Enables and starts the service.
- Generates an admin password for a new config and prints it once.

Useful flags:
```sh
./install.sh --no-start --no-enable
./install.sh --config /path/to/raidraccoon.json
./install.sh --password 'your-password'
./install.sh --version v1.0.3
./install.sh --asset raidraccoon-freebsd-amd64
```

## Configure (first run)
Open the web UI after install. Use the printed admin password. Then update it in `System Settings`. Review `allowed_cmds` before running jobs. Keep the config in `/usr/local/etc/raidraccoon.json` unless you need a custom path.

## Quick start (FreeBSD)
```sh
# create service user
pw useradd raidraccoon -m -s /usr/sbin/nologin

# create config
./raidraccoon init

# set admin password
./raidraccoon passwd

# run server
./raidraccoon serve
```

Default config resolution (unless `--config` is provided):
- `RAIDRACCOON_CONFIG` environment variable.
- `/usr/local/etc/raidraccoon.json` if it exists.
- `raidraccoon.json` in the current working directory.

## Manual service install (rc.d autostart)
```sh
# install rc script
install -m 0555 contrib/rc.d/raidraccoon /usr/local/etc/rc.d/raidraccoon

# move config to a system path (adjust if you prefer a different location)
cp raidraccoon.json /usr/local/etc/raidraccoon.json
chown raidraccoon:raidraccoon /usr/local/etc/raidraccoon.json

# enable + start
sysrc raidraccoon_enable=YES
service raidraccoon start
```

Optional overrides via `sysrc`:
```sh
sysrc raidraccoon_user=raidraccoon
sysrc raidraccoon_command=/usr/local/bin/raidraccoon
sysrc raidraccoon_config=/usr/local/etc/raidraccoon.json
sysrc raidraccoon_flags="--unsafe"
```

## Config
- Example config: `raidraccoon.json.example`.
- Use `./raidraccoon passwd` to set the password hash and salt.
- `allowed_cmds` is enforced for the Terminal page (absolute paths only).
- `binary_path` is used by the scheduler to call `raidraccoon snapshot`.
- HTTP Basic Auth uses `auth.username`, `auth.salt_hex`, `auth.password_hash_hex`.

## Sudoers (Variant A)
All system actions are executed via `sudo -n <abs_cmd> <args...>`.

Create `/usr/local/etc/sudoers.d/raidraccoon`:
```sudoers
Defaults:raidraccoon secure_path="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
raidraccoon ALL=(ALL) NOPASSWD: /sbin/zfs, /sbin/zpool, /sbin/geom, /sbin/sysctl, /usr/sbin/service, /usr/local/bin/smbpasswd, /usr/local/bin/pdbedit, /usr/local/bin/testparm, /usr/local/bin/rsync, /usr/sbin/sysrc, /sbin/shutdown, /usr/bin/install
```

Ensure the binary and config are readable by the `raidraccoon` user.

## Samba config file
The app reads and writes the main Samba config directly:
```
/usr/local/etc/smb4.conf
```
The `[global]` section and preamble lines are preserved. Share sections are rewritten by the UI. Any `include = ...` lines are removed on save. If you want a different path, set `samba.include_file` in the config.

## Cron file ownership
The schedules API reads/writes the cron file (default `/etc/crontab`) and preserves non-managed lines.
Managed entries are marked with `# rrd:` metadata.

## Snapshot subcommand (cron target)
Example:
```sh
/usr/local/bin/raidraccoon snapshot --dataset tank/data --retention 7 --prefix nightly
```

## Manual click-through acceptance checklist
- Navigation loads without JS console errors; sidebar and menu links work.
- Every primary button triggers the expected API endpoint and updates UI.
- Forms validate inline; errors show the banner; success shows a toast.
- Destructive actions always require confirmation and show results.
- Lists refresh after actions; empty states are informative.

## Notes
- Password prompts echo in the terminal (stdlib only). Avoid typing in shared terminals.
- The UI includes a persistent banner: “sudo/root actions enabled”.
- Use `raidraccoon.json.example` as your baseline for secure defaults.
