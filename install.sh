#!/bin/sh
# RaidRaccoon Deluxe installer (FreeBSD)
# - Installs binary, config, rc.d service, sudoers entry
# - Creates service user/group and sets permissions
# - Optionally enables + starts service and sets admin password for a new config
set -e

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

Options:
  --bin PATH         Path to raidraccoon binary (default: download latest stable release).
  --version TAG      GitHub release tag to install (default: latest stable release).
  --asset NAME       Override GitHub release asset name (default: raidraccoon-<os>-<arch>).
  --config PATH      Config path (default: /usr/local/etc/raidraccoon.json).
  --user NAME        Service user (default: raidraccoon).
  --group NAME       Service group (default: raidraccoon).
  --prefix PATH      Install prefix (default: /usr/local).
  --no-enable        Do not enable service at boot.
  --no-start         Do not start service after install.
  --no-rc            Skip rc.d script install.
  --no-sudoers       Skip sudoers install.
  --password VALUE   Set admin password (only when creating a new config).
  --no-password      Keep default password (changeme) on new config.
  -h, --help         Show this help.
USAGE
}

if [ "$(/usr/bin/id -u)" -ne 0 ]; then
  echo "error: run as root (use doas/sudo)." >&2
  exit 1
fi

if [ "$(/usr/bin/uname -s)" != "FreeBSD" ]; then
  echo "error: install.sh targets FreeBSD." >&2
  exit 1
fi

# Prerequisite checks
require_cmd() {
  cmd_path="$1"
  label="$2"
  if [ ! -x "$cmd_path" ]; then
    echo "error: missing ${label} (${cmd_path})." >&2
    exit 1
  fi
}

warn_cmd() {
  cmd_path="$1"
  label="$2"
  if [ ! -x "$cmd_path" ]; then
    echo "warning: missing ${label} (${cmd_path})." >&2
    return 1
  fi
  return 0
}

ensure_pkg() {
  pkg_name="$1"
  bin_path="$2"
  label="$3"
  if [ -x "$bin_path" ]; then
    return 0
  fi
  if [ ! -x /usr/sbin/pkg ]; then
    echo "error: missing pkg (/usr/sbin/pkg); cannot install ${pkg_name}." >&2
    exit 1
  fi
  echo "Installing ${pkg_name} (${label})..."
  if ! /usr/sbin/pkg install -y "$pkg_name"; then
    echo "error: failed to install ${pkg_name}." >&2
    exit 1
  fi
  if [ ! -x "$bin_path" ]; then
    echo "error: ${pkg_name} installed but ${label} still missing (${bin_path})." >&2
    exit 1
  fi
}

# Defaults (override via CLI flags)
PREFIX="/usr/local"        # install prefix for bin/etc/rc.d
SRC_BIN=""                 # source binary path (auto-detect/build if empty)
CONFIG_PATH=""             # config path (defaults to /usr/local/etc/raidraccoon.json)
USER_NAME="raidraccoon"    # service user
GROUP_NAME="raidraccoon"   # service group
ENABLE_SERVICE=1           # sysrc raidraccoon_enable=YES
START_SERVICE=1            # service raidraccoon start/restart
INSTALL_RC=1               # install rc.d script
INSTALL_SUDOERS=1          # install sudoers entry
SET_PASSWORD=1             # set admin password for new config
PASSWORD_VALUE=""          # explicit password (otherwise generate)
REPO_OWNER="szymon-zasada"
REPO_NAME="raid-raccoon-deluxe"
RELEASE_TAG="latest"
ASSET_NAME=""
CLEANUP_BIN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --bin)
      SRC_BIN="$2"; shift 2 ;;
    --version)
      RELEASE_TAG="$2"; shift 2 ;;
    --asset)
      ASSET_NAME="$2"; shift 2 ;;
    --config)
      CONFIG_PATH="$2"; shift 2 ;;
    --user)
      USER_NAME="$2"; shift 2 ;;
    --group)
      GROUP_NAME="$2"; shift 2 ;;
    --prefix)
      PREFIX="$2"; shift 2 ;;
    --no-enable)
      ENABLE_SERVICE=0; shift ;;
    --no-start)
      START_SERVICE=0; shift ;;
    --no-rc)
      INSTALL_RC=0; shift ;;
    --no-sudoers)
      INSTALL_SUDOERS=0; shift ;;
    --password)
      PASSWORD_VALUE="$2"; SET_PASSWORD=1; shift 2 ;;
    --no-password)
      SET_PASSWORD=0; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "error: unknown option $1" >&2
      usage
      exit 1
      ;;
  esac
done

# Core tools
require_cmd /sbin/zfs "zfs"
require_cmd /sbin/zpool "zpool"
warn_cmd /sbin/geom "geom"
warn_cmd /sbin/sysctl "sysctl"
warn_cmd /usr/sbin/service "service"
warn_cmd /usr/sbin/sysrc "sysrc"

# Optional feature tools
warn_cmd /usr/local/bin/smbpasswd "smbpasswd (Samba users)"
warn_cmd /usr/local/bin/pdbedit "pdbedit (Samba users)"
warn_cmd /usr/local/bin/testparm "testparm (Samba shares)"
ensure_pkg rsync /usr/local/bin/rsync "rsync (Rsync jobs)"

# Derived paths
BINDIR="$PREFIX/bin"
ETCDIR="$PREFIX/etc"
RCDIR="$PREFIX/etc/rc.d"
if [ -z "$CONFIG_PATH" ]; then
  CONFIG_PATH="$ETCDIR/raidraccoon.json"
fi
BIN_PATH="$BINDIR/raidraccoon"

# Resolve script directory (for contrib/rc.d and repo-local binary)
SCRIPT_DIR=$(/usr/bin/dirname "$0")
SCRIPT_DIR=$(cd "$SCRIPT_DIR" && /bin/pwd)

download_to() {
  url="$1"
  dest="$2"
  if command -v /usr/bin/fetch >/dev/null 2>&1; then
    /usr/bin/fetch -q -o "$dest" "$url"
    return $?
  fi
  if command -v /usr/bin/curl >/dev/null 2>&1; then
    /usr/bin/curl -fsSL -o "$dest" "$url"
    return $?
  fi
  if command -v /usr/bin/wget >/dev/null 2>&1; then
    /usr/bin/wget -q -O "$dest" "$url"
    return $?
  fi
  echo "error: need fetch, curl, or wget to download releases." >&2
  return 1
}

detect_arch() {
  arch=$(/usr/bin/uname -m)
  case "$arch" in
    amd64|x86_64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *) echo "" ;;
  esac
}

download_release() {
  os="freebsd"
  arch=$(detect_arch)
  if [ -z "$arch" ]; then
    echo "error: unsupported architecture. Use --bin PATH." >&2
    exit 1
  fi
  if [ -z "$ASSET_NAME" ]; then
    ASSET_NAME="raidraccoon-${os}-${arch}"
  fi
  api_url="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/latest"
  if [ "$RELEASE_TAG" != "latest" ]; then
    case "$RELEASE_TAG" in
      v*) ;;
      [0-9]*)
        RELEASE_TAG="v${RELEASE_TAG}"
        ;;
    esac
    api_url="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/releases/tags/${RELEASE_TAG}"
  fi
  json_tmp=$(/usr/bin/mktemp -t raidraccoon.release)
  if ! download_to "$api_url" "$json_tmp"; then
    /bin/rm -f "$json_tmp"
    echo "error: failed to fetch release metadata." >&2
    exit 1
  fi
  asset_url=$(/usr/bin/awk -v asset="$ASSET_NAME" '
    /"name":/ {
      name=$0
      sub(/^.*"name":[[:space:]]*"/, "", name)
      sub(/".*$/, "", name)
    }
    /"browser_download_url":/ {
      url=$0
      sub(/^.*"browser_download_url":[[:space:]]*"/, "", url)
      sub(/".*$/, "", url)
      if (name == asset) {
        print url
        exit
      }
    }
  ' "$json_tmp")
  /bin/rm -f "$json_tmp"
  if [ -z "$asset_url" ]; then
    echo "error: release asset not found (${ASSET_NAME})." >&2
    echo "hint: use --asset NAME or --bin PATH." >&2
    exit 1
  fi
  dl_bin=$(/usr/bin/mktemp -t raidraccoon.bin)
  if ! download_to "$asset_url" "$dl_bin"; then
    /bin/rm -f "$dl_bin"
    echo "error: failed to download release binary." >&2
    exit 1
  fi
  SRC_BIN="$dl_bin"
  CLEANUP_BIN=1
}

if [ -z "$SRC_BIN" ]; then
  download_release
fi

# Ensure install directories exist
/bin/mkdir -p "$BINDIR" "$ETCDIR" "$RCDIR"

# Ensure sudoers.d exists if needed
if [ "${INSTALL_SUDOERS}" -eq 1 ]; then
  /bin/mkdir -p /usr/local/etc/sudoers.d
fi

# Create service user/group if missing
if ! /usr/sbin/pw group show "$GROUP_NAME" >/dev/null 2>&1; then
  /usr/sbin/pw groupadd "$GROUP_NAME"
fi

if ! /usr/sbin/pw user show "$USER_NAME" >/dev/null 2>&1; then
  /usr/sbin/pw useradd "$USER_NAME" -g "$GROUP_NAME" -m -s /usr/sbin/nologin
fi

# Install binary
/usr/bin/install -m 0555 "$SRC_BIN" "$BIN_PATH"

if [ "${CLEANUP_BIN}" -eq 1 ] && [ -f "$SRC_BIN" ]; then
  /bin/rm -f "$SRC_BIN"
elif [ "${SRC_BIN}" != "$SCRIPT_DIR/raidraccoon" ] && [ "${SRC_BIN}" != "./raidraccoon" ] && [ -f "$SRC_BIN" ]; then
  case "$SRC_BIN" in
    /tmp/raidraccoon.*|/var/tmp/raidraccoon.*)
      # Clean up temporary build output
      /bin/rm -f "$SRC_BIN" ;;
  esac
fi

# Install rc.d script (from repo or fallback embedded copy)
if [ "${INSTALL_RC}" -eq 1 ]; then
  RC_SOURCE="$SCRIPT_DIR/contrib/rc.d/raidraccoon"
  if [ -f "$RC_SOURCE" ]; then
    /usr/bin/install -m 0555 "$RC_SOURCE" "$RCDIR/raidraccoon"
  else
    RC_TMP=$(/usr/bin/mktemp -t raidraccoon.rc)
    /bin/cat > "$RC_TMP" <<'RC_EOF'
#!/bin/sh
# PROVIDE: raidraccoon
# REQUIRE: LOGIN
# KEYWORD: shutdown

. /etc/rc.subr

# Service definition
name="raidraccoon"
rcvar=raidraccoon_enable

load_rc_config $name

# Defaults (override via sysrc)
: ${raidraccoon_enable:=NO}                     # enable at boot
: ${raidraccoon_user:=raidraccoon}              # run as service user
: ${raidraccoon_command:=/usr/local/bin/raidraccoon}
: ${raidraccoon_config:=/usr/local/etc/raidraccoon.json}
: ${raidraccoon_flags:=""}                      # optional flags (e.g. --unsafe)

command="${raidraccoon_command}"
command_args="serve --config ${raidraccoon_config} ${raidraccoon_flags}"
pidfile="/var/run/${name}.pid"

start_cmd="${name}_start"
stop_cmd="${name}_stop"
status_cmd="${name}_status"

raidraccoon_start() {
  echo "Starting ${name}."
  # daemonize and drop privileges to the service user
  /usr/sbin/daemon -p ${pidfile} -u ${raidraccoon_user} ${command} ${command_args}
}

raidraccoon_stop() {
  echo "Stopping ${name}."
  if [ -f ${pidfile} ]; then
    kill $(cat ${pidfile})
  else
    # fallback if pidfile is missing
    pkill -u ${raidraccoon_user} -f "${command} serve"
  fi
}

raidraccoon_status() {
  if [ -f ${pidfile} ]; then
    echo "${name} running (pid $(cat ${pidfile}))"
    return 0
  fi
  echo "${name} not running"
  return 1
}

run_rc_command "$1"
RC_EOF
    /usr/bin/install -m 0555 "$RC_TMP" "$RCDIR/raidraccoon"
    /bin/rm -f "$RC_TMP"
  fi
fi

# Create config if missing
CONFIG_CREATED=0
if [ ! -f "$CONFIG_PATH" ]; then
  /bin/mkdir -p "$(/usr/bin/dirname "$CONFIG_PATH")"
  "$BIN_PATH" init --config "$CONFIG_PATH"
  CONFIG_CREATED=1
fi

# Secure config ownership/permissions
/usr/sbin/chown "$USER_NAME":"$GROUP_NAME" "$CONFIG_PATH"
/bin/chmod 0640 "$CONFIG_PATH"

# Install sudoers entry for required commands
if [ "${INSTALL_SUDOERS}" -eq 1 ]; then
  SUDOERS_TMP=$(/usr/bin/mktemp -t raidraccoon.sudoers)
  /bin/cat > "$SUDOERS_TMP" <<SUDO_EOF
# RaidRaccoon Deluxe sudoers (required for web UI actions)
Defaults:${USER_NAME} secure_path="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
# Allow only the system commands the UI needs
${USER_NAME} ALL=(ALL) NOPASSWD: /sbin/zfs, /sbin/zpool, /sbin/geom, /sbin/sysctl, /usr/sbin/service, /usr/local/bin/smbpasswd, /usr/local/bin/pdbedit, /usr/local/bin/testparm, /usr/local/bin/rsync, /usr/sbin/sysrc, /sbin/shutdown, /usr/bin/install
SUDO_EOF
  /usr/bin/install -m 0440 "$SUDOERS_TMP" /usr/local/etc/sudoers.d/raidraccoon
  /bin/rm -f "$SUDOERS_TMP"

  if [ ! -x /usr/local/bin/sudo ]; then
    echo "warning: /usr/local/bin/sudo not found; install sudo for web UI actions." >&2
  fi
fi

# Ensure audit log exists and is writable by the service user
AUDIT_LOG="/var/log/raidraccoon-audit.log"
if [ ! -f "$AUDIT_LOG" ]; then
  /usr/bin/touch "$AUDIT_LOG"
fi
/usr/sbin/chown "$USER_NAME":"$GROUP_NAME" "$AUDIT_LOG"
/bin/chmod 0640 "$AUDIT_LOG"

# Set admin password only for a newly created config
if [ "${CONFIG_CREATED}" -eq 1 ] && [ "${SET_PASSWORD}" -eq 1 ]; then
  if [ -n "$PASSWORD_VALUE" ]; then
    ADMIN_PASS="$PASSWORD_VALUE"
  else
    # Generate a 24-char hex password from urandom
    ADMIN_PASS=$(/bin/dd if=/dev/urandom bs=18 count=1 2>/dev/null | /usr/bin/hexdump -v -e '/1 "%02x"' | /usr/bin/cut -c1-24)
  fi
  printf "%s\n%s\n" "$ADMIN_PASS" "$ADMIN_PASS" | "$BIN_PATH" passwd --config "$CONFIG_PATH" >/dev/null
  echo "Admin password: $ADMIN_PASS"
  echo "Change it later in Settings -> Auth."
fi

# Update rc.conf defaults (user, command, config, enable)
if [ "${INSTALL_RC}" -eq 1 ]; then
  /usr/sbin/sysrc "raidraccoon_user=${USER_NAME}" >/dev/null
  /usr/sbin/sysrc "raidraccoon_command=${BIN_PATH}" >/dev/null
  /usr/sbin/sysrc "raidraccoon_config=${CONFIG_PATH}" >/dev/null
  if [ "${ENABLE_SERVICE}" -eq 1 ]; then
    /usr/sbin/sysrc raidraccoon_enable=YES >/dev/null
  else
    /usr/sbin/sysrc raidraccoon_enable=NO >/dev/null
  fi
fi

# Start or restart service.
# Use one* actions so startup doesn't depend on raidraccoon_enable being YES.
if [ "${INSTALL_RC}" -eq 1 ] && [ "${START_SERVICE}" -eq 1 ]; then
  if /usr/sbin/service raidraccoon onestatus >/dev/null 2>&1; then
    OUT=$(/usr/sbin/service raidraccoon onerestart 2>&1) || {
      echo "error: failed to restart raidraccoon service" >&2
      echo "$OUT" >&2
      exit 1
    }
  else
    OUT=$(/usr/sbin/service raidraccoon onestart 2>&1) || {
      echo "error: failed to start raidraccoon service" >&2
      echo "$OUT" >&2
      exit 1
    }
  fi

  # Give daemon a moment to create the pidfile.
  /bin/sleep 1
  if ! /usr/sbin/service raidraccoon onestatus >/dev/null 2>&1; then
    echo "error: raidraccoon did not start (service status follows)" >&2
    /usr/sbin/service raidraccoon status 2>&1 || true
    exit 1
  fi
fi

echo "Installed raidraccoon to ${BIN_PATH}"
echo "Config: ${CONFIG_PATH}"
if [ "${INSTALL_RC}" -eq 1 ]; then
  echo "Service: ${RCDIR}/raidraccoon"
fi
if [ "${ENABLE_SERVICE}" -eq 1 ]; then
  echo "Autostart: enabled (toggle in the web UI under Settings -> System)"
else
  echo "Autostart: disabled (toggle in the web UI under Settings -> System)"
fi

if [ "${INSTALL_RC}" -eq 1 ] && [ "${START_SERVICE}" -eq 1 ]; then
  echo "Service status: running"
fi
