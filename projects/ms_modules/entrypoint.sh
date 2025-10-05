#!/usr/bin/env bash
set -euo pipefail



DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"
DOCKER_SOCK="${DOCKER_HOST#unix://}"

# --- Host docker socket: align docker group so 'app' can use docker CLI ---
if [ -S "$DOCKER_SOCK" ]; then
  echo "[entrypoint] Using host docker socket at $DOCKER_SOCK"
  SOCK_GID="$(stat -c '%g' "$DOCKER_SOCK" 2>/dev/null || echo 999)"
  if ! getent group docker >/dev/null 2>&1; then
    groupadd -g "$SOCK_GID" docker || true
  else
    CURRENT_GID="$(getent group docker | cut -d: -f3 || true)"
    if [ "${CURRENT_GID:-}" != "$SOCK_GID" ]; then
      groupmod -g "$SOCK_GID" docker || true
    fi
  fi
  usermod -aG docker app || true
else
  echo "[entrypoint] WARNING: $DOCKER_SOCK not present; docker commands will fail."
fi

# --- /vbtmp mount: make sure 'app' can write (can't change host ownership) ---
if [ -d /vbtmp ]; then
  # Try best-effort to grant wide write perms for the mount point inside container
  chmod 1777 /vbtmp || true
  echo "[entrypoint] /vbtmp present; perms set to 1777 (sticky tmp)."
fi

echo "[entrypoint] Launching ms_modules as 'app'..."
exec gosu app "$@"