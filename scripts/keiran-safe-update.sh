#!/usr/bin/env bash
# Safe `hermes update` wrapper for the keiranhaax/hermes-agent VPS fork.
#
# Why this exists: tools.lazy_deps.active_features() infers "active" lazy backends
# from packages present in the CURRENT venv. A managed-Python cutover during
# `hermes update` can replace that venv before the built-in refresh pass runs, so
# refresh_active_features() sees an empty list and Telegram (plus every other lazy
# backend) silently disappears. This wrapper captures the feature NAMES first and
# restores them against the post-update interpreter.
#
# Modes:
#   (default)              snapshot -> hermes update --backup --yes -> restore -> restart -> verify
#   --check                validate preconditions only, then exit
#   --dry-run              preconditions + snapshot + report the restore plan; no update, no restart
#   --restore-from FILE    skip the update; restore from an existing snapshot
#                          (combine with --dry-run to report without installing)
#   --keep-snapshot        never delete the snapshot, even on success
#
# Env overrides (used by the tests in the migration plan):
#   HERMES_SAFE_UPDATE_PYTHON   interpreter to introspect (default: <repo>/venv/bin/python)
#   HERMES_SAFE_UPDATE_HERMES   hermes entrypoint (default: PATH, then <repo>/venv/bin/hermes)
#   HERMES_SAFE_UPDATE_TIMEOUT  gateway readiness timeout in seconds (default: 240)
#
# Exit codes: 0 ok · 1 runtime failure (snapshot kept) · 2 precondition refusal · 64 usage.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERMES_HOME="${HERMES_HOME:-$HOME/.hermes}"
EXPECTED_ORIGIN="keiranhaax/hermes-agent"
EXPECTED_UPSTREAM="NousResearch/hermes-agent"
SNAP_DIR="$HERMES_HOME/backups/lazy-features"
STATE_FILE="$HERMES_HOME/gateway_state.json"
READY_TIMEOUT="${HERMES_SAFE_UPDATE_TIMEOUT:-240}"
UNITS=(hermes-gateway.service hermes-serve.service)

MODE="update"
DRY_RUN=0
KEEP_SNAPSHOT=0
RESTORE_FROM=""
PYTHON=""
HERMES_BIN=""
SNAPSHOT=""

say()  { printf '→ %s\n' "$*"; }
ok()   { printf '  ✓ %s\n' "$*"; }
warn() { printf '  ⚠ %s\n' "$*" >&2; }
die()  { printf '✗ %s\n' "$*" >&2; exit 2; }
fail() { printf '✗ %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    --check)         MODE="check" ;;
    --dry-run)       DRY_RUN=1 ;;
    --restore-from)  RESTORE_FROM="${2:?--restore-from requires a path}"; shift ;;
    --keep-snapshot) KEEP_SNAPSHOT=1 ;;
    -h|--help)       usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 64 ;;
  esac
  shift
done

normalize_slug() {
  local url="${1:-}"
  url="${url%/}"; url="${url%.git}"
  url="${url#https://github.com/}"; url="${url#git@github.com:}"; url="${url#ssh://git@github.com/}"
  printf '%s\n' "$url"
}

unit_start_epoch() {
  local stamp
  stamp="$(systemctl --user show "$1" -p ActiveEnterTimestamp --value 2>/dev/null || true)"
  if [ -n "$stamp" ] && date -d "$stamp" +%s >/dev/null 2>&1; then
    date -d "$stamp" +%s
  else
    echo 0
  fi
}

read -r -d '' PY_ALLOW <<'PY' || true
import sys
sys.path.insert(0, ".")
from tools.lazy_deps import _allow_lazy_installs
sys.exit(0 if _allow_lazy_installs() else 1)
PY

read -r -d '' PY_SNAPSHOT <<'PY' || true
import json, sys
sys.path.insert(0, ".")
from tools.lazy_deps import active_features
feats = sorted(active_features())
with open(sys.argv[1], "w", encoding="utf-8") as fh:
    json.dump(feats, fh, indent=1)
print(f"  captured {len(feats)} active lazy feature(s)")
for feat in feats:
    print(f"    {feat}")
PY

read -r -d '' PY_RESTORE <<'PY' || true
import json, sys
sys.path.insert(0, ".")
from tools.lazy_deps import LAZY_DEPS, FeatureUnavailable, ensure, feature_missing

path, dry = sys.argv[1], sys.argv[2] == "1"
with open(path, encoding="utf-8") as fh:
    wanted = json.load(fh)
if not isinstance(wanted, list) or not all(isinstance(item, str) for item in wanted):
    print("  snapshot is not a list of feature names", file=sys.stderr)
    sys.exit(2)

unknown = [item for item in wanted if item not in LAZY_DEPS]
if unknown:
    print(f"  snapshot contains names outside the LAZY_DEPS allowlist: {', '.join(unknown)}", file=sys.stderr)
    sys.exit(2)

failed = []
for feat in [item for item in wanted if item in LAZY_DEPS]:
    missing = feature_missing(feat)
    if not missing:
        print(f"  ✓ {feat} present")
        continue
    if dry:
        print(f"  · {feat} would be reinstalled ({', '.join(missing)})")
        continue
    try:
        ensure(feat, prompt=False)
        print(f"  ↑ {feat} restored")
    except FeatureUnavailable as exc:
        failed.append(feat); print(f"  ✗ {feat}: {exc.reason}")
    except Exception as exc:  # never let one backend abort the rest
        failed.append(feat); print(f"  ✗ {feat}: {exc}")

if failed:
    print(f"  {len(failed)} feature(s) could not be restored: {', '.join(failed)}", file=sys.stderr)
    sys.exit(1)
PY

read -r -d '' PY_LIVE <<'PY' || true
import json, sys
from datetime import datetime
state_path, since = sys.argv[1], float(sys.argv[2])
try:
    data = json.loads(open(state_path, encoding="utf-8").read())
except Exception:
    print("")
    sys.exit(0)
live = []
for name, entry in (data.get("platforms") or {}).items():
    try:
        fresh = datetime.fromisoformat(entry.get("updated_at") or "").timestamp() >= since - 2
    except ValueError:
        fresh = False
    if entry.get("state") == "connected" and fresh:
        live.append(name)
print(",".join(sorted(live)))
PY

read -r -d '' PY_READY <<'PY' || true
import json, sys, time
from datetime import datetime
state_path, want_csv, timeout, since = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
want = [name for name in want_csv.split(",") if name]
deadline = time.time() + timeout
detail = "no reading yet"
while time.time() < deadline:
    try:
        data = json.loads(open(state_path, encoding="utf-8").read())
    except Exception as exc:
        detail = f"unreadable state file: {exc}"
        time.sleep(3); continue
    platforms = data.get("platforms") or {}
    pending = []
    for name in want:
        entry = platforms.get(name) or {}
        try:
            fresh = datetime.fromisoformat(entry.get("updated_at") or "").timestamp() >= since - 2
        except ValueError:
            fresh = False
        if entry.get("state") != "connected" or not fresh:
            pending.append(f"{name}={entry.get('state') or 'absent'}{'' if fresh else '/stale'}")
    if data.get("gateway_state") == "running" and not pending:
        print(f"  ✓ gateway ready: {', '.join(want) or 'no platforms expected'}")
        sys.exit(0)
    detail = f"gateway_state={data.get('gateway_state')} pending={', '.join(pending) or 'none'}"
    time.sleep(3)
print(f"  ✗ gateway not ready within {timeout:.0f}s — {detail}", file=sys.stderr)
sys.exit(1)
PY

check_preconditions() {
  if grep -qE 'hermes-(gateway|serve)\.service' /proc/self/cgroup 2>/dev/null; then
    die "running inside a Hermes unit cgroup — restarting would kill this shell; run from SSH"
  fi
  cd "$REPO_ROOT"
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "$REPO_ROOT is not a git work tree"

  local branch origin_slug upstream_slug
  branch="$(git branch --show-current)"
  [ "$branch" = "main" ] || die "branch is '$branch', expected main"

  origin_slug="$(normalize_slug "$(git remote get-url origin 2>/dev/null || true)")"
  [ "$origin_slug" = "$EXPECTED_ORIGIN" ] || die "origin is '${origin_slug:-<none>}', expected $EXPECTED_ORIGIN"

  upstream_slug="$(normalize_slug "$(git remote get-url upstream 2>/dev/null || true)")"
  [ "$upstream_slug" = "$EXPECTED_UPSTREAM" ] || die \
    "upstream remote is '${upstream_slug:-<none>}', expected $EXPECTED_UPSTREAM — hermes update would prompt on stdin. Fix: git remote add upstream https://github.com/NousResearch/hermes-agent.git"

  [ -z "$(git status --porcelain)" ] || die "working tree is dirty — commit or stash first"

  PYTHON="${HERMES_SAFE_UPDATE_PYTHON:-$REPO_ROOT/venv/bin/python}"
  [ -x "$PYTHON" ] || die "no usable interpreter at $PYTHON"

  HERMES_BIN="${HERMES_SAFE_UPDATE_HERMES:-$(command -v hermes || true)}"
  [ -n "$HERMES_BIN" ] || HERMES_BIN="$REPO_ROOT/venv/bin/hermes"
  [ -x "$HERMES_BIN" ] || die "no hermes entrypoint at $HERMES_BIN"

  "$PYTHON" -c "$PY_ALLOW" \
    || die "security.allow_lazy_installs is off — restoring lazy backends would fail"

  ok "preconditions satisfied (branch=main origin=$EXPECTED_ORIGIN upstream=$EXPECTED_UPSTREAM tree=clean lazy_installs=on)"
}

take_snapshot() {
  install -d -m 700 "$SNAP_DIR"
  SNAPSHOT="$SNAP_DIR/$(date -u +%Y%m%dT%H%M%SZ).json"
  ( umask 077; : > "$SNAPSHOT" )
  chmod 600 "$SNAPSHOT"
  say "snapshotting active lazy backends -> $SNAPSHOT"
  "$PYTHON" -c "$PY_SNAPSHOT" "$SNAPSHOT" || fail "could not snapshot active features"
  [ "$(stat -c %a "$SNAPSHOT")" = "600" ] || fail "snapshot permissions are not 0600"
}

restart_units() {
  say "restarting Hermes units"
  "$HERMES_BIN" gateway restart || warn "hermes gateway restart returned non-zero"
  if systemctl --user list-unit-files hermes-serve.service >/dev/null 2>&1; then
    systemctl --user restart hermes-serve.service || warn "hermes-serve restart returned non-zero"
  fi
}

main() {
  check_preconditions
  if [ "$MODE" = "check" ]; then
    ok "check complete"
    exit 0
  fi

  # Restore-only path (exercises the real restore code without an update).
  if [ -n "$RESTORE_FROM" ]; then
    [ -r "$RESTORE_FROM" ] || die "snapshot not readable: $RESTORE_FROM"
    say "restoring lazy backends from $RESTORE_FROM (dry_run=$DRY_RUN)"
    "$PYTHON" -c "$PY_RESTORE" "$RESTORE_FROM" "$DRY_RUN" || fail "restore reported failures"
    ok "restore pass complete"
    exit 0
  fi

  take_snapshot

  local gw_start live
  gw_start="$(unit_start_epoch hermes-gateway.service)"
  live="$("$PYTHON" -c "$PY_LIVE" "$STATE_FILE" "$gw_start")"
  say "live platforms before update: ${live:-<none>}"

  if [ "$DRY_RUN" = "1" ]; then
    say "dry run — reporting the restore plan; no update, no restart"
    "$PYTHON" -c "$PY_RESTORE" "$SNAPSHOT" 1 || fail "dry-run restore plan reported failures"
    if [ "$KEEP_SNAPSHOT" = "1" ]; then
      ok "snapshot kept at $SNAPSHOT"
    else
      rm -f "$SNAPSHOT"; ok "dry-run snapshot removed"
    fi
    exit 0
  fi

  say "running hermes update --backup --yes"
  set +e
  "$HERMES_BIN" update --backup --yes
  local update_rc=$?
  set -e
  [ "$update_rc" -eq 0 ] || warn "hermes update exited $update_rc — continuing to restore and restart, then failing loudly"

  PYTHON="${HERMES_SAFE_UPDATE_PYTHON:-$REPO_ROOT/venv/bin/python}"
  [ -x "$PYTHON" ] || fail "post-update interpreter missing at $PYTHON — snapshot kept at $SNAPSHOT"

  say "restoring lazy backends from $SNAPSHOT"
  local restore_rc=0
  "$PYTHON" -c "$PY_RESTORE" "$SNAPSHOT" 0 || restore_rc=$?

  say "checking dependency consistency"
  local pipcheck_rc=0
  if command -v uv >/dev/null 2>&1; then
    uv pip check --python "$PYTHON" || pipcheck_rc=$?
  else
    warn "uv not found — skipping uv pip check"
  fi

  local since ready_rc=0 unit_rc=0
  since="$(date +%s)"          # capture BEFORE the restart; restart_units logs to stdout
  restart_units
  systemctl --user is-active --quiet hermes-gateway.service hermes-serve.service || unit_rc=$?
  "$PYTHON" -c "$PY_READY" "$STATE_FILE" "$live" "$READY_TIMEOUT" "$since" || ready_rc=$?

  if [ "$update_rc" -ne 0 ] || [ "$restore_rc" -ne 0 ] || [ "$pipcheck_rc" -ne 0 ] || [ "$unit_rc" -ne 0 ] || [ "$ready_rc" -ne 0 ]; then
    printf '✗ safe update finished with failures (update=%s restore=%s pip_check=%s units=%s ready=%s)\n' \
      "$update_rc" "$restore_rc" "$pipcheck_rc" "$unit_rc" "$ready_rc" >&2
    printf '  snapshot kept for retry: %s\n' "$SNAPSHOT" >&2
    printf '  retry restore with: %s --restore-from %s\n' "${BASH_SOURCE[0]}" "$SNAPSHOT" >&2
    exit 1
  fi

  if [ "$KEEP_SNAPSHOT" = "1" ]; then
    ok "snapshot kept at $SNAPSHOT"
  else
    rm -f "$SNAPSHOT"; ok "snapshot removed after successful verification"
  fi
  ok "safe update complete"
}

main "$@"
