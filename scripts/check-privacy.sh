#!/usr/bin/env bash
# Privacy check: fail loudly if machine-specific secrets leaked into tracked
# repository files. Run by the git pre-commit hook (install once per clone
# with `bash scripts/install-hooks.sh`), and on demand:
# `bash scripts/check-privacy.sh`
#
# Scans (tracked files only — `git grep`, so gitignored scratch dirs and the
# local config are out of scope by construction):
#   1. exact real values from the gitignored qqflow-server.json — qq, key and
#      db_path (skipped when the file is absent, e.g. CI)
#   2. the local username and machine-specific paths (D:\AppData\Tencent
#      Files, C:\Users\*)
#
# Exits non-zero (and lists every hit) when anything leaks, which aborts the
# commit; findings go to stderr, which git shows verbatim. Values are NEVER
# echoed — only labels and the files they were found in.
#
# Deliberately NOT scanned: a generic 64-hex shape (weflow-server uses one).
# QQ keys are 16 printable-ASCII bytes, NOT hex (see keystore::validate_key),
# so a hex-shape scan could never catch a leaked key — the exact-value layer
# above is the only check that can. A 64-hex scan would be noise here.

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONFIG="qqflow-server.json"

# Paths excluded from every scan:
#   *.lock        — Cargo.lock carries ~200 crate checksums in 64-hex shape
#   this script   — it contains the pattern strings below
PATHSPEC=(':!*.lock' ':!scripts/check-privacy.sh')

hits=0

# report <label> <fixed-string-pattern>
# Findings go to STDERR so the git pre-commit hook surfaces them verbatim
# alongside the aborted commit. The pattern itself is never printed.
report() {
  local label="$1"
  local pattern="$2"
  local found
  # `-e` guards patterns that begin with `-`; pathspecs go after `--`.
  found=$(git grep -lF -e "$pattern" -- "${PATHSPEC[@]}" 2>/dev/null)
  if [ -n "$found" ]; then
    echo "[隐私检查] 检测到 $label:" >&2
    echo "$found" | sed 's/^/    /' >&2
    hits=$((hits + 1))
  fi
}

# JSON field reader for the config: tries the interpreters that may exist
# on a dev machine (python / python3 / the Windows `py` launcher). Prints one
# `label<TAB>value` line per present field. Returns 1 when no interpreter
# works — callers must NOT treat that as "nothing to check" while the config
# exists (see the loud error below).
read_config_values() {
  local py
  for py in python python3 "py -3"; do
    $py - "$CONFIG" <<'PY' 2>/dev/null && return 0
import json, sys
# Force LF: on Windows Python defaults to \r\n, and a trailing \r would make
# every `git grep -F` search for "<secret>\r" and silently match nothing.
try:
    sys.stdout.reconfigure(newline="\n")
except AttributeError:  # Python < 3.7
    pass
with open(sys.argv[1], encoding="utf-8") as fh:
    cfg = json.load(fh)
for field in ("qq", "key", "db_path"):
    value = cfg.get(field)
    if isinstance(value, str) and value:
        print("%s\t%s" % (field, value))
PY
  done
  return 1
}

# ---- 1. Real values from the gitignored local config -----------------------
if [ -f "$CONFIG" ]; then
  if ! CONFIG_VALUES="$(read_config_values)" || [ -z "$CONFIG_VALUES" ]; then
    # A machine holding qqflow-server.json must actually run the most
    # sensitive checks. Failing silently (no python, the Windows Store stub,
    # broken JSON) would print 通过 while real qq/key leaks go unchecked —
    # block until the environment can read the config.
    echo "[隐私检查] 错误：存在 $CONFIG 但无法读取其中的 qq/key（python/python3/py 不可用，或 JSON 损坏？）。最敏感的泄露检查无法运行，拒绝放行；请安装 Python 后重试。" >&2
    exit 1
  fi
  while IFS=$'\t' read -r label value; do
    # Belt and braces against CRLF from any interpreter: a trailing \r would
    # turn every search into "<secret>\r" and match nothing (silent pass).
    value="${value%$'\r'}"
    [ -n "${value:-}" ] || continue
    case "$label" in
      qq)    report "真实 QQ 号（$CONFIG）" "$value" ;;
      key)   report "真实数据库密钥（$CONFIG 的 key）" "$value" ;;
      *)     report "真实数据库路径（$CONFIG 的 $label）" "$value" ;;
    esac
  done <<< "$CONFIG_VALUES"
fi

# ---- 2. Machine-specific paths / usernames --------------------------------
# MSYS bash's `whoami` prints the bare username (`alice`); Windows'
# `whoami.exe` prints `HOSTNAME\user` (`laptop-abc123\alice`), and the UPN
# form is `user@domain`. Normalize to the bare name: `C:\Users\$USER_NAME`
# needs it, and the bare name is a substring of every other form.
#
# The scan is case-SENSITIVE (`grep -F`, no -i) on purpose: a short lowercase
# username can appear as a case-different substring inside CamelCase Windows
# API names (e.g. ReadDirectoryChangesW), which is not a leak.
USER_NAME="$(whoami 2>/dev/null | sed 's/.*[\\/@]//' || true)"
if [ -n "$USER_NAME" ]; then
  report "本机用户名" "$USER_NAME"
  report "用户目录绝对路径 C:\\Users\\<用户名>" "C:\\Users\\$USER_NAME"
fi
# Real QQ NT data roots carry the "Tencent Files" marker; scanning the bare
# prefix would false-positive on docs that mention the pattern generically.
report "D 盘腾讯数据路径" 'D:\AppData\Tencent Files'
report "D 盘腾讯数据路径（正斜杠变体）" 'D:/AppData/Tencent Files'

if [ "$hits" -gt 0 ]; then
  echo "[隐私检查] 发现 $hits 类敏感信息泄露，请清理后再继续。" >&2
  echo "  确认为误报时，请人工复核后用 git commit --no-verify 跳过（谨慎）。" >&2
  exit 1
fi
echo "[隐私检查] 通过：未发现本机特定信息（QQ 号 / 密钥 / 路径 / 用户名）。"
exit 0
