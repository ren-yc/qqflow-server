#!/usr/bin/env bash
# Privacy check: fail loudly if machine-specific secrets leaked into source
# code or docs. Run automatically by the project PostToolUse hook after every
# file edit, and on demand: `bash scripts/check-privacy.sh`
#
# Scans for:
#   - the real QQ number and SQLCipher key from the gitignored
#     qqflow-server.json (skipped when the file is absent, e.g. CI)
#   - the local username and machine-specific paths (D:\AppData, C:\Users\*)
# Exits 1 (and lists every hit) when anything leaks; the hook surfaces that
# output so the edit can be fixed before committing.

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONFIG="qqflow-server.json"
INCLUDES=(--include='*.rs' --include='*.md' --include='*.toml' --include='*.json' --include='*.yaml' --include='*.yml')
# check-privacy.sh itself contains the pattern strings below — exclude it
# from its own scan.
EXCLUDES=(--exclude-dir=target --exclude-dir=.git --exclude-dir=.claude --exclude="$CONFIG" --exclude='*.lock' --exclude=check-privacy.sh)

hits=0

# report <label> <fixed-string-pattern>
# Findings go to STDERR: the PostToolUse hook shows stderr to Claude on
# exit 2, so the leak lands in the model's context for immediate fixing.
report() {
  local label="$1"
  local pattern="$2"
  local found
  found=$(grep -rln -F "${INCLUDES[@]}" "${EXCLUDES[@]}" -e "$pattern" . 2>/dev/null)
  if [ -n "$found" ]; then
    echo "[隐私检查] 检测到 $label:" >&2
    echo "$found" | sed 's/^/    /' >&2
    hits=$((hits + 1))
  fi
}

# JSON field reader for the config: tries the interpreters that may exist
# on a dev machine (python / python3 / the Windows `py` launcher). Returns 1
# when none of them works — callers must NOT treat that as "nothing to
# check" while the config exists (see the loud error below).
read_json_field() {
  local field="$1" py v
  for py in python python3 "py -3"; do
    v=$($py -c "import json;print(json.load(open('qqflow-server.json')).get('$field',''))" 2>/dev/null) \
      || continue
    [ -n "$v" ] && { printf '%s' "$v"; return 0; }
  done
  return 1
}

# 1. Real machine-specific values from the gitignored local config.
if [ -f "$CONFIG" ]; then
  QQ=$(read_json_field qq) || true
  KEY=$(read_json_field key) || true
  if [ -z "$QQ" ] && [ -z "$KEY" ]; then
    # A machine that holds qqflow-server.json must actually run the two most
    # sensitive checks. Failing silently (no python, the Windows Store stub,
    # broken JSON) would print 通过 while real qq/key leaks go unchecked —
    # block until the environment can read the config.
    echo "[隐私检查] 错误：存在 qqflow-server.json 但无法读取 qq/key（python/python3/py 不可用？）。最敏感的泄露检查无法运行，拒绝放行；请安装 Python 后重试。" >&2
    exit 2
  fi
  [ -n "$QQ" ] && report "真实 QQ 号 $QQ" "$QQ"
  [ -n "$KEY" ] && report "真实数据库密钥（qqflow-server.json 中的 key）" "$KEY"
fi

# 2. Machine-specific paths / usernames.
# MSYS bash's `whoami` prints the bare username (`ryc`); Windows'
# `whoami.exe` prints `HOSTNAME\user` (`laptop-ku4u72es\ryc`), and the UPN
# form is `user@domain`. Normalize to the bare name: `C:\Users\$USER_NAME`
# needs it, and the bare name is a substring of every other form.
USER_NAME="$(whoami 2>/dev/null | sed 's/.*[\\/@]//' || true)"
[ -n "$USER_NAME" ] && report "本机用户名 $USER_NAME" "$USER_NAME"
# Real QQ NT data roots carry the "Tencent Files" marker; scanning the bare
# prefix would false-positive on docs that mention the pattern generically.
report "D 盘腾讯数据路径" 'D:\AppData\Tencent Files'
report "D 盘腾讯数据路径（正斜杠变体）" 'D:/AppData/Tencent Files'
[ -n "$USER_NAME" ] && report "用户目录绝对路径 C:\Users\$USER_NAME" "C:\\Users\\$USER_NAME"

if [ "$hits" -gt 0 ]; then
  echo "[隐私检查] 发现 $hits 类敏感信息泄露，请清理后再继续。" >&2
  exit 2  # exit 2: PostToolUse shows stderr to Claude; Stop blocks the stop
fi
echo "[隐私检查] 通过：未发现本机特定信息（QQ 号 / 密钥 / 路径 / 用户名）。"
exit 0
