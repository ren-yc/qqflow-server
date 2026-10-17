# qqflow-server quickstart

本文件收任务导向的接入配方。当前先收「重启后恢复账号」；其余配方（接 ChatLab、增量同步、
批量导出、MCP）随后补齐。

## 重启后恢复账号

密钥只存进程内存（见 `docs/architecture.md` 的「密钥不落盘」），所以**服务重启后账号回到
未注册**：`/health` 的 `account` 是 `unregistered`，业务端点返回 503。恢复动作只有一步——
用**与重启前相同的密钥与 `db_path`** 重新调 `POST /api/v1/accounts`。

### 1. 准备 token

token 的来源链是 `QQFLOW_TOKEN`（非空）→ 本机凭据库（与 `qqflow-server token` 同一份）。
同机可以直接从凭据库取；跨机把 `QQFLOW_TOKEN` 设成服务端实际使用的那个值。

### 2. 准备恢复配置

把密钥放在**你自备的本地安全位置**（不放进仓库、不写进脚本）。示例脚本读一个 JSON：

```json
{
  "base_url": "http://127.0.0.1:5032",
  "token": "<API token>",
  "qq": "<QQ 号>",
  "db_path": "C:\\Users\\<用户名>\\Documents\\Tencent Files\\<QQ 号>\\nt_qq\\nt_db\\nt_msg.db",
  "key": "<16 字节 ASCII 库密钥>"
}
```

本仓只有**一把库密钥**（没有 weflow 的独立图片键）；密钥格式正确但内容过期时，后台解密失败，
账号会进入 `error`，错误字段与日志会点名「疑似密钥已轮换、参数过期或密钥缺失」。

### 3. 注册并确认

```powershell
powershell -File scripts/examples/register-accounts.ps1 -ConfigPath <你的配置.json>
```

```bash
bash scripts/examples/register-accounts.sh <你的配置.json>
```

脚本只读配置、不写盘、不回显 token 或密钥；注册后轮询账号明细直到 `state == "ready"`：

```powershell
# 需要 token；同机未设 QQFLOW_TOKEN 时 CLI 会回落到凭据库
qqflow-server accounts
```
