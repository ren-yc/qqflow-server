# 使用示例

本页是任务导向的操作指南（从 [API 参考](../qqflow-server-api.md) 拆出）；端点、字段与错误形状见该参考。

---

提供 cURL 与 Python 两套可直接复制的调用示例，覆盖从取 token、注册账号到查询、注销的主干流程。

### cURL

```bash
# 取 token：它存在系统凭据库里、不落文件，只能用子命令打印。
# bash / Git-Bash：
TOKEN=$(qqflow-server token)
# PowerShell（`VAR=$(…)` 是 bash 语法，PowerShell 里会当成命令名报错、$TOKEN 变空）：
#   $TOKEN = qqflow-server token
# 注册账号（客户端驱动启动；密钥仅内存保存）。鉴权走 Header，别把 token 放进 body。
curl -X POST http://127.0.0.1:5032/api/v1/accounts \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"qq\": \"1234567890\", \"key\": \"<16字节密钥>\", \"db_path\": \"C:\\\\Users\\\\<用户名>\\\\Documents\\\\Tencent Files\"}"
# 账号明细（需鉴权；/health 只给标量 account 阶段）
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:5032/api/v1/accounts
# 注销账号（恢复未注册状态；加 purge_media=1 才删导出媒体）
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:5032/api/v1/accounts/1234567890?purge_media=1"
# GET 带 Token Header
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:5032/api/v1/messages?talker=10001&limit=20"
# 读端点只有 GET：参数走查询串（POST 会得 405，body 里的 token 也不是鉴权通道）
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:5032/api/v1/messages?talker=10001&limit=50"
# SSE
curl -N "http://127.0.0.1:5032/api/v1/push/messages?access_token=$TOKEN"
```

### Python

```python
import requests

BASE_URL = "http://127.0.0.1:5032"
headers = {"Authorization": "Bearer YOUR_TOKEN", "Content-Type": "application/json"}

# 读端点只接受 GET（POST 得 405）
messages = requests.get(
    f"{BASE_URL}/api/v1/messages",
    params={"talker": "10001", "limit": 50},
    headers=headers,
).json()

sessions = requests.get(f"{BASE_URL}/api/v1/sessions", params={"limit": 20}, headers=headers).json()
```

