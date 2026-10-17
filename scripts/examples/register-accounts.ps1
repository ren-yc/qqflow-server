<#
.SYNOPSIS
  从调用方自备的本地配置注册一个账号（重启后恢复用）。

.DESCRIPTION
  只读配置文件、不写盘、不回显 token 或密钥。配置形如：

    {
      "base_url": "http://127.0.0.1:5032",
      "token": "<API token>",
      "qq": "<QQ 号>",
      "db_path": "C:\\...\\nt_qq\\nt_db\\nt_msg.db",
      "key": "<16 字节 ASCII 库密钥>"
    }

  token 放在请求头，不进命令行；密钥只从配置读，不代存。
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath
)

$ErrorActionPreference = 'Stop'
if (-not (Test-Path -LiteralPath $ConfigPath)) {
    throw "配置不存在: $ConfigPath"
}
$cfg = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
foreach ($required in 'base_url', 'token', 'qq', 'key') {
    if (-not $cfg.$required) { throw "配置缺少 $required" }
}

$body = [ordered]@{ qq = $cfg.qq; key = $cfg.key }
if ($cfg.db_path) { $body.db_path = $cfg.db_path }

$json = $body | ConvertTo-Json -Depth 4 -Compress
$headers = @{ Authorization = "Bearer $($cfg.token)" }
$uri = '{0}/api/v1/accounts' -f $cfg.base_url.TrimEnd('/')
$resp = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -ContentType 'application/json' -Body $json
"state=$($resp.state) status=$($resp.status)"
