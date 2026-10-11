#!/usr/bin/env bash
# 10.2 §5 优雅关闭集成验证（POSIX/Git Bash 下执行；§7：Windows 信号语义差异）
# 场景：起服务 → 在途半开请求（模拟流式长连接）→ kill -TERM →
#       观察是否等待在途完成 → 完成请求 → 进程退出码 0 → 新连接被拒
#
# 【2026-10-10 Windows 实测记录】在 Windows 原生 Go 进程上：
#   - kill -TERM 经 msys 实现为 TerminateProcess 硬终止（不可捕获）：
#     进程立即死亡、无优雅关闭日志、在途连接被 reset、bash 报 exit 143；
#   - kill -INT 被 msys 接受但未投递到原生进程（无共享控制台）；
#   - 优雅关闭触发在 Windows 下仅剩真实 Ctrl+C（SIGINT 控制台事件）。
# 优雅关闭的语义（在途等待/超时截断/逆序关闭）由 cmd/g-aigate 单测覆盖；
# 本脚本的 kill -TERM 场景须在 POSIX 环境（Linux/WSL，B2 末 store 就绪后）复跑。
set -u
cd "$(dirname "$0")/../.."

export GAIGATE_SECRET_MASTER_KEY=MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=

./bin/g-aigate.exe > bin/sig.log 2>&1 &
pid=$!
sleep 1

echo "== healthz =="
curl -s -i http://localhost:8080/healthz | head -3

echo "== 开一个在途半开请求（模拟流式长连接，不发完整请求头）=="
exec 3<>/dev/tcp/127.0.0.1/8080
printf 'GET /healthz HTTP/1.1\r\nHost: localhost\r\n' >&3

echo "== kill -TERM $pid =="
kill -TERM "$pid"
sleep 2

if kill -0 "$pid" 2>/dev/null; then
  echo "进程仍在（等待在途请求，Shutdown 生效）"
else
  echo "进程已退出（未等待在途请求——SIGTERM 在 Windows 下为硬终止）"
fi

echo "== 补完请求头并读取响应 =="
printf '\r\n' >&3
timeout 3 cat <&3 || true
exec 3<&- 3>&-

sleep 1
if kill -0 "$pid" 2>/dev/null; then
  echo "进程仍在运行"
else
  echo "进程已退出"
fi

wait "$pid"
echo "exit_code=$?"

echo "== 新连接行为（Shutdown 期间/之后应拒绝）=="
curl -s --max-time 2 -o /dev/null -w "%{http_code}\n" http://localhost:8080/healthz || echo "连接被拒（预期）"

echo "== 进程日志 =="
cat bin/sig.log
