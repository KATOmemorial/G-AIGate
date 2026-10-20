#!/usr/bin/env bash
# B2 five-scenario acceptance. Requires an already running standalone mock.
set -u

if [ "$#" -gt 1 ]; then
    printf '%s\n' 'usage: bash test/e2e/mock_scenarios.sh [mock_base_url]' >&2
    exit 1
fi
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
source "$script_dir/lib/mock_control.sh" || exit 1
base_url=${1:-http://127.0.0.1:18080}
temp_root=${TMPDIR:-/tmp}
work_dir=$(mktemp -d "${temp_root%/}/gaigate-mock.XXXXXXXX") || exit 1
restore_keys=0
curl_rc=0
http_status=000
ttfb=0
elapsed=0
content_count=0
usage_count=0
done_count=0
bad_count=0
valid_usage=0

cleanup() {
    local original=$? failed=0 label key
    trap - EXIT HUP INT TERM
    if [ "$restore_keys" -eq 1 ]; then
        for label in a b; do
            key="sk-mock-e2e-$label"
            if ! mock_set_scenario "$base_url" "$key" normal; then
                printf 'cleanup: cannot restore e2e-%s to normal defaults\n' "$label" >&2
                failed=1
            fi
        done
    fi
    # Only these files in our own mktemp directory; never recurse over a path.
    if ! rm -f -- "$work_dir/body" "$work_dir/headers" "$work_dir/stats"; then
        printf '%s\n' 'cleanup: cannot remove temporary response files' >&2
        failed=1
    fi
    if ! rmdir -- "$work_dir"; then
        printf '%s\n' 'cleanup: cannot remove temporary directory' >&2
        failed=1
    fi
    if [ "$original" -eq 0 ] && [ "$failed" -ne 0 ]; then original=1; fi
    exit "$original"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
    printf '[FAIL] %s: %s (HTTP=%s curl=%s content=%s usage=%s DONE=%s invalid=%s ttfb=%s total=%s)\n' \
        "$1" "$2" "$http_status" "$curl_rc" "$content_count" "$usage_count" \
        "$done_count" "$bad_count" "$ttfb" "$elapsed" >&2
    exit 1
}

request() {
    local path=$1 key=${2-} budget=${3:-10} stats
    local -a args=(--disable --silent --noproxy '*' --http1.1
        --connect-timeout 2 --max-time "$budget"
        --output "$work_dir/body" --dump-header "$work_dir/headers"
        --write-out '%{http_code} %{time_starttransfer} %{time_total}\n')
    content_count=0 usage_count=0 done_count=0 bad_count=0 valid_usage=0
    http_status=000 ttfb=0 elapsed=0
    if ! { : >"$work_dir/body" && : >"$work_dir/headers" && : >"$work_dir/stats"; }; then
        fail request 'cannot prepare response files'
    fi
    if [ -n "$key" ]; then
        args+=(--no-buffer --request POST --header 'Content-Type: application/json'
            --header "Authorization: Bearer $key"
            --data-binary '{"model":"gpt-4o-demo","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}')
    fi
    if curl "${args[@]}" --url "${base_url%/}$path" >"$work_dir/stats" 2>/dev/null; then
        curl_rc=0
    else
        curl_rc=$?
    fi
    # Native Windows curl writes CRLF; keep the last numeric field free of CR.
    stats=$(awk '{ sub(/\r$/, ""); print }' "$work_dir/stats") || fail request 'cannot read curl statistics'
    read -r http_status ttfb elapsed <<<"$stats" || fail request 'missing curl statistics'
}

count_frames() {
    local counts
    # This is the frozen mock template subset, not a general JSON/SSE parser.
    # Field order is irrelevant. Content, usage and DONE are separate events.
    counts=$(awk '
        { sub(/\r$/, "") }
        /^data: / {
            payload = substr($0, 7)
            if (payload == "[DONE]") {
                done++
                if (content != 8 || usage != 1) bad++
                next
            }
            if (payload ~ /"choices"[[:space:]]*:[[:space:]]*\[[[:space:]]*\]/ &&
                payload ~ /"usage"[[:space:]]*:[[:space:]]*\{/) {
                usage++
                if (content != 8 || usage != 1 || done) bad++
                if (payload ~ /"prompt_tokens"[[:space:]]*:[[:space:]]*12[[:space:]]*[,}]/ &&
                    payload ~ /"completion_tokens"[[:space:]]*:[[:space:]]*34[[:space:]]*[,}]/ &&
                    payload ~ /"total_tokens"[[:space:]]*:[[:space:]]*46[[:space:]]*[,}]/) valid++
                else bad++
            } else if (payload ~ /"delta"[[:space:]]*:[[:space:]]*\{/ &&
                       payload ~ /"content"[[:space:]]*:/) {
                content++
                if (usage || done) bad++
                expected = "\"content\"[[:space:]]*:[[:space:]]*\"mock-" content " \""
                if (payload !~ expected) bad++
            } else bad++
            next
        }
        NF { bad++ }
        END { print content+0, usage+0, done+0, bad+0, valid+0 }
    ' "$work_dir/body") || fail parser 'cannot classify SSE events'
    read -r content_count usage_count done_count bad_count valid_usage <<<"$counts" || fail parser 'missing frame counts'
}

complete_stream() {
    count_frames
    if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 200 ] ||
       [ "$content_count" -ne 8 ] || [ "$usage_count" -ne 1 ] ||
       [ "$done_count" -ne 1 ] || [ "$bad_count" -ne 0 ] || [ "$valid_usage" -ne 1 ]; then
        fail "$1" 'expected completed stream with 8 content, 1 usage (12/34/46), 1 DONE'
    fi
}

configure() {
    if ! mock_set_scenario "$base_url" sk-mock-e2e-a "$1"; then
        fail "$1" 'control update failed'
    fi
}

request /healthz
if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 200 ] ||
   ! grep -Eq '"status"[[:space:]]*:[[:space:]]*"ok"' "$work_dir/body"; then
    fail healthz 'mock is unavailable or unhealthy'
fi
# Enable recovery before the first PUT, so partial initialization is repaired.
restore_keys=1
for label in a b; do
    if ! mock_set_scenario "$base_url" "sk-mock-e2e-$label" normal; then
        fail initialization 'cannot initialize dedicated test keys'
    fi
done

configure normal
request /v1/chat/completions sk-mock-e2e-a
complete_stream normal
printf '[PASS] normal (HTTP=%s curl=%s content=8 usage=1 DONE=1)\n' "$http_status" "$curl_rc"

configure slow
request /v1/chat/completions sk-mock-e2e-a
complete_stream slow
if ! awk -v first="$ttfb" -v total="$elapsed" 'BEGIN { exit !(first >= 1.8 && total < 10) }'; then
    fail slow 'first response headers must wait >=1.8s, completing within 10s'
fi
printf '[PASS] slow (ttfb=%s total=%s content=8 usage=1 DONE=1)\n' "$ttfb" "$elapsed"

configure downtime
request /v1/chat/completions sk-mock-e2e-a
if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 503 ] ||
   ! grep -Eq '"code"[[:space:]]*:[[:space:]]*"mock_unavailable"' "$work_dir/body"; then
    fail downtime 'expected HTTP 503 mock_unavailable with completed transport'
fi
request /v1/chat/completions sk-mock-e2e-b
complete_stream downtime/isolation
printf '%s\n' '[PASS] downtime (HTTP=503 curl=0; isolated peer content=8 usage=1 DONE=1)'

configure rate_limited
request /v1/chat/completions sk-mock-e2e-a
if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 429 ] ||
   ! grep -Eq '"code"[[:space:]]*:[[:space:]]*"mock_rate_limited"' "$work_dir/body" ||
   ! awk 'tolower($1) == "retry-after:" { sub(/\r$/, "", $2); if ($2 == "1") found=1 } END { exit !found }' "$work_dir/headers"; then
    fail rate_limited 'expected HTTP 429 mock_rate_limited with Retry-After: 1'
fi
printf '[PASS] rate_limited (HTTP=%s curl=%s Retry-After=1)\n' "$http_status" "$curl_rc"

configure interrupted
request /v1/chat/completions sk-mock-e2e-a 5
count_frames
# 18 = partial transfer; 56 = receiving network data failed. These are actual
# abnormal response ends. DNS/connect errors, local write errors and timeout 28
# must not pass merely because curl exited nonzero.
case "$curl_rc" in
    18|56) ;;
    *) fail interrupted 'expected abnormal transport end (not timeout or connection failure)' ;;
esac
if [ "$http_status" != 200 ] || [ "$content_count" -ne 3 ] ||
   [ "$usage_count" -ne 0 ] || [ "$done_count" -ne 0 ] || [ "$bad_count" -ne 0 ]; then
    fail interrupted 'expected exactly 3 content frames, no usage or DONE'
fi
printf '[PASS] interrupted (HTTP=%s curl=%s content=3 usage=0 DONE=0)\n' "$http_status" "$curl_rc"
