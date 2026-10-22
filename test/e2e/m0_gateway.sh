#!/usr/bin/env bash
# Read-only M0 smoke test. The configured primary must target this normal mock.
set -u

if [ "$#" -gt 2 ]; then
    printf '%s\n' 'usage: bash test/e2e/m0_gateway.sh [gateway_base_url] [mock_base_url]' >&2
    exit 1
fi
api_key=${GAIGATE_E2E_API_KEY-}
if [ "${#api_key}" -ne 46 ]; then
    printf '%s\n' 'GAIGATE_E2E_API_KEY must contain a valid gateway Key' >&2
    exit 1
fi
case "$api_key" in
    sk-*) ;;
    *) printf '%s\n' 'invalid gateway Key format' >&2; exit 1 ;;
esac
case "$api_key" in
    *[!a-zA-Z0-9_-]*) printf '%s\n' 'invalid gateway Key format' >&2; exit 1 ;;
esac
gateway=${1:-http://127.0.0.1:8080}
mock=${2:-http://127.0.0.1:18080}
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/gaigate-m0.XXXXXXXX") || exit 1
curl_rc=0 http_status=000 content=0 usage=0 done_count=0 invalid=0
cleanup() {
    local original=$? failed=0
    trap - EXIT HUP INT TERM
    if ! rm -f -- "$work_dir/body" "$work_dir/headers" "$work_dir/stats"; then failed=1; fi
    if ! rmdir -- "$work_dir"; then failed=1; fi
    if [ "$failed" -ne 0 ]; then
        printf '%s\n' 'cleanup: cannot remove owned temporary response files' >&2
        if [ "$original" -eq 0 ]; then original=1; fi
    fi
    exit "$original"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

fail() {
    printf '[FAIL] %s (HTTP=%s curl=%s content=%s usage=%s DONE=%s invalid=%s)\n' \
        "$1" "$http_status" "$curl_rc" "$content" "$usage" "$done_count" "$invalid" >&2
    if [ "$curl_rc" -ne 0 ]; then exit "$curl_rc"; fi
    exit 1
}

request() {
    local base=$1 path=$2 key=${3-} body=${4-}
    local -a args=(--disable --silent --noproxy '*' --http1.1 --no-buffer
        --connect-timeout 2 --max-time 10 --output "$work_dir/body"
        --dump-header "$work_dir/headers" --write-out '%{http_code}\n')
    content=0 usage=0 done_count=0 invalid=0 http_status=000 curl_rc=0
    if ! { : >"$work_dir/body" && : >"$work_dir/headers" && : >"$work_dir/stats"; }; then fail 'cannot prepare response files'; fi
    if [ -n "$body" ]; then args+=(--request POST --header 'Content-Type: application/json' --data-binary "$body"); fi
    # Credentials only cross stdin, not command arguments, files or diagnostics.
    if { if [ -n "$key" ]; then printf 'header = "Authorization: Bearer %s"\n' "$key"; fi; } |
        curl "${args[@]}" --config - --url "${base%/}$path" >"$work_dir/stats" 2>/dev/null; then
        curl_rc=0
    else
        curl_rc=$?
    fi
    http_status=$(awk '{ sub(/\r$/, ""); print }' "$work_dir/stats") || fail 'cannot read HTTP status'
}

media_is() {
    awk -v expected="$1" '
        { sub(/\r$/, ""); colon=index($0, ":") }
        colon && tolower(substr($0,1,colon-1))=="content-type" {
            value=substr($0,colon+1); sub(/;.*/,"",value)
            gsub(/^[[:space:]]+|[[:space:]]+$/,"",value)
            if (tolower(value)==expected) found=1
        }
        END { exit !found }
    ' "$work_dir/headers"
}

stream_check() {
    local counts
    counts=$(awk '
        { sub(/\r$/, "") }
        /^data: / {
            p=substr($0,7)
            if (p=="[DONE]") { end++; if (c!=8 || u!=1 || end!=1) bad++; next }
            if (p ~ /"choices"[[:space:]]*:[[:space:]]*\[[[:space:]]*\]/ && p ~ /"usage"[[:space:]]*:/) {
                u++; if (c!=8 || u!=1 || end) bad++
                if (p !~ /"prompt_tokens"[[:space:]]*:[[:space:]]*12[[:space:]]*[,}]/ ||
                    p !~ /"completion_tokens"[[:space:]]*:[[:space:]]*34[[:space:]]*[,}]/ ||
                    p !~ /"total_tokens"[[:space:]]*:[[:space:]]*46[[:space:]]*[,}]/) bad++
            } else if (p ~ /"delta"[[:space:]]*:/ && p ~ /"content"[[:space:]]*:/) {
                c++; if (u || end) bad++
                expected="\"content\"[[:space:]]*:[[:space:]]*\"mock-" c " \""
                if (p !~ expected) bad++
            } else bad++
            next
        }
        NF { bad++ }
        END { print c+0,u+0,end+0,bad+0 }
    ' "$work_dir/body") || fail 'cannot classify SSE'
    read -r content usage done_count invalid <<<"$counts" || fail 'missing SSE counts'
    if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 200 ] || [ "$content" -ne 8 ] ||
       [ "$usage" -ne 1 ] || [ "$done_count" -ne 1 ] || [ "$invalid" -ne 0 ] ||
       ! media_is text/event-stream; then fail "$1"; fi
    printf '[PASS] %s HTTP=200 curl=0 content=8 usage=1 DONE=1\n' "$1"
}

json_check() {
    if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 200 ] ||
       ! media_is application/json ||
       ! grep -Eq '"object"[[:space:]]*:[[:space:]]*"chat.completion"' "$work_dir/body" ||
       ! grep -Eq '"content"[[:space:]]*:[[:space:]]*"mock-1 mock-2 mock-3 mock-4 mock-5 mock-6 mock-7 mock-8 "' "$work_dir/body" ||
       ! grep -Eq '"prompt_tokens"[[:space:]]*:[[:space:]]*12[[:space:]]*[,}]' "$work_dir/body" ||
       ! grep -Eq '"completion_tokens"[[:space:]]*:[[:space:]]*34[[:space:]]*[,}]' "$work_dir/body" ||
       ! grep -Eq '"total_tokens"[[:space:]]*:[[:space:]]*46[[:space:]]*[,}]' "$work_dir/body"; then fail "$1"; fi
    printf '[PASS] %s HTTP=200 curl=0 usage=12/34/46\n' "$1"
}

for base in "$gateway" "$mock"; do
    request "$base" /healthz
    if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 200 ] ||
       ! grep -Eq '"status"[[:space:]]*:[[:space:]]*"ok"' "$work_dir/body"; then fail 'healthz'; fi
done
printf '%s\n' '[PASS] healthz gateway/mock'
stream='{"model":"gpt-4o-demo","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}'
request "$mock" /v1/chat/completions sk-mock-a "$stream"
stream_check 'direct mock normal'
request "$gateway" /v1/chat/completions "$api_key" "$stream"
stream_check 'gateway normal (same include_usage=true)'
request "$gateway" /v1/chat/completions "$api_key" '{"model":"gpt-4o-demo","messages":[{"role":"user","content":"hi"}],"stream":true}'
stream_check 'gateway forced usage (client omitted options)'
nonstream='{"model":"gpt-4o-demo","messages":[{"role":"user","content":"hi"}],"stream":false}'
request "$mock" /v1/chat/completions sk-mock-a "$nonstream"
json_check 'direct mock nonstream'
request "$gateway" /v1/chat/completions "$api_key" "$nonstream"
json_check 'gateway nonstream'
request "$gateway" /v1/chat/completions '' "$nonstream"
if [ "$curl_rc" -ne 0 ] || [ "$http_status" != 401 ] ||
   ! grep -Eq '"code"[[:space:]]*:[[:space:]]*"gaigate_unauthorized"' "$work_dir/body" ||
   ! grep -Eq '"request_id"[[:space:]]*:[[:space:]]*"req_' "$work_dir/body"; then fail 'missing Key'; fi
printf '%s\n' '[PASS] missing Key HTTP=401 curl=0'
