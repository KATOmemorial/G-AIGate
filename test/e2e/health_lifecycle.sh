#!/usr/bin/env bash
# B4: only run against the owned Go process fixture. Its transparent mock
# observer adds counters to /healthz; production mock/control APIs stay intact.
set -u
set +x
if [ "${GAIGATE_E2E_OWNED_FIXTURE-}" != 1 ]; then
    printf '%s\n' 'health_lifecycle requires GAIGATE_E2E_OWNED_FIXTURE=1' >&2; exit 2
fi
if [ "$#" -gt 2 ]; then printf '%s\n' 'expected [gateway_base_url] [mock_base_url]' >&2; exit 2; fi
admin=${GAIGATE_E2E_ADMIN_API_KEY-} user=${GAIGATE_E2E_API_KEY-}
channel=${GAIGATE_E2E_CHANNEL_ID-} mock_key=${GAIGATE_E2E_MOCK_KEY-}
for key in "$admin" "$user"; do
    case "$key" in sk-*) ;; *) printf '%s\n' 'missing gateway credential' >&2; exit 2;; esac
    if [ "${#key}" -ne 46 ]; then exit 2; fi
    case "$key" in *[!a-zA-Z0-9_-]*) exit 2;; esac
done
case "$channel" in ''|0|*[!0-9]*) printf '%s\n' 'invalid owned channel ID' >&2; exit 2;; esac
case "$mock_key" in ''|*[!a-zA-Z0-9_-]*) printf '%s\n' 'invalid owned mock identifier' >&2; exit 2;; esac
gateway=${1:-http://127.0.0.1:8080} mock=${2:-http://127.0.0.1:18080}
for url in "$gateway" "$mock"; do
    if ! [[ "$url" =~ ^http://127\.0\.0\.1:[0-9]+$ ]]; then
        printf '%s\n' 'owned fixture must use explicit loopback HTTP URLs' >&2; exit 2
    fi
done
work=$(mktemp -d "${TMPDIR:-/tmp}/gaigate-health.XXXXXXXX") || exit 1
started=$SECONDS curl_rc=0 status=000
cleanup() {
    local original=$? failed=0
    trap - EXIT HUP INT TERM
    rm -f -- "$work/body" "$work/headers" "$work/status" || failed=1
    rmdir -- "$work" || failed=1
    if [ "$failed" -ne 0 ] && [ "$original" -eq 0 ]; then original=1; fi
    exit "$original"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
fail() {
    printf '[FAIL] %s HTTP=%s curl=%s elapsed=%ss\n' "$1" "$status" "$curl_rc" "$((SECONDS-started))" >&2
    if [ "$curl_rc" -ne 0 ]; then exit "$curl_rc"; fi
    exit 1
}
watchdog() { if [ "$((SECONDS-started))" -ge 40 ]; then fail '40s total watchdog'; fi; }
request() {
    local method=$1 url=$2 credential=$3 body=${4-}
    local -a args=(--disable --silent --noproxy '*' --http1.1 --connect-timeout 1 --max-time 2
        --request "$method" --output "$work/body" --dump-header "$work/headers" --write-out '%{http_code}\n')
    watchdog
    if [ -n "$body" ]; then args+=(--header 'Content-Type: application/json' --data-binary "$body"); fi
    curl_rc=0 status=000
    # Both gateway credentials travel only over stdin, never argv or files.
    if { if [ -n "$credential" ]; then printf 'header = "Authorization: Bearer %s"\n' "$credential"; fi; } |
        curl "${args[@]}" --config - --url "$url" >"$work/status" 2>/dev/null; then :; else curl_rc=$?; fi
    status=$(tr -d '\r\n' <"$work/status") || fail 'read status'
    if [ "$curl_rc" -ne 0 ]; then fail 'transport'; fi
}
control() {
    # The mock identifier also stays out of argv/temp files. Normal is not a
    # DELETE/restore operation: the enclosing fixture destroys the owned mock.
    watchdog
    curl_rc=0
    if printf '{"key":"%s","scenario":"%s","params":{}}' "$mock_key" "$1" |
        curl --disable --silent --noproxy '*' --connect-timeout 1 --max-time 2 --request PUT \
        --header 'Content-Type: application/json' --data-binary @- --output /dev/null \
        --write-out '%{http_code}\n' --url "$mock/admin/scenario" >"$work/status" 2>/dev/null; then :; else curl_rc=$?; fi
    status=$(tr -d '\r\n' <"$work/status")
    if [ "$curl_rc" -ne 0 ] || [ "$status" != 200 ]; then fail 'owned scenario control'; fi
}
number_or_bool() {
    # The health contract contains only seven flat numeric/bool fields.
    tr ',' '\n' <"$work/body" | sed -nE "s/.*\"$1\":[[:space:]]*([0-9]+|true|false)[[:space:]]*}?[[:space:]]*$/\1/p"
}
health() {
    request GET "$gateway/api/v1/admin/channels/$channel/health" "$admin"
    [ "$status" = 200 ] || fail 'real-time health'
    available=$(number_or_bool available) until=$(number_or_bool disabled_until)
    failures=$(number_or_bool consecutive_failures) probe_at=$(number_or_bool last_probe_at) probe_ok=$(number_or_bool last_probe_ok)
    [ "$(number_or_bool channel_id)" = "$channel" ] && [ "$(number_or_bool enabled)" = true ] || fail 'channel identity/enabled'
    case "$available:$until:$failures:$probe_at:$probe_ok" in *::*|:*|*:) fail 'malformed health';; esac
    printf 'health HTTP=200 available=%s failures=%s disabled_until=%s last_probe_at=%s last_probe_ok=%s elapsed=%ss\n' \
        "$available" "$failures" "$until" "$probe_at" "$probe_ok" "$((SECONDS-started))"
}
business_count() {
    request GET "$mock/healthz" ''
    [ "$status" = 200 ] || fail 'owned mock health'
    grep -iq '^X-Gaigate-Fixture-Owned: 14.3' "$work/headers" || fail 'missing owned observer proof'
    count=$(sed -nE 's/^[Xx]-[Gg]aigate-[Ff]ixture-[Bb]usiness-[Cc]alls:[[:space:]]*([0-9]+)\r?$/\1/p' "$work/headers")
    case "$count" in ''|*[!0-9]*) fail 'missing separate business counter';; esac
}
chat() {
    request POST "$gateway/v1/chat/completions" "$user" '{"model":"gpt-4o-demo","messages":[{"role":"user","content":"B4-owned"}],"stream":false}'
    printf 'business HTTP=%s curl=%s elapsed=%ss\n' "$status" "$curl_rc" "$((SECONDS-started))"
}
# Validate authenticated management URL and the actual transparent observer.
request GET "$gateway/api/v1/admin/channels/$channel" "$admin"
[ "$status" = 200 ] || fail 'owned channel authentication'
grep -Fq "\"base_url\":\"$mock\"" "$work/body" || fail 'channel/mock URL mismatch'
business_count
control normal
chat; [ "$status" = 200 ] || fail 'initial normal'
control downtime
for ((i=0; i<80; i++)); do
    health
    if [ "$available" = false ]; then break; fi
    chat
    [ "$status" = 502 ] || [ "$status" = 503 ] || fail 'downtime request'
    sleep 0.05
done
[ "$available" = false ] && [ "$until" -gt 0 ] || fail 'never disabled'
business_count; before=$count
chat; [ "$status" = 503 ] || fail 'disabled request'
business_count; [ "$count" = "$before" ] || fail 'disabled request reached upstream'
printf 'disabled HTTP=503 business_calls_before=%s after=%s\n' "$before" "$count"
control normal
for ((i=0; i<160; i++)); do
    health
    if [ "$available" = true ]; then break; fi
    business_count; before=$count
    chat
    if [ "$status" = 200 ]; then
        health
        [ "$available" = true ] && [ "$until" = 0 ] && [ "$probe_ok" = true ] || fail 'released before valid probe'
        break
    fi
    [ "$status" = 503 ] || fail 'closed request'
    business_count; [ "$count" = "$before" ] || fail 'closed channel reached upstream'
    sleep 0.05
done
[ "$available" = true ] && [ "$until" = 0 ] && [ "$probe_ok" = true ] || fail 'no valid probe recovery'
chat; [ "$status" = 200 ] || fail 'recovered normal'
printf '[PASS] B4 owned health lifecycle; loops=80/160 watchdog=40s elapsed=%ss\n' "$((SECONDS-started))"
