#!/usr/bin/env bash
# B5. Only the owned dual-process fixture may supply these credentials/URLs.
set -u
set +x
if [ "${GAIGATE_E2E_OWNED_FIXTURE-}" != 1 ]; then printf '%s\n' 'failover requires an owned fixture' >&2; exit 2; fi
if [ "$#" -ne 3 ]; then printf '%s\n' 'expected gateway_a gateway_b fixture_control' >&2; exit 2; fi
gateway_a=$1 gateway_b=$2 control=$3
user=${GAIGATE_E2E_API_KEY-} token=${GAIGATE_E2E_CONTROL_TOKEN-} model=${GAIGATE_E2E_MODEL-}
for url in "$gateway_a" "$gateway_b" "$control"; do
    if ! [[ "$url" =~ ^http://127\.0\.0\.1:[0-9]+$ ]]; then printf '%s\n' 'explicit owned loopback URLs required' >&2; exit 2; fi
done
if [ "${#user}" -ne 46 ] || [ "${#token}" -ne 64 ]; then printf '%s\n' 'missing fixture credentials' >&2; exit 2; fi
case "$user" in sk-*) ;; *) exit 2;; esac
case "$user$token" in *[!a-zA-Z0-9_-]*) exit 2;; esac
case "$model" in ''|*[!a-zA-Z0-9_-]*) exit 2;; esac
work=$(mktemp -d "${TMPDIR:-/tmp}/gaigate-failover.XXXXXXXX") || exit 1
probe_pid= started=$SECONDS status=000 curl_rc=0
cleanup() {
    local original=$?
    trap - EXIT HUP INT TERM
    if [ -n "$probe_pid" ]; then kill "$probe_pid" 2>/dev/null || :; wait "$probe_pid" 2>/dev/null || :; fi
    rm -f -- "$work/body" "$work/status" "$work/probe-body" "$work/probe-status" || original=1
    rmdir -- "$work" || original=1
    exit "$original"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
fail() { printf '[FAIL] %s HTTP=%s curl=%s\n' "$1" "$status" "$curl_rc" >&2; if [ "$curl_rc" -ne 0 ]; then exit "$curl_rc"; fi; exit 1; }
watchdog() { if [ "$((SECONDS-started))" -ge 40 ]; then fail '40s total watchdog'; fi; }
request() {
    watchdog
    curl_rc=0 status=000
    if printf 'header = "Authorization: Bearer %s"\n' "$3" |
        curl --disable --silent --noproxy '*' --http1.1 --connect-timeout 1 --max-time 8 --config - \
        --request "$1" --header 'Content-Type: application/json' --data-binary "$4" \
        --output "$work/body" --write-out '%{http_code}\n' --url "$2" >"$work/status" 2>/dev/null; then :; else curl_rc=$?; fi
    status=$(tr -d '\r\n' <"$work/status")
    if [ "$curl_rc" -ne 0 ]; then fail 'transport'; fi
    if [ "$status" != 200 ]; then fail 'unexpected response'; fi
}
number() { tr ',' '\n' <"$work/body" | sed -nE "s/.*\"$1\":[[:space:]]*([0-9]+|true|false)[[:space:]]*}?[[:space:]]*$/\1/p"; }
state() {
    request GET "$control/state" "$token" ''
    primary=$(number primary) backup=$(number backup) active=$(number active) eligible=$(number eligible) healthy=$(number health)
    breaker=$(sed -nE 's/.*"state":"([a-z-]+)".*/\1/p' <"$work/body")
    case "$primary:$backup:$active" in *[!0-9:]*|'') fail 'invalid fixture counters';; esac
    if [ -z "$primary" ] || [ -z "$backup" ] || [ -z "$active" ] || [ "$healthy" != true ]; then fail 'health must not mask breaker'; fi
}
mode() { request POST "$control/mode" "$token" "{\"target\":\"primary\",\"mode\":\"$1\"}"; }
chat_body="{\"model\":\"$model\",\"stream\":false,\"messages\":[{\"role\":\"user\",\"content\":\"owned-b5\"}]}"
chat() { request POST "$1/v1/chat/completions" "$user" "$chat_body"; }
state
p0=$primary b0=$backup
chat "$gateway_a"
state
[ "$primary" -eq "$((p0+1))" ] && [ "$backup" -eq "$b0" ] || fail 'normal must use primary'
printf '%s\n' '[PASS] normal primary'
mode 429
chat "$gateway_a"
chat "$gateway_a"
state
[ "$primary" -eq "$((p0+3))" ] && [ "$backup" -eq "$((b0+2))" ] && [ "$breaker" = open ] || fail 'two failures must open and use backup'
printf '%s\n' '[PASS] uncommitted fallback and Redis open'
p1=$primary b1=$backup
chat "$gateway_b"
state
[ "$primary" -eq "$p1" ] && [ "$backup" -eq "$((b1+1))" ] || fail 'open must skip primary in other process'
printf '%s\n' '[PASS] open skips primary'
mode hold
while :; do state; [ "$eligible" = true ] && break; watchdog; sleep 0.05; done
# Keep $! as the actual curl owner so cleanup kills and waits for the request.
# The short credential config is supplied on stdin, never in argv or work files.
curl --disable --silent --noproxy '*' --http1.1 --connect-timeout 1 --max-time 8 --config - \
    --request POST --header 'Content-Type: application/json' --data-binary "$chat_body" \
    --output "$work/probe-body" --write-out '%{http_code}\n' --url "$gateway_a/v1/chat/completions" \
    >"$work/probe-status" 2>/dev/null <<EOF &
header = "Authorization: Bearer $user"
EOF
probe_pid=$!
while :; do state; if [ "$breaker" = half-open ] && [ "$active" -eq 1 ]; then break; fi; watchdog; sleep 0.02; done
p2=$primary b2=$backup
chat "$gateway_b"
state
[ "$primary" -eq "$p2" ] && [ "$active" -eq 1 ] && [ "$backup" -eq "$((b2+1))" ] || fail 'only one cross-process half-open probe'
request POST "$control/mode" "$token" '{"target":"primary","release":true}'
if wait "$probe_pid"; then probe_pid=; else probe_pid=; fail 'held probe transport'; fi
[ "$(tr -d '\r\n' <"$work/probe-status")" = 200 ] || fail 'held probe did not succeed'
while :; do state; [ "$breaker" = closed ] && break; watchdog; sleep 0.02; done
printf '%s\n' '[PASS] one cross-process probe closes breaker'
p3=$primary b3=$backup
chat "$gateway_b"
state
[ "$primary" -eq "$((p3+1))" ] && [ "$backup" -eq "$b3" ] || fail 'closed must return to primary'
printf '%s\n' '[PASS] recovered primary'
