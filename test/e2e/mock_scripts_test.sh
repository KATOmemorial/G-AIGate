#!/usr/bin/env bash
# 12.2 shell contract regression. Uses only dedicated e2e-a/b on a live mock;
# child-local curl wrappers inject failures without stopping the service.
set -u
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd) || exit 1
source "$script_dir/lib/mock_control.sh" || exit 1
base_url=${1:-http://127.0.0.1:18080}
lab_dir=$(mktemp -d "${TMPDIR:-/tmp}/gaigate-mock-test.XXXXXXXX") || exit 1
real_curl=$(command -v curl) || exit 1
restore_keys=0

finish() {
    local original=$? failed=0 file
    trap - EXIT HUP INT TERM
    if [ "$restore_keys" -eq 1 ]; then
        mock_set_scenario "$base_url" sk-mock-e2e-a normal || failed=1
        mock_set_scenario "$base_url" sk-mock-e2e-b normal || failed=1
    fi
    # All generated paths live in this mktemp directory. No recursive delete.
    for file in "$lab_dir"/* "$lab_dir/bin"/*; do
        if [ -f "$file" ]; then rm -f -- "$file" || failed=1; fi
    done
    rmdir -- "$lab_dir/bin" "$lab_dir/runs" "$lab_dir" || failed=1
    if [ "$failed" -ne 0 ]; then
        printf '%s\n' 'regression cleanup failed' >&2
        if [ "$original" -eq 0 ]; then original=1; fi
    fi
    exit "$original"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$lab_dir/bin" "$lab_dir/runs" || exit 1

die() {
    printf '[FAIL] shell regression: %s\n' "$1" >&2
    if [ -s "$lab_dir/case-err" ]; then cat "$lab_dir/case-err" >&2; fi
    exit 1
}
pass() { printf '[PASS] shell regression: %s\n' "$1"; }

# Source under nondefault shell options, traps, positional args and exports.
# A curl tripwire also catches unintended calls even when source hides output.
(
    set -euf -o pipefail
    shopt -s nullglob
    set -- 'first argument' 'second argument'
    export MOCK_SOURCE_SENTINEL='unchanged value'
    trap ':' USR1
    curl() { printf called >"$lab_dir/source-network"; return 99; }
    options=$-
    set -o >"$lab_dir/options-before"
    shopt -p >"$lab_dir/shopt-before" || :
    trap -p >"$lab_dir/traps-before"
    export -p >"$lab_dir/exports-before"
    pwd >"$lab_dir/pwd-before"
    source "$script_dir/lib/mock_control.sh" >"$lab_dir/source-out" 2>"$lab_dir/source-err"
    [ "$options" = "$-" ]
    set -o >"$lab_dir/options-after"
    shopt -p >"$lab_dir/shopt-after" || :
    trap -p >"$lab_dir/traps-after"
    export -p >"$lab_dir/exports-after"
    pwd >"$lab_dir/pwd-after"
    for item in options shopt traps exports pwd; do
        cmp "$lab_dir/$item-before" "$lab_dir/$item-after"
    done
    [ "$#" -eq 2 ] && [ "$1" = 'first argument' ] && [ "$2" = 'second argument' ]
    [ ! -s "$lab_dir/source-out" ] && [ ! -s "$lab_dir/source-err" ]
    [ ! -e "$lab_dir/source-network" ]
)
if [ "$?" -ne 0 ]; then die 'source changed caller state, printed output or called curl'; fi
pass 'source preserves options/shopt/traps/environment/cwd/arguments; no output or network'

(
    # This test runs with errexit too; every expected failure is guarded.
    set -eu -o pipefail
    fake_http=200 fake_rc=0
    curl() {
        printf 'call\n' >>"$lab_dir/helper-calls"
        printf '%s\n' "$@" >"$lab_dir/helper-argv"
        printf '%s' "$fake_http"
        return "$fake_rc"
    }
    mock_set_scenario "$base_url" sk-mock-e2e-a normal >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"
    [ ! -s "$lab_dir/helper-out" ] && [ ! -s "$lab_dir/helper-err" ]
    [ "$(wc -l <"$lab_dir/helper-calls")" -eq 1 ]
    grep -Fq '"params":{}' "$lab_dir/helper-argv"
    for failure in http transport; do
        fake_http=503 fake_rc=0
        if [ "$failure" = transport ]; then fake_http=200 fake_rc=7; fi
        if mock_set_scenario "$base_url" sk-mock-e2e-a normal >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then exit 1; fi
        [ ! -s "$lab_dir/helper-out" ] && [ -s "$lab_dir/helper-err" ]
    done
    fake_http=200 fake_rc=0
    printf -v payload '{"x":"$(printf EXECUTED > %s/executed); `printf EXECUTED`"}' "$lab_dir"
    mock_set_scenario "$base_url" sk-mock-e2e-a normal "$payload" >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"
    grep -Fq -- "$payload" "$lab_dir/helper-argv"
    [ ! -e "$lab_dir/executed" ] && [ ! -s "$lab_dir/helper-out" ]
    before=$(wc -l <"$lab_dir/helper-calls")
    for key in '' 'has space' '测试' 'sk-$(printf EXECUTED)' 'sk-"'; do
        if mock_set_scenario "$base_url" "$key" normal >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then exit 1; fi
        [ -s "$lab_dir/helper-err" ] && [ ! -s "$lab_dir/helper-out" ]
    done
    if mock_set_scenario "$base_url" sk-mock-e2e-a unknown >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then exit 1; fi
    if mock_set_scenario >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then exit 1; fi
    if mock_set_scenario a b normal '{}' extra >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then exit 1; fi
    [ "$before" -eq "$(wc -l <"$lab_dir/helper-calls")" ]
)
if [ "$?" -ne 0 ]; then die 'helper output/status/arity/identifier/data contract'; fi
pass 'helper is silent on success; single PUT; rejects bad arguments; parameters remain data'

# Real network checks: HTTP rejection and actual connection failure are distinct.
restore_keys=1
mock_set_scenario "$base_url" sk-mock-e2e-a normal || die 'live mock required'
mock_set_scenario "$base_url" sk-mock-e2e-b normal || die 'live mock required'
if mock_set_scenario "$base_url" sk-mock-e2e-a normal '{"chunks":0}' >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then
    die 'real HTTP 400 accepted'
fi
grep -q 'HTTP=400' "$lab_dir/helper-err" || die 'missing HTTP failure reason'
if mock_set_scenario http://127.0.0.1:1 sk-mock-e2e-a normal >"$lab_dir/helper-out" 2>"$lab_dir/helper-err"; then
    die 'unreachable service accepted'
fi
grep -q 'transport failed' "$lab_dir/helper-err" || die 'missing connection failure reason'
[ ! -s "$lab_dir/helper-out" ] || die 'helper failure wrote stdout'
pass 'real HTTP 400 and unreachable service fail with stderr and no stdout'

snapshot() {
    "$real_curl" --disable --silent --fail --noproxy '*' --connect-timeout 2 --max-time 5 \
        --url "${base_url%/}/admin/scenarios" >"$1" || die 'cannot read control snapshot'
}
snapshot "$lab_dir/baseline"

# Child-local curl shim. All unaffected requests use the actual curl executable
# and live server. No eval, bash -c, service management or global state resets.
cat >"$lab_dir/bin/curl" <<'SHIM'
#!/usr/bin/env bash
set -u
args=("$@")
url='' output='' headers=''
while [ "$#" -gt 0 ]; do
    case "$1" in
        --url) url=$2; shift ;;
        --output) output=$2; shift ;;
        --dump-header) headers=$2; shift ;;
    esac
    shift
done
dir=$MOCK_SCRIPT_TEST_DIR
mode=$MOCK_SCRIPT_CASE
case "$url" in
    */admin/scenario)
        n=0
        if [ -f "$dir/put-count" ]; then read -r n <"$dir/put-count"; fi
        n=$((n+1)); printf '%s\n' "$n" >"$dir/put-count"
        if { [ "$mode" = partial_init ] && [ "$n" -eq 2 ]; } ||
           { [ "$mode" = cleanup_success ] && [ "$n" -eq 8 ]; } ||
           { [ "$mode" = cleanup_failure ] && [ -f "$dir/chat-failed" ] && [ "$n" -eq 6 ]; }; then
            printf 503
            exit 0
        fi ;;
    */v1/chat/completions)
        n=0
        if [ -f "$dir/chat-count" ]; then read -r n <"$dir/chat-count"; fi
        n=$((n+1)); printf '%s\n' "$n" >"$dir/chat-count"
        if { [ "$mode" = connection ] || [ "$mode" = cleanup_failure ]; } && [ "$n" -eq 3 ]; then
            printf failed >"$dir/chat-failed"
            # An actual refused TCP connection, preserving all other options.
            for i in "${!args[@]}"; do
                if [ "${args[$i]}" = --url ]; then args[$((i+1))]=http://127.0.0.1:1/v1/chat/completions; fi
            done
            exec "$MOCK_SCRIPT_REAL_CURL" "${args[@]}"
        fi
        if [ "$n" -eq 6 ] && { [ "$mode" = timeout ] || [ "$mode" = local_write ]; }; then
            # Keep real HTTP 200 and exactly 3 interrupted frames, then inject
            # a disallowed exit status to prove frame absence alone cannot pass.
            "$MOCK_SCRIPT_REAL_CURL" "${args[@]}"
            if [ "$mode" = timeout ]; then exit 28; else exit 23; fi
        fi
        if [ "$mode" = reordered ] && { [ "$n" -eq 1 ] || [ "$n" -eq 2 ] || [ "$n" -eq 4 ]; }; then
            "$MOCK_SCRIPT_REAL_CURL" "${args[@]}"
            result=$?
            cp "$dir/reordered-frames" "$output" || exit 23
            exit "$result"
        fi
        if [ "$mode" = bad_usage ] && [ "$n" -eq 1 ]; then
            "$MOCK_SCRIPT_REAL_CURL" "${args[@]}"
            result=$?
            sed 's/"total_tokens":46/"total_tokens":47/' "$output" >"$dir/changed-body" || exit 23
            cp "$dir/changed-body" "$output" || exit 23
            exit "$result"
        fi
        if [ "$mode" = early_headers ] && [ "$n" -eq 2 ]; then
            # Preserve the real delayed stream, but report early response
            # headers; waiting only for delayed content must fail acceptance.
            stats=$("$MOCK_SCRIPT_REAL_CURL" "${args[@]}")
            result=$?
            printf '200 0.001 2.4\n'
            exit "$result"
        fi ;;
esac
exec "$MOCK_SCRIPT_REAL_CURL" "${args[@]}"
SHIM
chmod +x "$lab_dir/bin/curl" || die 'cannot prepare child curl shim'
awk 'BEGIN {
    for (i=1; i<=8; i++)
        printf "data: {\"usage\":null,\"choices\":[{\"delta\":{\"content\":\"mock-%d \",\"role\":\"assistant\"}}]}\n\n", i
    print "data: {\"usage\":{\"total_tokens\":46,\"completion_tokens\":34,\"prompt_tokens\":12},\"choices\":[]}\n"
    print "data: [DONE]\n"
}' >"$lab_dir/reordered-frames" || die 'cannot prepare reordered frame fixture'

run_case() {
    local mode=$1 expected=$2 result puts=0
    local -a shell_args=()
    if [ "$mode" = strict ]; then shell_args=(-e -u -o pipefail); fi
    rm -f -- "$lab_dir/put-count" "$lab_dir/chat-count" "$lab_dir/chat-failed" || die 'cannot reset test counters'
    if PATH="$lab_dir/bin:$PATH" TMPDIR="$lab_dir/runs" MOCK_SCRIPT_CASE="$mode" \
        MOCK_SCRIPT_TEST_DIR="$lab_dir" MOCK_SCRIPT_REAL_CURL="$real_curl" \
        bash "${shell_args[@]}" "$script_dir/mock_scenarios.sh" "$base_url" >"$lab_dir/case-out" 2>"$lab_dir/case-err"; then
        result=0
    else
        result=$?
    fi
    [ "$result" -eq "$expected" ] || die "$mode: unexpected script exit $result"
    if grep -Eq 'sk-mock-e2e-[ab]' "$lab_dir/case-out" "$lab_dir/case-err"; then die "$mode: full test key leaked"; fi
    [ -z "$(find "$lab_dir/runs" ! -path "$lab_dir/runs" -print)" ] || die "$mode: response files leaked"
    case "$mode" in
        success|strict|reordered)
            [ "$(grep -c '^\[PASS\]' "$lab_dir/case-out")" -eq 5 ] || die "$mode: expected five PASS groups"
            [ ! -s "$lab_dir/case-err" ] || die "$mode: unexpected stderr"
            grep -Eq 'interrupted .*curl=(18|56)' "$lab_dir/case-out" || die "$mode: missing actual disconnect exit"
            ;;
        partial_init)
            grep -q '\[FAIL\] initialization' "$lab_dir/case-err" || die 'missing initialization error'
            read -r puts <"$lab_dir/put-count"
            [ "$puts" -eq 4 ] || die 'partial init did not attempt both cleanup PUTs' ;;
        connection)
            grep -q '\[FAIL\] downtime' "$lab_dir/case-err" || die 'connection failure was hidden'
            # Windows may time out during connect rather than immediately
            # refuse the unused loopback port. Both must fail downtime.
            grep -Eq 'HTTP=000 curl=(7|28)' "$lab_dir/case-err" || die 'connection failure exit not collected' ;;
        timeout|local_write)
            grep -q '\[FAIL\] interrupted' "$lab_dir/case-err" || die "$mode: failure misclassified as disconnect"
            grep -q 'content=3 usage=0 DONE=0' "$lab_dir/case-err" || die "$mode: did not reach real final frames" ;;
        bad_usage)
            grep -q '\[FAIL\] normal' "$lab_dir/case-err" || die 'incorrect usage accepted' ;;
        early_headers)
            grep -q '\[FAIL\] slow' "$lab_dir/case-err" || die 'early response headers accepted' ;;
        cleanup_success|cleanup_failure)
            grep -q 'cleanup: cannot restore e2e-a' "$lab_dir/case-err" || die 'cleanup error not reported'
            read -r puts <"$lab_dir/put-count"
            if [ "$mode" = cleanup_success ]; then
                [ "$puts" -eq 9 ] || die 'cleanup stopped after first failed restore'
                [ "$(grep -c '^\[PASS\]' "$lab_dir/case-out")" -eq 5 ] || die 'cleanup failure not isolated after successful assertions'
            else
                [ "$puts" -eq 7 ] || die 'cleanup after original failure missed second key'
                grep -q '\[FAIL\] downtime' "$lab_dir/case-err" || die 'cleanup masked original failure'
            fi
            # The injected restore failure intentionally leaves a dirty key.
            # Repair via real curl before asserting exact global restoration.
            mock_set_scenario "$base_url" sk-mock-e2e-a normal || die 'cannot repair injected cleanup failure'
            ;;
    esac
    snapshot "$lab_dir/after"
    cmp -s "$lab_dir/baseline" "$lab_dir/after" || die "$mode: final global snapshot changed"
    printf '[PASS] shell regression: %s exit=%s; state restored; temp files removed\n' "$mode" "$result"
    if [ "$mode" = success ]; then cat "$lab_dir/case-out"; fi
}

run_case success 0
run_case strict 0
run_case reordered 0
run_case partial_init 1
run_case connection 1
run_case timeout 1
run_case local_write 1
run_case bad_usage 1
run_case early_headers 1
run_case cleanup_success 1
run_case cleanup_failure 1

# First health request must fail fast without any control writes.
if TMPDIR="$lab_dir/runs" bash "$script_dir/mock_scenarios.sh" http://127.0.0.1:1 >"$lab_dir/case-out" 2>"$lab_dir/case-err"; then
    die 'unreachable health probe accepted'
fi
grep -q '\[FAIL\] healthz' "$lab_dir/case-err" || die 'missing health failure'
! grep -q 'cleanup:' "$lab_dir/case-err" || die 'unhealthy startup attempted state restoration'
[ -z "$(find "$lab_dir/runs" ! -path "$lab_dir/runs" -print)" ] || die 'health failure leaked temporary files'
snapshot "$lab_dir/after"
cmp -s "$lab_dir/baseline" "$lab_dir/after" || die 'health failure changed server state'
pass 'unreachable health check fails without state changes; service remains usable'
