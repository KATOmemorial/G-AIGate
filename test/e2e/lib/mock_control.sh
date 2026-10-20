# Sourceable control functions for 12.2 and downstream scenario orchestration.
# Deliberately no top-level execution or shell configuration.

mock_set_scenario() {
    local LC_ALL=C
    local base_url test_key scenario params_json body status curl_rc
    if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
        printf '%s\n' 'mock_set_scenario: expected base_url test_key scenario [params_json]' >&2
        return 2
    fi
    base_url=$1
    test_key=$2
    scenario=$3
    params_json='{}'
    if [ "$#" -eq 4 ]; then params_json=$4; fi
    case "$test_key" in
        ''|*[!a-zA-Z0-9_-]*)
            printf '%s\n' 'mock_set_scenario: invalid test identifier' >&2
            return 2 ;;
    esac
    case "$scenario" in
        normal|slow|downtime|rate_limited|interrupted) ;;
        *) printf '%s\n' 'mock_set_scenario: unknown scenario' >&2; return 2 ;;
    esac
    printf -v body '{"key":"%s","scenario":"%s","params":%s}' "$test_key" "$scenario" "$params_json"
    # --disable prevents user curlrc options (including retries or verbose
    # credential output) from changing this single-request helper's contract.
    if status=$(curl --disable --silent --noproxy '*' --http1.1 \
        --connect-timeout 2 --max-time 10 --request PUT \
        --header 'Content-Type: application/json' --data-binary "$body" \
        --output /dev/null --write-out '%{http_code}' \
        --url "${base_url%/}/admin/scenario" 2>/dev/null); then
        curl_rc=0
    else
        curl_rc=$?
    fi
    if [ "$curl_rc" -ne 0 ]; then
        printf 'mock_set_scenario: transport failed (curl=%s)\n' "$curl_rc" >&2
        return 1
    fi
    if [ "$status" != 200 ]; then
        printf 'mock_set_scenario: expected HTTP 200 (HTTP=%s)\n' "$status" >&2
        return 1
    fi
    return 0
}
