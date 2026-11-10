package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFailoverScriptLinuxJoinsActualCurl(t *testing.T) {
	for _, failure := range []string{"request", "signal"} {
		t.Run(failure, func(t *testing.T) {
			checkFailoverScriptCleanup(t, failure, func(cmd *exec.Cmd) func() {
				// Observe actual descendants before releasing the failure response.
				// An orphan or zombie is still present in /proc and fails the check.
				var probes []int
				var visit func(int)
				visit = func(parent int) {
					children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", parent, parent))
					if err != nil { // A short-lived foreground state request may exit.
						return
					}
					for _, child := range strings.Fields(string(children)) {
						pid, err := strconv.Atoi(child)
						require.NoError(t, err)
						argv, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
						comm, _ := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
						if strings.TrimSpace(string(comm)) == "curl" && bytes.Contains(argv, []byte("/probe-body")) {
							probes = append(probes, pid)
						}
						visit(pid)
					}
				}
				visit(cmd.Process.Pid)
				require.Len(t, probes, 1, "must observe the actual held curl, not a wrapper shell")
				if failure == "signal" {
					require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
				}
				return func() {
					require.Eventually(t, func() bool {
						return syscall.Kill(probes[0], 0) == syscall.ESRCH
					}, 500*time.Millisecond, 10*time.Millisecond, "actual curl must exit and be reaped before fixture teardown")
					t.Logf("observed held curl PID %d exited and was reaped", probes[0])
				}
			})
		})
	}
}
