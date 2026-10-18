// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build harness

package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	defaultTimeout = 2 * time.Minute
	pollInterval   = 2 * time.Second

	controllerModeObserve = "observe"
	controllerModeIgnore  = "ignore"
	controllerModeAct     = "act"
	watchdogModeAct       = "act"

	adviceRestarted  = "ADVICE_STATE_RESTARTED"
	adviceTerminated = "ADVICE_STATE_TERMINATED"
	adviceReleased   = "ADVICE_STATE_RELEASED"

	jobStatusPending   = "JOB_STATUS_PENDING"
	jobStatusRunning   = "JOB_STATUS_RUNNING"
	jobStatusCompleted = "JOB_STATUS_COMPLETED"
	jobStatusFailed    = "JOB_STATUS_FAILED"

	slurmStatePending = "PENDING"
	slurmStateRunning = "RUNNING"
)

type harness struct {
	t            *testing.T
	dir          string
	debugURL     string
	peerDebugURL string
	timeout      time.Duration
	httpClient   *http.Client
}

type upsertSummary struct {
	JobID             string         `json:"job_id"`
	State             string         `json:"state"`
	SchedulerMetadata map[string]any `json:"scheduler_metadata"`
}

type squeueJobSummary struct {
	State        string
	Reason       string
	RestartCount int
	Held         bool
}

type squeueJobState string

func (state *squeueJobState) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*state = squeueJobState(single)
		return nil
	}

	var states []string
	if err := json.Unmarshal(data, &states); err != nil {
		return fmt.Errorf("job state must be a string or array of strings: %w", err)
	}
	if len(states) == 0 {
		return fmt.Errorf("job state array cannot be empty")
	}
	for index, value := range states {
		if value == "" {
			return fmt.Errorf("job state array element %d cannot be empty", index)
		}
	}
	*state = squeueJobState(states[0])
	return nil
}

type debugAdvice struct {
	AdviceID          string         `json:"advice_id"`
	JobID             string         `json:"job_id"`
	AttemptIndex      int32          `json:"attempt_index"`
	State             string         `json:"state,omitempty"`
	Exclusions        []string       `json:"exclusions,omitempty"`
	SchedulerMetadata map[string]any `json:"scheduler_metadata"`
}

func TestEnvironmentWithOverrides(t *testing.T) {
	got := environmentWithOverrides(
		[]string{"PATH=/usr/bin", "SLURM_ADVISOR_MODE=act", "UNCHANGED=value"},
		[]string{"SLURM_ADVISOR_MODE=observe", "SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=false"},
	)
	want := []string{
		"PATH=/usr/bin",
		"SLURM_ADVISOR_MODE=observe",
		"UNCHANGED=value",
		"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=false",
	}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("environmentWithOverrides() = %#v, want %#v", got, want)
	}
}

func TestSafetyMatrix(t *testing.T) {
	h := newHarness(t)
	h.waitForSlurm()

	h.runScenario("ignore default does not upsert an uncommented job", func(h harness) {
		h.startController(controllerModeIgnore)
		jobID := h.submitRunningSleepJob("advisor-default-ignore")
		h.requireNoUpsert(jobID)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("ignore default ignores a valid comment without mode", func(h harness) {
		h.startController(controllerModeIgnore)
		jobID := h.submitManagedRunningSleepJob("advisor-default-ignore-managed", "")
		h.requireNoUpsert(jobID)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("job observe overrides ignore default", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-job-observe", controllerModeObserve)
		h.startController(controllerModeIgnore)
		h.waitForUpsert(jobID, jobStatusRunning)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("job ignore overrides observe default", func(h harness) {
		h.startController(controllerModeObserve)
		jobID := h.submitManagedRunningSleepJob("advisor-job-ignore", controllerModeIgnore)
		h.requireNoUpsert(jobID)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("observe upserts pending running and final jobs", func(h harness) {
		h.startController(controllerModeObserve)

		heldJob := h.submitJob("advisor-pending", "--comment="+controllerComment(""), "--hold", "--wrap=sleep 300")
		h.waitForUpsert(heldJob, jobStatusPending)

		runningJob := h.submitJob("advisor-running", "--comment="+controllerComment(""), "/jobs/two-step.sh")
		h.waitForUpsert(runningJob, jobStatusRunning)
		h.waitForUpsert(runningJob, jobStatusCompleted)
	})

	h.runScenario("observe publishes expanded parent and numeric step stdout paths", func(h harness) {
		jobID := h.submitJob(
			"advisor-log-paths",
			"--comment="+controllerComment(controllerModeObserve),
			"--output=/data/parent-%j.out",
			"--wrap=srun --output=/data/step-%j-%s.out sleep 300",
		)
		h.waitForSqueueState(jobID, slurmStateRunning)
		h.waitForAccountingSteps(jobID, "batch", "0")

		h.startController(controllerModeObserve)
		h.waitForUpsertLogPaths(jobID, []string{
			"/data/parent-" + jobID + ".out",
			"/data/step-" + jobID + "-0.out",
		})
	})

	h.runScenario("observe materializes managed parent and numeric step log links", func(h harness) {
		jobID := h.submitJob(
			"advisor-log-links",
			"--comment="+controllerComment(controllerModeObserve),
			"--output=/data/parent-%j.log",
			"--wrap=srun --output=/data/step-%j-%s.log sleep 300",
		)
		h.waitForSqueueState(jobID, slurmStateRunning)
		h.waitForAccountingSteps(jobID, "batch", "0")

		h.startControllerWithLogLinks(controllerModeObserve)
		h.waitForLogLink("parent-"+jobID+".log", "/data/parent-"+jobID+".log")
		h.waitForLogLink("step-"+jobID+"-0.log", "/data/step-"+jobID+"-0.log")
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("explicit ignore excludes log link materialization", func(h harness) {
		jobID := h.submitJob(
			"advisor-log-links-ignore",
			"--comment="+controllerComment(controllerModeIgnore),
			"--output=/data/ignored-%j.log",
			"--wrap=sleep 300",
		)
		h.waitForSqueueState(jobID, slurmStateRunning)

		h.startControllerWithLogLinks(controllerModeObserve)
		h.requireNoLogLink("ignored-" + jobID + ".log")
		h.requireNoUpsert(jobID)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("observe-only restart advice does not mutate Slurm", func(h harness) {
		jobID := h.submitRunningSleepJob("advisor-observe-only")
		h.putAdvice(restartAdvice(jobID))

		h.startController(controllerModeObserve)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("observe default observes a valid comment without mode", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-default-observe-managed", "")
		h.putAdvice(terminateAdvice(jobID))

		h.startController(controllerModeObserve)
		h.waitForUpsert(jobID, jobStatusRunning)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("action restart advice releases requeued job from hold", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-restart", "")
		initialRestartCount := h.requireSqueueJob(jobID).RestartCount
		h.putAdvice(restartAdvice(jobID))

		h.startController(controllerModeAct)
		h.waitForRequeuedReleasedJob(jobID, initialRestartCount)
	})

	h.runScenario("action release advice opens the exact initial DAG gate", func(h harness) {
		jobID := h.submitJob(
			"advisor-dag-release",
			"--hold",
			"--comment="+watchdogComment(true),
			"--wrap=sleep 300",
		)
		h.putAdvice(releaseAdvice(jobID))

		h.startController(controllerModeAct)
		h.waitForReleasedFromHold(jobID)
	})

	h.runScenario("fresh restart advice requeues a completed job missing from squeue", func(h harness) {
		jobID := h.submitJob(
			"advisor-restart-completed",
			"--comment="+controllerComment(""),
			"--wrap=bash /jobs/complete-then-sleep.sh",
		)
		h.waitForSacctStateAndComment(jobID, "COMPLETED", controllerComment(""))
		h.waitForSqueueAbsence(jobID)
		h.putAdvice(restartAdvice(jobID))

		h.startController(controllerModeAct)
		h.waitForRequeuedReleasedJob(jobID, 0)
	})

	h.runScenario("failed job waits for delayed restart advice before creating a successor", func(h harness) {
		h.startController(controllerModeAct)
		jobID := h.submitTrackedFailingJob("advisor-delayed-restart")
		h.waitForUpsert(jobID, jobStatusFailed)
		h.requireFailedWithoutSuccessor(jobID)

		h.putAdvice(restartAdvice(jobID))
		h.waitForRequeuedReleasedJob(jobID, 0)
	})

	h.runScenario("delayed termination advice leaves a failed job without a successor", func(h harness) {
		h.startController(controllerModeAct)
		jobID := h.submitTrackedFailingJob("advisor-delayed-termination")
		h.waitForUpsert(jobID, jobStatusFailed)
		h.requireFailedWithoutSuccessor(jobID)

		h.putAdvice(terminateAdvice(jobID))
		h.requireFailedWithoutSuccessor(jobID)
	})

	h.runScenario("explicit eager recovery flag requeues a failed job without advice", func(h harness) {
		h.startControllerWithEnv(controllerModeAct, "SLURM_ADVISOR_ENABLE_EAGER_REQUEUE=true")
		jobID := h.submitTrackedFailingJob("advisor-eager-recovery")
		h.waitForUpsert(jobID, jobStatusFailed)
		h.wait(func() (bool, string) {
			job, ok := h.squeueJob(jobID)
			return ok && job.RestartCount == 1 && job.Held,
				fmt.Sprintf("found=%t restart_count=%d held=%t", ok, job.RestartCount, job.Held)
		})
	})

	h.runScenario("action terminate advice converges through scancel", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-terminate", controllerModeAct)
		h.putAdvice(terminateAdvice(jobID))

		h.startController(controllerModeAct)
		h.waitForSacctState(jobID, "CANCELLED")
	})

	h.runScenario("action restart with exclusion updates constraints after requeuehold", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-node-exclusion", "ACT")
		h.putAdvice(restartAdvice(jobID, "c1"))

		h.startController(controllerModeAct)
		h.waitForPendingWithJobDetail(jobID, "ExcNodeList=c1")
	})

	h.runScenario("action failure does not starve later terminate advice", func(h harness) {
		failingJob := h.submitManagedRunningSleepJob("advisor-failing-action", controllerModeAct)
		terminatedJob := h.submitManagedRunningSleepJob("advisor-after-failure", controllerModeAct)

		h.putAdvice(
			nodeConstraintAdvice(failingJob, "c3"),
			terminateAdvice(terminatedJob),
		)

		h.startController(controllerModeAct)
		h.waitForSacctState(terminatedJob, "CANCELLED")
		h.requireSqueueState(failingJob, slurmStateRunning)
	})

	h.runScenario("api outage does not mutate Slurm", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-api-outage", controllerModeAct)

		h.compose("stop", "mock-heimdall")
		h.startController(controllerModeAct)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)

		h.compose("up", "-d", "mock-heimdall")
		h.waitForHTTP(h.debugURL + "/healthz")
	})

	h.runScenario("act default without comment cannot mutate Slurm", func(h harness) {
		jobID := h.submitRunningSleepJob("advisor-missing-comment")
		h.putAdvice(terminateAdvice(jobID))

		h.startController(controllerModeAct)
		h.requireNoUpsert(jobID)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("invalid job mode cannot mutate Slurm", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-invalid-mode", "act ")
		h.putAdvice(terminateAdvice(jobID))

		h.startController(controllerModeAct)
		h.waitForUpsert(jobID, jobStatusRunning)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("unacknowledged job act request is capped to observe", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-denied-job-act", controllerModeAct)
		h.putAdvice(terminateAdvice(jobID))

		h.startController(controllerModeObserve)
		h.waitForUpsert(jobID, jobStatusRunning)
		time.Sleep(3 * pollInterval)
		h.requireSqueueState(jobID, slurmStateRunning)
	})

	h.runScenario("acknowledged job act request overrides observe default", func(h harness) {
		jobID := h.submitManagedRunningSleepJob("advisor-allowed-job-act", controllerModeAct)
		h.putAdvice(terminateAdvice(jobID))

		h.startControllerWithEnv(
			controllerModeObserve,
			"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=true",
		)
		h.waitForSacctState(jobID, "CANCELLED")
	})

	h.runScenario("two environment controllers isolate publication and actions", func(h harness) {
		peer := h.peer()
		h.ensureEnvironmentPeers()
		h.resetAdvice()
		peer.resetAdvice()

		blueJob := h.submitEnvironmentRunningSleepJob("advisor-blue", controllerModeAct, "blue")
		greenJob := h.submitEnvironmentRunningSleepJob("advisor-green", controllerModeAct, "green")
		h.putAdvice(terminateAdvice(greenJob))
		peer.putAdvice(terminateAdvice(blueJob))
		h.startEnvironmentControllers("blue", "green")

		h.waitForUpsert(blueJob, jobStatusRunning)
		peer.waitForUpsert(greenJob, jobStatusRunning)
		h.requireNoUpsert(greenJob)
		peer.requireNoUpsert(blueJob)
		h.requireSqueueState(blueJob, slurmStateRunning)
		h.requireSqueueState(greenJob, slurmStateRunning)

		h.putAdvice(terminateAdvice(blueJob))
		peer.putAdvice(terminateAdvice(greenJob))
		h.waitForSacctState(blueJob, "CANCELLED")
		h.waitForSacctState(greenJob, "CANCELLED")
		h.requireNoUpsert(greenJob)
		peer.requireNoUpsert(blueJob)
	})

	h.runScenario("two environment watchdogs isolate release actions", func(h harness) {
		blueJob := h.submitJob(
			"watchdog-blue",
			"--hold",
			"--comment="+watchdogCommentForEnvironment(false, "blue/primary"),
			"--wrap=sleep 300",
		)
		greenJob := h.submitJob(
			"watchdog-green",
			"--hold",
			"--comment="+watchdogCommentForEnvironment(false, "green/primary"),
			"--wrap=sleep 300",
		)
		redJob := h.submitJob(
			"watchdog-red",
			"--hold",
			"--comment="+watchdogCommentForEnvironment(false, "red/primary"),
			"--wrap=sleep 300",
		)

		h.compose("stop", "controller", "mock-heimdall")
		h.startEnvironmentWatchdogs("blue/*", "green/*")
		h.waitForReleasedFromHold(blueJob)
		h.waitForReleasedFromHold(greenJob)
		h.requireHeld(redJob)

		blueLogs := h.waitForWatchdogReleaseLog("watchdog", blueJob)
		greenLogs := h.waitForWatchdogReleaseLog("watchdog-peer", greenJob)
		if strings.Contains(blueLogs, `"job_id":"`+greenJob+`"`) {
			h.t.Fatalf("blue watchdog reported a release candidate for green job %s", greenJob)
		}
		if strings.Contains(greenLogs, `"job_id":"`+blueJob+`"`) {
			h.t.Fatalf("green watchdog reported a release candidate for blue job %s", blueJob)
		}
		if strings.Contains(blueLogs, `"job_id":"`+redJob+`"`) || strings.Contains(greenLogs, `"job_id":"`+redJob+`"`) {
			h.t.Fatalf("an environment-scoped watchdog reported a release candidate for unmatched red job %s", redJob)
		}
	})

	h.runScenario("standalone watchdog derives release eligibility from current Slurm state", func(h harness) {
		jobID := h.submitJob(
			"standalone-watchdog-same-name",
			"--comment="+watchdogComment(false),
			"--wrap=sleep 300",
		)
		h.waitForSqueueState(jobID, slurmStateRunning)
		initialRestartCount := h.requireSqueueJob(jobID).RestartCount

		coldStartSibling := h.submitJob(
			"standalone-watchdog-same-name",
			"--hold",
			"--comment="+watchdogComment(false),
			"--wrap=sleep 300",
		)
		unmanaged := h.submitJob(
			"standalone-watchdog-unmanaged",
			"--hold",
			"--wrap=sleep 300",
		)
		dagInitialAttempt := h.submitJob(
			"standalone-watchdog-dag-initial",
			"--hold",
			"--comment="+watchdogComment(true),
			"--wrap=sleep 300",
		)

		h.compose("stop", "controller", "mock-heimdall")
		h.requireServiceStopped("controller")
		h.requireServiceStopped("mock-heimdall")
		h.startWatchdog(watchdogModeAct)
		h.waitForWatchdogManagedJobsAtLeast(3)

		h.requeueHold(jobID)
		h.waitForRequeuedReleasedJob(jobID, initialRestartCount)
		h.waitForReleasedFromHold(coldStartSibling)
		h.requireHeld(unmanaged)
		h.requireHeld(dagInitialAttempt)
		h.requireServiceStopped("controller")
		h.requireServiceStopped("mock-heimdall")
	})
}

func restartAdvice(jobID string, exclusions ...string) debugAdvice {
	advice := managedAdvice("restart-"+jobID, jobID, adviceRestarted)
	advice.Exclusions = append([]string(nil), exclusions...)
	return advice
}

func terminateAdvice(jobID string) debugAdvice {
	return managedAdvice("terminate-"+jobID, jobID, adviceTerminated)
}

func releaseAdvice(jobID string) debugAdvice {
	return managedAdvice("release-"+jobID, jobID, adviceReleased)
}

func nodeConstraintAdvice(jobID string, exclusions ...string) debugAdvice {
	advice := managedAdvice("constraints-"+jobID, jobID, "")
	advice.Exclusions = append([]string(nil), exclusions...)
	return advice
}

func watchdogComment(dagChild bool) string {
	return watchdogCommentForEnvironment(dagChild, "")
}

func watchdogCommentForEnvironment(dagChild bool, environment string) string {
	spec := map[string]any{"workflow_id": "watchdog-harness"}
	if environment != "" {
		spec["environment"] = environment
	}
	if dagChild {
		spec["dag_dependencies"] = []map[string]any{{
			"target":  map[string]string{"type": "name", "name": "parent"},
			"trigger": "completed",
		}}
	}
	raw, err := json.Marshal(map[string]map[string]any{"APS": spec})
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func controllerComment(mode string) string {
	return controllerCommentForEnvironment(mode, "")
}

func controllerCommentForEnvironment(mode, environment string) string {
	spec := map[string]string{}
	if mode != "" {
		spec["mode"] = mode
	}
	if environment != "" {
		spec["environment"] = environment
	}
	raw, err := json.Marshal(map[string]map[string]string{"APS": spec})
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func managedAdvice(adviceID, jobID, state string) debugAdvice {
	return debugAdvice{
		AdviceID:     adviceID,
		JobID:        jobID,
		AttemptIndex: 0,
		State:        state,
		SchedulerMetadata: map[string]any{
			"scheduler":  "slurm",
			"managed_by": "slurm-advisor-controller",
		},
	}
}

func newHarness(t *testing.T) harness {
	t.Helper()
	dir := os.Getenv("SLURM_HARNESS_DIR")
	if dir == "" {
		var err error
		dir, err = filepath.Abs("..")
		if err != nil {
			t.Fatalf("resolve harness dir: %v", err)
		}
	}
	timeout := defaultTimeout
	if raw := os.Getenv("SLURM_HARNESS_TIMEOUT"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("parse SLURM_HARNESS_TIMEOUT: %v", err)
		}
		timeout = parsed
	}
	debugURL := os.Getenv("MOCK_DEBUG_URL")
	if debugURL == "" {
		debugURL = "http://127.0.0.1:18081"
	}
	peerDebugURL := os.Getenv("MOCK_PEER_DEBUG_URL")
	if peerDebugURL == "" {
		peerDebugURL = "http://127.0.0.1:18082"
	}
	return harness{
		t:            t,
		dir:          dir,
		debugURL:     strings.TrimRight(debugURL, "/"),
		peerDebugURL: strings.TrimRight(peerDebugURL, "/"),
		timeout:      timeout,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (h harness) peer() harness {
	h.debugURL = h.peerDebugURL
	return h
}

func (h harness) runScenario(name string, test func(harness)) {
	h.t.Helper()
	h.t.Run(name, func(t *testing.T) {
		scenario := h
		scenario.t = t
		scenario.prepareScenario()
		test(scenario)
	})
}

func (h harness) prepareScenario() {
	h.t.Helper()
	h.stopController()
	h.stopEnvironmentPeers()
	h.stopWatchdog()
	h.ensureMockHeimdall()
	h.resetAdvice()
	h.cancelAllJobs()
	h.resetLogLinks()
	h.t.Cleanup(func() {
		h.stopController()
		h.stopEnvironmentPeers()
		h.stopWatchdog()
		h.ensureMockHeimdall()
		h.resetAdvice()
		h.cancelAllJobs()
		h.resetLogLinks()
	})
}

func (h harness) compose(args ...string) string {
	h.t.Helper()
	return h.composeWithEnv(nil, args...)
}

func (h harness) composeWithEnv(extraEnv []string, args ...string) string {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose"}, args...)...)
	cmd.Dir = h.dir
	cmd.Env = environmentWithOverrides(os.Environ(), extraEnv)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		h.t.Fatalf("docker compose %s failed: %v\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), err, stdout.String(), stderr.String())
	}
	return strings.TrimSpace(stdout.String())
}

func environmentWithOverrides(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	for _, entry := range append(append([]string(nil), base...), overrides...) {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = entry
	}

	out := make([]string, 0, len(order))
	for _, key := range order {
		out = append(out, values[key])
	}
	return out
}

func (h harness) waitForHTTP(url string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		resp, err := h.httpClient.Get(url)
		if err != nil {
			return false, err.Error()
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return false, resp.Status
		}
		return true, resp.Status
	})
}

func (h harness) waitForSlurm() {
	h.t.Helper()
	h.wait(func() (bool, string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", "compose", "exec", "-T", "slurmctld", "sinfo", "-h")
		cmd.Dir = h.dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, string(out)
		}
		return true, string(out)
	})
}

func (h harness) waitForUpsert(jobID, state string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		upserts, err := h.upserts()
		if err != nil {
			return false, err.Error()
		}
		for _, upsert := range upserts {
			if upsert.JobID == jobID && upsert.State == state {
				return true, fmt.Sprintf("found %s %s", jobID, state)
			}
		}
		payload, _ := json.Marshal(upserts)
		return false, string(payload)
	})
}

func (h harness) waitForUpsertLogPaths(jobID string, want []string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		upserts, err := h.upserts()
		if err != nil {
			return false, err.Error()
		}
		for _, upsert := range upserts {
			if upsert.JobID != jobID {
				continue
			}
			rawPaths, ok := upsert.SchedulerMetadata["slurm_log_file_paths"].([]any)
			if !ok {
				continue
			}
			got := make([]string, 0, len(rawPaths))
			for _, rawPath := range rawPaths {
				path, ok := rawPath.(string)
				if !ok {
					return false, fmt.Sprintf("job %s log paths contain non-string value: %#v", jobID, rawPaths)
				}
				got = append(got, path)
			}
			if strings.Join(got, "\x00") == strings.Join(want, "\x00") {
				return true, fmt.Sprintf("job %s log paths: %v", jobID, got)
			}
		}
		payload, _ := json.Marshal(upserts)
		return false, string(payload)
	})
}

func (h harness) waitForLogLink(name, target string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		observed := h.compose(
			"exec",
			"-T",
			"slurmctld",
			"find",
			"/log-links",
			"-maxdepth",
			"1",
			"-type",
			"l",
			"-name",
			name,
			"-printf",
			"%l",
		)
		return observed == target, fmt.Sprintf("%s -> %q", name, observed)
	})
}

func (h harness) requireNoLogLink(name string) {
	h.t.Helper()
	time.Sleep(3 * pollInterval)
	observed := h.compose(
		"exec",
		"-T",
		"slurmctld",
		"find",
		"/log-links",
		"-maxdepth",
		"1",
		"-name",
		name,
		"-print",
	)
	if observed != "" {
		h.t.Fatalf("found ignored log link entry %s", observed)
	}
}

func (h harness) requireNoUpsert(jobID string) {
	h.t.Helper()
	time.Sleep(3 * pollInterval)
	upserts, err := h.upserts()
	if err != nil {
		h.t.Fatalf("read upserts: %v", err)
	}
	for _, upsert := range upserts {
		if upsert.JobID == jobID {
			h.t.Fatalf("found ignored job %s in upserts: %#v", jobID, upserts)
		}
	}
}

func (h harness) upserts() ([]upsertSummary, error) {
	resp, err := h.httpClient.Get(h.debugURL + "/upserts")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET /upserts: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var out []upsertSummary
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (h harness) resetAdvice() {
	h.t.Helper()
	h.putAdvice()
}

func (h harness) ensureMockHeimdall() {
	h.t.Helper()
	h.compose("up", "-d", "mock-heimdall")
	h.waitForHTTP(h.debugURL + "/healthz")
}

func (h harness) cancelAllJobs() {
	h.t.Helper()
	h.compose("exec", "-T", "slurmctld", "bash", "-lc", "squeue -h -o '%A' | xargs -r scancel --quiet")
	h.wait(func() (bool, string) {
		out := h.compose("exec", "-T", "slurmctld", "squeue", "-h", "-o", "%A")
		out = strings.TrimSpace(out)
		return out == "", out
	})
}

func (h harness) resetLogLinks() {
	h.t.Helper()
	h.compose("exec", "-T", "slurmctld", "mkdir", "-p", "--", "/log-links")
	h.compose(
		"exec",
		"-T",
		"slurmctld",
		"find",
		"/log-links",
		"-mindepth",
		"1",
		"-maxdepth",
		"1",
		"-delete",
	)
}

func (h harness) putAdvice(advice ...debugAdvice) {
	h.t.Helper()
	payload, err := json.Marshal(advice)
	if err != nil {
		h.t.Fatalf("marshal advice: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, h.debugURL+"/advice", bytes.NewReader(payload))
	if err != nil {
		h.t.Fatalf("build advice request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.httpClient.Do(req)
	if err != nil {
		h.t.Fatalf("PUT /advice: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		h.t.Fatalf("PUT /advice: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
}

func (h harness) submitJob(name string, args ...string) string {
	h.t.Helper()
	command := "cd /data && sbatch --parsable --job-name=" + shellQuote(name)
	for _, arg := range args {
		command += " " + shellQuote(arg)
	}
	return strings.TrimSpace(h.compose("exec", "-T", "slurmctld", "bash", "-lc", command))
}

// submitTrackedFailingJob publishes the held source before allowing its first
// execution to fail, so accounting discovery does not depend on poll timing.
func (h harness) submitTrackedFailingJob(name string) string {
	h.t.Helper()
	jobID := h.submitJob(name,
		"--hold",
		"--comment="+controllerComment(controllerModeAct),
		"--wrap=bash /jobs/fail-then-sleep.sh",
	)
	h.waitForUpsert(jobID, jobStatusPending)
	h.compose("exec", "-T", "slurmctld", "scontrol", "release", jobID)
	return jobID
}

// requireFailedWithoutSuccessor checks the retained scheduler record over
// multiple controller polls. Accounting alone could still show the failed
// source after an unauthorized successor has already been created.
func (h harness) requireFailedWithoutSuccessor(jobID string) {
	h.t.Helper()
	h.waitForSacctState(jobID, "FAILED")
	for range 3 {
		detail := h.jobDetail(jobID)
		fields := strings.Fields(detail)
		if !slices.Contains(fields, "JobState=FAILED") || !slices.Contains(fields, "Restarts=0") {
			h.t.Fatalf("job %s should remain failed without a successor: %s", jobID, detail)
		}
		time.Sleep(pollInterval)
	}
}

func (h harness) submitRunningSleepJob(name string) string {
	h.t.Helper()
	jobID := h.submitJob(name, "--wrap=sleep 300")
	h.waitForSqueueState(jobID, slurmStateRunning)
	return jobID
}

func (h harness) submitManagedRunningSleepJob(name, mode string) string {
	h.t.Helper()
	jobID := h.submitJob(name, "--comment="+controllerComment(mode), "--wrap=sleep 300")
	h.waitForSqueueState(jobID, slurmStateRunning)
	return jobID
}

func (h harness) submitEnvironmentRunningSleepJob(name, mode, environment string) string {
	h.t.Helper()
	jobID := h.submitJob(name, "--comment="+controllerCommentForEnvironment(mode, environment), "--wrap=sleep 300")
	h.waitForSqueueState(jobID, slurmStateRunning)
	return jobID
}

func (h harness) stopController() {
	h.t.Helper()
	h.compose("stop", "controller")
}

func (h harness) stopEnvironmentPeers() {
	h.t.Helper()
	h.compose("--profile", "environment-isolation", "stop", "controller-peer", "mock-heimdall-peer")
}

func (h harness) ensureEnvironmentPeers() {
	h.t.Helper()
	h.compose("--profile", "environment-isolation", "up", "-d", "mock-heimdall-peer")
	h.waitForHTTP(h.peerDebugURL + "/healthz")
}

func (h harness) stopWatchdog() {
	h.t.Helper()
	h.compose("--profile", "environment-isolation", "stop", "watchdog", "watchdog-peer")
}

func (h harness) startController(mode string) {
	h.t.Helper()
	h.startControllerWithEnv(mode)
}

func (h harness) startControllerWithEnv(mode string, extraEnv ...string) {
	h.t.Helper()
	env := append(controllerEnv(mode), extraEnv...)
	h.composeWithEnv(env, "up", "-d", "--force-recreate", "--no-deps", "controller")
}

func (h harness) startEnvironmentControllers(primaryEnvironment, peerEnvironment string) {
	h.t.Helper()
	env := []string{
		"SLURM_ADVISOR_MODE=" + controllerModeAct,
		"SLURM_ADVISOR_PEER_MODE=" + controllerModeAct,
		"SLURM_ADVISOR_ENVIRONMENT=" + primaryEnvironment,
		"SLURM_ADVISOR_PEER_ENVIRONMENT=" + peerEnvironment,
		"SLURM_ADVISOR_POLL_INTERVAL=1s",
		"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=true",
		"SLURM_ADVISOR_PEER_ACKNOWLEDGE_SCHEDULER_ACTIONS=true",
	}
	h.composeWithEnv(env, "--profile", "environment-isolation", "up", "-d", "--force-recreate", "--no-deps", "controller", "controller-peer")
}

func (h harness) startControllerWithLogLinks(mode string) {
	h.t.Helper()
	h.startControllerWithEnv(
		mode,
		"SLURM_ADVISOR_LOG_LINKS_ENABLED=true",
		"SLURM_ADVISOR_LOG_LINKS_DESTINATION=/log-links",
		`SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON=["/data"]`,
		"SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL=1s",
		"SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL=1s",
	)
}

func controllerEnv(mode string) []string {
	env := []string{
		"SLURM_ADVISOR_MODE=" + mode,
		"SLURM_ADVISOR_POLL_INTERVAL=1s",
		"SLURM_ADVISOR_ADVICE_LOOP_TIMEOUT=10s",
		"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=false",
	}
	if mode == controllerModeAct {
		env = append(env, "SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS=true")
	}
	return env
}

func (h harness) startWatchdog(mode string) {
	h.t.Helper()
	env := []string{
		"SLURM_WATCHDOG_MODE=" + mode,
		"SLURM_WATCHDOG_JOB_DEFAULT_MODE=act",
		"SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED=true",
		"SLURM_WATCHDOG_POLL_INTERVAL=1s",
		"SLURM_WATCHDOG_RELEASE_TIMEOUT=1s",
		"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD=10s",
		"SLURM_WATCHDOG_METRICS_ENABLED=true",
	}
	if mode == watchdogModeAct {
		env = append(env, "SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS=true")
	}
	h.composeWithEnv(env, "up", "-d", "--force-recreate", "--no-deps", "watchdog")
}

func (h harness) startEnvironmentWatchdogs(primaryPattern, peerPattern string) {
	h.t.Helper()
	env := []string{
		"SLURM_WATCHDOG_MODE=" + watchdogModeAct,
		"SLURM_WATCHDOG_JOB_DEFAULT_MODE=act",
		"SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED=true",
		"SLURM_WATCHDOG_PEER_MODE=" + watchdogModeAct,
		"SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS=true",
		"SLURM_WATCHDOG_PEER_ACKNOWLEDGE_SCHEDULER_ACTIONS=true",
		"SLURM_WATCHDOG_ENVIRONMENT_PATTERN=" + primaryPattern,
		"SLURM_WATCHDOG_PEER_ENVIRONMENT_PATTERN=" + peerPattern,
		"SLURM_WATCHDOG_POLL_INTERVAL=1s",
		"SLURM_WATCHDOG_RELEASE_TIMEOUT=1s",
		"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD=10s",
	}
	h.composeWithEnv(
		env,
		"--profile", "environment-isolation",
		"up", "-d", "--force-recreate", "--no-deps", "watchdog", "watchdog-peer",
	)
}

func (h harness) waitForWatchdogReleaseLog(service, jobID string) string {
	h.t.Helper()
	var logs string
	h.wait(func() (bool, string) {
		logs = h.compose("--profile", "environment-isolation", "logs", "--no-color", service)
		return strings.Contains(logs, `"job_id":"`+jobID+`"`), logs
	})
	return logs
}

func (h harness) waitForWatchdogManagedJobsAtLeast(want int) {
	h.t.Helper()
	url := "http://127.0.0.1:19091/metrics"
	h.wait(func() (bool, string) {
		resp, err := h.httpClient.Get(url)
		if err != nil {
			return false, err.Error()
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err.Error()
		}
		observed := strings.TrimSpace(string(body))
		if resp.StatusCode != http.StatusOK {
			return false, observed
		}
		for _, line := range strings.Split(observed, "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 || fields[0] != "slurm_watchdog_managed_jobs" {
				continue
			}
			managed, err := strconv.Atoi(fields[1])
			if err != nil {
				return false, observed
			}
			return managed >= want, observed
		}
		return false, observed
	})
}

func (h harness) requireServiceStopped(service string) {
	h.t.Helper()
	running := strings.TrimSpace(h.compose("ps", "--status", "running", "--services", service))
	if running != "" {
		h.t.Fatalf("service %s is running, want stopped: %s", service, running)
	}
}

func (h harness) requeueHold(jobID string) {
	h.t.Helper()
	h.compose("exec", "-T", "slurmctld", "scontrol", "requeuehold", jobID)
}

func (h harness) requireHeld(jobID string) {
	h.t.Helper()
	job := h.requireSqueueJob(jobID)
	if !job.Held {
		h.t.Fatalf("job %s held = false, want held", jobID)
	}
}

func (h harness) waitForSqueueState(jobID, state string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		got := h.squeueState(jobID)
		return got == state, got
	})
}

func (h harness) waitForSqueueAbsence(jobID string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		state := h.squeueState(jobID)
		return state == "", state
	})
}

func (h harness) requireSqueueState(jobID, state string) {
	h.t.Helper()
	got := h.squeueState(jobID)
	if got != state {
		h.t.Fatalf("squeue state for job %s = %q, want %q", jobID, got, state)
	}
}

func (h harness) squeueState(jobID string) string {
	h.t.Helper()
	out := h.compose("exec", "-T", "slurmctld", "squeue", "-j", jobID, "-h", "-o", "%T")
	return strings.TrimSpace(out)
}

func (h harness) requireSqueueJob(jobID string) squeueJobSummary {
	h.t.Helper()
	job, ok := h.squeueJob(jobID)
	if !ok {
		h.t.Fatalf("job %s not found in squeue", jobID)
	}
	return job
}

func (h harness) squeueJob(jobID string) (squeueJobSummary, bool) {
	h.t.Helper()
	out := h.compose("exec", "-T", "slurmctld", "squeue", "-j", jobID, "--json")
	var payload struct {
		Jobs []struct {
			State        squeueJobState `json:"job_state"`
			Reason       string         `json:"state_reason"`
			RestartCount int            `json:"restart_cnt"`
			Held         bool           `json:"hold"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		h.t.Fatalf("parse squeue --json for job %s: %v\noutput:\n%s", jobID, err, out)
	}
	if len(payload.Jobs) == 0 {
		return squeueJobSummary{}, false
	}
	job := payload.Jobs[0]
	return squeueJobSummary{
		State:        string(job.State),
		Reason:       job.Reason,
		RestartCount: job.RestartCount,
		Held:         job.Held,
	}, true
}

func (h harness) waitForRequeuedReleasedJob(jobID string, initialRestartCount int) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		job, ok := h.squeueJob(jobID)
		if !ok {
			return false, "job not found in squeue"
		}
		ok = job.RestartCount > initialRestartCount && !job.Held
		observed := fmt.Sprintf("state=%q reason=%q restart_count=%d held=%t", job.State, job.Reason, job.RestartCount, job.Held)
		return ok, observed
	})
}

func (h harness) waitForReleasedFromHold(jobID string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		job, ok := h.squeueJob(jobID)
		if !ok {
			return false, "job not found in squeue"
		}
		observed := fmt.Sprintf("state=%q reason=%q restart_count=%d held=%t", job.State, job.Reason, job.RestartCount, job.Held)
		return !job.Held, observed
	})
}

func (h harness) waitForSacctState(jobID, statePrefix string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		out := h.compose("exec", "-T", "slurmctld", "sacct", "-Xj", jobID, "--duplicates", "-n", "-o", "State%30")
		out = strings.TrimSpace(out)
		return strings.Contains(out, statePrefix), out
	})
}

func (h harness) waitForSacctStateAndComment(jobID, statePrefix, comment string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		out := h.compose(
			"exec", "-T", "slurmctld",
			"sacct", "-Xj", jobID, "--duplicates", "-n", "-P", "-o", "State,Comment",
		)
		out = strings.TrimSpace(out)
		want := statePrefix + "|" + comment
		return strings.Contains(out, want), out
	})
}

func (h harness) waitForAccountingSteps(jobID string, want ...string) {
	h.t.Helper()
	h.wait(func() (bool, string) {
		out := h.compose(
			"exec", "-T", "slurmctld",
			"sacct", "-j", jobID, "--duplicates", "-n", "-P", "-o", "JobID",
		)
		found := make(map[string]bool, len(want))
		for _, line := range strings.Split(out, "\n") {
			rowID := strings.TrimSuffix(strings.TrimSpace(line), "|")
			stepID, ok := strings.CutPrefix(rowID, jobID+".")
			if ok {
				found[stepID] = true
			}
		}
		for _, stepID := range want {
			if !found[stepID] {
				return false, out
			}
		}
		return true, out
	})
}

func (h harness) waitForPendingWithJobDetail(jobID, want string) {
	h.t.Helper()
	deadline := time.Now().Add(h.timeout)
	var last string
	for time.Now().Before(deadline) {
		state := h.squeueState(jobID)
		detail := h.jobDetail(jobID)
		if state == slurmStatePending && strings.Contains(detail, want) {
			return
		}
		last = fmt.Sprintf("state=%q detail=%s", state, detail)
		time.Sleep(pollInterval)
	}
	h.t.Fatalf("timed out after %s waiting for job %s to reach PENDING with %q; last observed: %s", h.timeout, jobID, want, last)
}

func (h harness) jobDetail(jobID string) string {
	h.t.Helper()
	return h.compose("exec", "-T", "slurmctld", "scontrol", "show", "job", jobID)
}

func (h harness) wait(check func() (bool, string)) {
	h.t.Helper()
	deadline := time.Now().Add(h.timeout)
	var last string
	for time.Now().Before(deadline) {
		ok, observed := check()
		if ok {
			return
		}
		last = observed
		time.Sleep(pollInterval)
	}
	h.t.Fatalf("timed out after %s; last observed: %s", h.timeout, last)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
