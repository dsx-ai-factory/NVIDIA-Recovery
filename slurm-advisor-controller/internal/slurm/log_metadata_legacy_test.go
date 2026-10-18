// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// The redacted 23.02.7 capture has six columns. Add the allocation ID and name
// from its companion JSON to model the eight-column legacy query, preserving
// the captured SubmitLine and accounting timestamps for all three attempts.
func TestLegacyLogMetadataOlderSlurmFixture(t *testing.T) {
	capture := readFixture(t, "testdata/redacted/sacct/slurm_23_02_7_requeued_submitlines.txt")
	var text strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(string(capture)), "\n") {
		fields := strings.SplitN(line, "|", 6)
		if len(fields) != 6 {
			t.Fatal("invalid six-column fixture row")
		}
		text.WriteString(strings.Join(fields[:5], "|") + "|" + fields[0] + "|workflow-redacted.sh|" + fields[5] + "\n")
	}
	candidates := []logRefreshCandidate{
		{ref: cluster.JobAttemptRef{JobID: "9402"}, parentDue: true, stepsDue: true},
		{ref: cluster.JobAttemptRef{JobID: "9402", AttemptIndex: 1}, parentDue: true, stepsDue: true},
		{ref: cluster.JobAttemptRef{JobID: "9402", AttemptIndex: 2}, parentDue: true, stepsDue: true},
	}
	results, err := logPathsFromLegacySacct([]byte(text.String()), candidates)
	if err != nil {
		t.Fatal(err)
	}
	wantSteps := []string{
		"/redacted/work/logs/workflow-redacted.sh_9402_date_26-10-02_time_02-23-22.log",
		"/redacted/work/logs/workflow-redacted.sh_9402_date_26-10-02_time_02-47-52.log",
		"/redacted/work/logs/workflow-redacted.sh_9402_date_26-10-02_time_02-54-26.log",
	}
	if len(results) != len(wantSteps) {
		t.Fatalf("attempts = %d; want %d", len(results), len(wantSteps))
	}
	for attempt, want := range wantSteps {
		ref := cluster.JobAttemptRef{JobID: "9402", AttemptIndex: int32(attempt)}
		result := results[ref]
		if result.missingParent || result.missingSteps || len(result.parent) != 0 ||
			result.invalidParent != 0 || result.invalidSteps != 0 || !slices.Equal(result.steps, []string{want}) {
			t.Fatalf("attempt %d = %#v; want step %q and no explicit parent output", attempt, result, want)
		}
	}
}

// Legacy discovery must use one metadata query, separate reused IDs after a
// requeue, derive array expansion values without JSON, and ignore historical
// parsing failures. A fresh cache prevents another query on later ticks.
func TestLegacyActiveLogMetadataUsesOneQuery(t *testing.T) {
	text := "9002.0|902|41|41||9000_2.0|train|srun --disable-status --vendor-flag -o %A-%a-%04j-%02s-%x.out train --output=/application/model\n" +
		"9002|901|10|20|/old|9000_2|training|sbatch -o parent-%u.out script\n" +
		"9002.0|901|21|21||9000_2.0|train|srun -o /wrong/%t.out train\n" +
		"9002|902|30|40|/work|9000_2|training|sbatch -o parent-%u.out script\n" +
		"9002.1|902|42|42||9000_2.1|train|srun --output=all.log train\n" +
		"9002.2|902|43|43||9000_2.2|train|srun --output=-worker.log train\n" +
		"9002.batch|||||||\n9002.extern|||||||\n"
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 23.02.7\n"), stderr: []byte("version warning\n")}},
		commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
			{"job_id":9002,"array_job_id":{"set":true,"number":9000},
			 "array_task_id":{"set":true,"number":2},"restart_cnt":1,
			 "job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
		]}`)}},
		commandKey(legacyLogArgs("9000_2")): {{output: []byte(text), stderr: []byte("accounting warning\n")}},
	})
	s := newTestLogMetadataCluster(t, executor, nil)
	if err := s.DetectLogMetadataMode(context.Background()); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.ActiveJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.EnrichActiveJobs(context.Background(), jobs)
	waitForLogMetadataIdle(t, s)
	for range 3 {
		jobs = s.EnrichActiveJobs(context.Background(), jobs)
		requireLogPaths(t, jobs, []string{
			"/application/model", "/logs/parent.out", "/work/9000-2-9002-00-training.out",
		})
	}
	if jobs[0].JobID != "9000_2" || jobs[0].AttemptIndex != 1 || jobs[0].State != cluster.JobStateRunning {
		t.Fatalf("active attempt changed: %#v", jobs[0])
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"sacct", "--version"}, {"squeue", "--json"}, legacyLogArgs("9000_2"),
	})
}

// The ordinary status query remains separate. One legacy metadata command fits
// a budget of one and discovers both parent/step paths for every due attempt.
func TestLegacyFinalizedLogMetadataUsesOneQuery(t *testing.T) {
	status := []byte(`{"jobs":[
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20,"end":30}},
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":40,"start":50,"end":60}}
	]}`)
	text := "12345.0|902|51|51||12345.0|train|srun -o /logs/step-1.out train\n" +
		"12345|901|10|20|/work|12345|training|sbatch -o /logs/parent-0.out script\n" +
		"12345|902|40|50|/work|12345|training|sbatch -o /logs/parent-1.out script\n" +
		"12345.0|901|21|21||12345.0|train|srun -o /logs/step-0.out train\n"
	statusArgs := []string{"sacct", "-Xj", "12345", "--duplicates", "--json"}
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 24.11.7\n")}},
		commandKey(statusArgs):                     {{output: status}},
		commandKey(legacyLogArgs("12345")):         {{output: []byte(text)}},
	})
	s := newTestLogMetadataCluster(t, executor, nil)
	s.logMetadata.queryBudget = 1
	if err := s.DetectLogMetadataMode(context.Background()); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
		{JobID: "12345"}, {JobID: "12345", AttemptIndex: 1},
	})
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs = %#v, error = %v", jobs, err)
	}
	for _, job := range jobs {
		want := [][]string{{"/logs/parent-0.out", "/logs/step-0.out"}, {"/logs/parent-1.out", "/logs/step-1.out"}}
		if job.State != cluster.JobStateCompleted || !slices.Equal(LogFilePaths(job), want[job.AttemptIndex]) {
			t.Fatalf("attempt %d = %#v", job.AttemptIndex, job)
		}
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"sacct", "--version"}, statusArgs, legacyLogArgs("12345"),
	})
}

// A missing start time in a historical allocation must not suppress discovery
// for the job. Reused step IDs still belong to their allocation's DBIndex.
func TestLegacyFinalizedLogMetadataMissingStartTimes(t *testing.T) {
	status := []byte(`{"jobs":[
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20,"end":30}},
		{"job_id":12345,"state":{"current":"FAILED"},"time":{"submission":40,"start":0,"end":60}},
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":70,"start":80,"end":90}},
		{"job_id":12345,"state":{"current":"CANCELLED"},"time":{"submission":100,"start":0,"end":110}}
	]}`)
	text := "12345|901|10|20|/work|12345|training|sbatch -o /logs/parent-0.out script\n" +
		"12345.0|901|21|21||12345.0|train|srun -o /logs/step-0.out train\n" +
		"12345|902|40|None|/work|12345|training|sbatch -o /logs/parent-1.out script\n" +
		"12345.0|902|45|45||12345.0|train|srun -o /logs/step-1.out train\n" +
		"12345|903|70|80|/work|12345|training|sbatch -o /logs/parent-2.out script\n" +
		"12345.0|903|81|81||12345.0|train|srun -o /logs/step-2.out train\n" +
		"12345|904|100|Unknown|/work|12345|training|sbatch -o /logs/parent-3.out script\n"
	statusArgs := []string{"sacct", "-Xj", "12345", "--duplicates", "--json"}
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 23.02.7\n")}},
		commandKey(statusArgs):                     {{output: status}},
		commandKey(legacyLogArgs("12345")):         {{output: []byte(text)}},
	})
	s := newTestLogMetadataCluster(t, executor, nil)
	if err := s.DetectLogMetadataMode(context.Background()); err != nil {
		t.Fatal(err)
	}
	refs := []cluster.JobAttemptRef{
		{JobID: "12345"}, {JobID: "12345", AttemptIndex: 1},
		{JobID: "12345", AttemptIndex: 2}, {JobID: "12345", AttemptIndex: 3},
	}
	jobs, err := s.FinalizedJobs(context.Background(), refs)
	if err != nil || len(jobs) != len(refs) {
		t.Fatalf("jobs = %#v, error = %v", jobs, err)
	}
	want := [][]string{
		{"/logs/parent-0.out", "/logs/step-0.out"},
		{"/logs/parent-1.out", "/logs/step-1.out"},
		{"/logs/parent-2.out", "/logs/step-2.out"},
		{"/logs/parent-3.out"},
	}
	for i, job := range jobs {
		if job.JobID != refs[i].JobID || job.AttemptIndex != refs[i].AttemptIndex ||
			!slices.Equal(LogFilePaths(job), want[i]) {
			t.Fatalf("attempt %d = %#v; want paths %v", i, job, want[i])
		}
	}
}

// Parent accounting fields predate step fields. The required status query may
// already contain a valid parent path; legacy parsing must keep it even when
// the text query fails and must not reparse unsupported parent options.
func TestLegacyFinalizedLogMetadataKeepsNativeParent(t *testing.T) {
	status := []byte(`{"jobs":[{"job_id":12345,"state":{"current":"COMPLETED"},
		"time":{"submission":10,"start":20,"end":30},"stdout_expanded":"/logs/native-parent.out"}]}`)
	statusArgs := []string{"sacct", "-Xj", "12345", "--duplicates", "--json"}
	for _, test := range []struct {
		name    string
		results []commandResult
		want    []string
	}{
		{"unsupported parent option", []commandResult{{output: []byte(
			"12345|901|10|20|/work|12345|training|sbatch -o /logs/parent-%u.out script\n" +
				"12345.0|901|21|21||12345.0|train|srun -o /logs/step.out train\n")}},
			[]string{"/logs/native-parent.out", "/logs/step.out"}},
		{"text query failure", []commandResult{{err: errors.New("unavailable")}, {err: errors.New("unavailable")}},
			[]string{"/logs/native-parent.out"}},
		{"malformed text", []commandResult{{output: []byte("invalid row")}}, []string{"/logs/native-parent.out"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 24.05.0")}},
				commandKey(statusArgs):                     {{output: status}},
				commandKey(legacyLogArgs("12345")):         test.results,
			})
			s := newTestLogMetadataCluster(t, executor, nil)
			if err := s.DetectLogMetadataMode(context.Background()); err != nil {
				t.Fatal(err)
			}
			jobs, err := s.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "12345"}})
			if err != nil {
				t.Fatal(err)
			}
			requireLogPaths(t, jobs, test.want)
			entry := s.logMetadata.attempts[cluster.JobAttemptRef{JobID: "12345"}]
			if !entry.parent.initialized || entry.parent.failures != 0 {
				t.Fatalf("valid native parent source did not succeed: %#v", entry.parent)
			}
			if got := executor.callCount(logSacctKey("12345")); got != 0 {
				t.Fatalf("legacy mode issued %d supplemental JSON queries", got)
			}
		})
	}
}

// A failed sibling accounting query must not discard successful jobs or their
// native parent paths. Keeping those paths avoids reparsing unsupported options.
func TestLegacyFinalizedLogMetadataKeepsPathsWhenSiblingAccountingFails(t *testing.T) {
	accountingErr := errors.New("accounting unavailable")
	statusArgs := []string{"sacct", "-Xj", "12345", "--duplicates", "--json"}
	failedArgs := []string{"sacct", "-Xj", "99999", "--duplicates", "--json"}
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 23.02.7")}},
		commandKey(statusArgs): {{output: []byte(`{"jobs":[{"job_id":12345,"state":{"current":"COMPLETED"},
			"time":{"submission":10,"start":20,"end":30},"stdout_expanded":"/logs/native-parent.out"}]}`)}},
		commandKey(failedArgs): {{err: accountingErr}},
		commandKey(legacyLogArgs("12345")): {{output: []byte(
			"12345|901|10|20|/work|12345|training|sbatch -o /logs/parent-%u.out script\n" +
				"12345.0|901|21|21||12345.0|train|srun -o /logs/step.out train\n")}},
	})
	s := newTestLogMetadataCluster(t, executor, nil)
	if err := s.DetectLogMetadataMode(context.Background()); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
		{JobID: "12345"}, {JobID: "99999"},
	})
	if !errors.Is(err, accountingErr) {
		t.Fatalf("FinalizedJobs() error = %v, want failed sibling accounting cause", err)
	}
	requireLogPaths(t, jobs, []string{"/logs/native-parent.out", "/logs/step.out"})
	if jobs[0].JobID != "12345" || jobs[0].AttemptIndex != 0 || jobs[0].State != cluster.JobStateCompleted {
		t.Fatalf("successful finalized job = %#v", jobs[0])
	}
}

func TestLegacyLogMetadataSeparatesSourceFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		parent  string
		step    string
		want    []string
		missing string
	}{
		{"parent fails", "sbatch -o /logs/parent-%u.out script", "srun -o /logs/step.out train",
			[]string{"/logs/step.out"}, "parent"},
		{"step fails", "sbatch -o /logs/parent.out script", "srun -o /logs/step-%t.out train",
			[]string{"/logs/parent.out"}, "steps"},
		{"one step partly fails", "sbatch -o /logs/parent.out script", "srun -o /logs/step-%t.out train --output=/logs/usable.out",
			[]string{"/logs/parent.out", "/logs/usable.out"}, "steps"},
		{"application output with parent failure", "sbatch -o /logs/parent-%u.out script", "srun train --output=/application/model",
			[]string{"/application/model"}, "parent"},
	} {
		t.Run(test.name, func(t *testing.T) {
			text := "12345|901|10|20|/work|12345|training|" + test.parent + "\n" +
				"12345.0|901|21|21||12345.0|train|" + test.step + "\n"
			ref := cluster.JobAttemptRef{JobID: "12345"}
			results, err := logPathsFromLegacySacct([]byte(text), []logRefreshCandidate{
				{ref: ref, parentDue: true, stepsDue: true},
			})
			result := results[ref]
			if err == nil || !slices.Equal(unionLogPaths(result.parent, result.steps), test.want) ||
				result.missingParent != (test.missing == "parent") || result.missingSteps != (test.missing == "steps") {
				t.Fatalf("result = %#v, error = %v", result, err)
			}
		})
	}
}

// Slurm flattens argv without escaping embedded newlines. Application scripts
// must not hide either their own explicit stdout path or another step's path.
func TestLegacyLogMetadataPreservesMultilineSubmitLines(t *testing.T) {
	for _, test := range []struct {
		name    string
		newline string
		ending  string
		script  string
	}{
		{"LF", "\n", "\n", "echo next"},
		{"CRLF", "\r\n", "\r\n", "echo next"},
		{"no trailing newline", "\n", "", "echo next"},
		{"large script", "\n", "\n", strings.Repeat("echo next\n", 1000)},
	} {
		t.Run(test.name, func(t *testing.T) {
			submitLine := strings.Join([]string{
				"srun -o /logs/step-%s.out bash -c echo first",
				"",
				"printf 'a|b|c|d|e|f|g|h'",
				test.script,
				"train --output=/application/model",
			}, test.newline)
			text := "12345|901|10|20|/work|12345|training|sbatch -o /logs/parent.out script" + test.newline +
				"12345.0|901|21|21||12345.0|train|" + submitLine + test.newline +
				"12345.1|901|22|22||12345.1|train|srun -o /logs/other.out train" + test.ending
			rows, err := parseLegacySacctRows([]byte(text))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 3 || rows[1].submitLine != submitLine {
				t.Fatalf("multiline SubmitLine was split or changed: %#v", rows)
			}
			ref := cluster.JobAttemptRef{JobID: "12345"}
			results, err := logPathsFromLegacySacct([]byte(text), []logRefreshCandidate{
				{ref: ref, parentDue: true, stepsDue: true},
			})
			if err != nil {
				t.Fatal(err)
			}
			result := results[ref]
			if result.missingParent || result.missingSteps ||
				!slices.Equal(result.parent, []string{"/logs/parent.out"}) ||
				!slices.Equal(result.steps, []string{"/application/model", "/logs/other.out", "/logs/step-0.out"}) {
				t.Fatalf("multiline response lost valid paths: %#v", result)
			}
		})
	}
}

func TestLegacyLogMetadataRejectsAmbiguousIdentity(t *testing.T) {
	allocation := "12345|901|10|20|/work|12345|training|sbatch script\n"
	for _, test := range []struct{ name, text string }{
		{"malformed row", "12345|901\n"},
		{"malformed row after allocation", allocation + "12345.0|901\n"},
		{"unknown timestamps", "12345|901|Unknown|Unknown|/work|12345|training|sbatch script\n"},
		{"missing timestamps", "12345|901|None|Unknown|/work|12345|training|sbatch script\n"},
		{"calendar timestamp", "12345|901|10|2026-10-12T20:25:14|/work|12345|training|sbatch script\n"},
		{"negative timestamp", "12345|901|10|-1|/work|12345|training|sbatch script\n"},
		{"conflicting allocation", allocation + "12345|901|30|40|/work|12345|training|sbatch script\n"},
		{"conflicting step", allocation + "12345.0|901|21|21||12345.0|train|srun -o /a train\n" +
			"12345.0|901|21|21||12345.0|train|srun -o /b train\n"},
		{"orphan step", allocation + "12345.0|902|21|21||12345.0|train|srun -o /wrong train\n"},
		{"wrong step ID", allocation + "12345.0|901|21|21||12345.1|train|srun -o /wrong train\n"},
		{"wrong array identity", allocation + "12345.0|901|21|21||12000_2.0|train|srun -o /wrong train\n"},
		{"synthetic array range", "12345|901|10|20|/work|12000_[1-2]|training|sbatch script\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			results, err := logPathsFromLegacySacct([]byte(test.text), []logRefreshCandidate{
				{ref: cluster.JobAttemptRef{JobID: "12345"}, stepsDue: true},
			})
			if err == nil || len(results) != 0 {
				t.Fatalf("ambiguous response published results: %#v, error = %v", results, err)
			}
		})
	}
}

// Partial legacy paths merge into the exact-attempt cache and retry with the
// same backoff as JSON. A complete later refresh replaces the stale step set.
func TestLegacyActiveLogMetadataRetainsPathsAndRetries(t *testing.T) {
	currentTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	allocation := "12345|901|10|20|/work|12345|training|sbatch script\n"
	for _, test := range []struct {
		name    string
		failure []commandResult
		partial []string
	}{
		{"query failure", []commandResult{{err: errors.New("unavailable")}, {err: errors.New("unavailable")}}, nil},
		{"malformed response", []commandResult{{output: []byte("invalid row")}}, nil},
		{"partial paths", []commandResult{{output: []byte(allocation +
			"12345.0|901|21|21||12345.0|train|srun -o /logs/new.out train\n" +
			"12345.1|901|22|22||12345.1|train|srun -o /logs/unsupported-%t.out train\n")}}, []string{"/logs/new.out"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			results := append([]commandResult{{output: []byte(allocation +
				"12345.0|901|21|21||12345.0|train|srun -o /logs/cached.out train\n")}}, test.failure...)
			results = append(results, commandResult{output: []byte(allocation +
				"12345.0|901|21|21||12345.0|train|srun -o /logs/recovered.out train\n")})
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 23.02.7")}},
				commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
					{"job_id":12345,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
				]}`)}},
				commandKey(legacyLogArgs("12345")): results,
			})
			s := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })
			if err := s.DetectLogMetadataMode(context.Background()); err != nil {
				t.Fatal(err)
			}
			jobs, err := s.ActiveJobs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			s.EnrichActiveJobs(context.Background(), jobs)
			waitForLogMetadataIdle(t, s)
			currentTime = currentTime.Add(time.Minute)
			s.EnrichActiveJobs(context.Background(), jobs)
			waitForLogMetadataIdle(t, s)
			jobs = s.EnrichActiveJobs(context.Background(), jobs)
			requireLogPaths(t, jobs, unionLogPaths([]string{"/logs/cached.out", "/logs/parent.out"}, test.partial))
			if got := executor.callCount(commandKey(legacyLogArgs("12345"))); got != 1+len(test.failure) {
				t.Fatalf("queries before retry = %d", got)
			}
			currentTime = currentTime.Add(10 * time.Second)
			s.EnrichActiveJobs(context.Background(), jobs)
			waitForLogMetadataIdle(t, s)
			jobs = s.EnrichActiveJobs(context.Background(), jobs)
			requireLogPaths(t, jobs, []string{"/logs/parent.out", "/logs/recovered.out"})
		})
	}
}

// legacyLogArgs names the production command independently so tests detect an
// accidental JSON query or a missing field needed for standalone text parsing.
func legacyLogArgs(jobID string) []string {
	return []string{"env", "SLURM_TIME_FORMAT=%s", "sacct", "--noheader", "--parsable2",
		"--duplicates", "--array", "-j", jobID,
		"-o", "JobIDRaw,DBIndex,Submit,Start,WorkDir,JobID,JobName,SubmitLine"}
}

func TestLogPathsFromSacctMissingPathsDoNotLeakAcrossAttempts(t *testing.T) {
	data := []byte(`{"jobs":[
		{"job_id":123,"time":{"submission":10,"start":20},
		 "steps":[{"step":{"id":"123.0","stdout_expanded":"/native/step"}}]},
		{"job_id":123,"time":{"submission":30,"start":40},"stdout_expanded":"/native/parent",
		 "steps":[{"step":{"id":"123.batch"}},{"step":{"id":"123.extern"}}]}
	]}`)
	got, err := logPathsFromSacct(data)
	if err != nil {
		t.Fatal(err)
	}
	first := got[cluster.JobAttemptRef{JobID: "123"}]
	second := got[cluster.JobAttemptRef{JobID: "123", AttemptIndex: 1}]
	if !first.missingParent || !slices.Equal(first.steps, []string{"/native/step"}) || (second.missingParent || second.missingSteps) {
		t.Fatalf("attempts = %#v", got)
	}
}

func TestSacctSubmitLogPathsUsesRecordedParentWorkDir(t *testing.T) {
	allocation := SacctJob{JobID: 123}
	got, err := sacctSubmitLogPaths("sbatch -D nested -o parent-%j.out script", "sbatch", "/work/nested", allocation, "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"/work/nested/parent-123.out"}) {
		t.Fatalf("path = %q; sbatch chdir must not be applied twice", got)
	}
}

func TestSacctSubmitLogPathsDashOutput(t *testing.T) {
	for _, test := range []struct {
		command string
		pattern string
		workDir string
		want    string
	}{
		{"srun", "-", "", ""}, // Inherited stdout needs no working directory.
		{"srun", "-worker.log", "", ""},
		{"sbatch", "-", "/work", "/work/-"}, // sbatch treats these values as filenames.
		{"sbatch", "-worker.log", "/work", "/work/-worker.log"},
		{"sbatch", "all.log", "/work", "/work/all.log"},
	} {
		t.Run(test.command+"_"+test.pattern, func(t *testing.T) {
			got, err := sacctSubmitLogPaths(test.command+" -o "+test.pattern+" executable", test.command, test.workDir, SacctJob{JobID: 123}, "0")
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, singleExpectedLogPath(test.want)) {
				t.Fatalf("path = %q, want %q", got, test.want)
			}
		})
	}
}

// Legacy discovery preserves filename expansion while matching output flags
// throughout the recorded SubmitLine.
func TestSacctSubmitLogPathsOptionsAndPatterns(t *testing.T) {
	allocation := SacctJob{JobID: 9002, Name: "training"}
	allocation.Array.JobID = 9000
	allocation.Array.TaskID = numberField{Set: true, Number: 2}
	for _, test := range []struct {
		name    string
		line    string
		want    string
		invalid bool
	}{
		{"array identity and padding", "srun -o %A-%a-%04j-%02s-%x.out train", "/work/9000-2-9002-00-training.out", false},
		{"application output", "srun train --output=/application/file", "/application/file", false},
		{"long dash stdout", "srun --output=- train", "", false},
		{"attached dash stdout", "srun -o- train", "", false},
		{"empty SubmitLine", "", "", false},
		{"non-file output", "srun -o none train", "", false},
		{"all stdout", "srun --output=all train", "", false},
		{"all prefix stdout", "srun --output=all.log train", "", false},
		{"mixed-case all prefix stdout", "srun --output=AlL.log train", "", false},
		{"dash prefix stdout", "srun --output=-worker.log train", "", false},
		{"relative all filename", "srun --output=./all.log train", "/work/all.log", false},
		{"absolute all filename", "srun --output=/logs/all.log train", "/logs/all.log", false},
		{"relative dash filename", "srun --output=./-worker.log train", "/work/-worker.log", false},
		{"absolute dash filename", "srun --output=/logs/-worker.log train", "/logs/-worker.log", false},
		{"none prefix filename", "srun --output=none.log train", "/work/none.log", false},
		{"relative task directory", "srun --chdir=sub -olog.out train", "/work/log.out", false},
		{"absolute task directory", "srun --chdir=/remote -olog.out train", "/work/log.out", false},
		{"short task directory option", "srun -D ../remote -o log.out train", "/work/log.out", false},
		{"absolute output and task directory", "srun -D /remote -o /logs/log.out train", "/logs/log.out", false},
		{"quoted output", "srun -o '/logs/with spaces.out' train", "/logs/with spaces.out", false},
		{"literal percent and padded job-step ID", "srun -o /logs/%%-%06J train", "/logs/%-009002.0", false},
		{"wait consumes its value", "srun --wait 30 -o /logs/step train", "/logs/step", false},
		{"output after double dash", "srun -- train -o /application/file", "/application/file", false},
		{"literal unmatched quote", "srun -o 'unterminated", "/work/'unterminated", false},
		{"unknown value before output", "srun --future-option value -o /unknown train", "/unknown", false},
		{"task pattern", "srun -o task-%t.out train", "", true},
		{"deferred task pattern", "srun -o task-%%t.out train", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := sacctSubmitLogPaths(test.line, "srun", "/work", allocation, "0")
			if (err != nil) != test.invalid || !slices.Equal(got, singleExpectedLogPath(test.want)) {
				t.Fatalf("path = %q, error = %v; want path %q, invalid %v", got, err, test.want, test.invalid)
			}
		})
	}
}

func TestParseLegacySacctRowsRejectsOversizedOrInvalidText(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(strings.Repeat("x", maxSacctSubmitOutputBytes+1)),
		[]byte("123|901|10|20|/work|123|training|" + strings.Repeat("x", maxSacctSubmitLineBytes)),
		[]byte("123|901|10|20|/work|123|training|" + strings.Repeat("x\n", maxSacctSubmitLineBytes/2)),
		[]byte("123|901|10|20|/work|123|training|\x00"),
		{0xff},
	} {
		if _, err := parseLegacySacctRows(data); err == nil {
			t.Fatal("invalid accounting text succeeded")
		}
	}
}

// singleExpectedLogPath keeps the one-file and inherited-output table readable.
func singleExpectedLogPath(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}
