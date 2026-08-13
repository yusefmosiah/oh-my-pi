
// shellout.go delegates every external CLI invocation to one native OMP manager.
// The manager has bash access; this Yaegi cell intentionally has none.
// No imports: fmt is host-injected; strings is registered by panel.go.

const shellManagerInstructions = `You are the process manager for an agentic consensus panel. You own every external CLI invocation for this run; do not delegate them to more agents.

Read each pre-written per-panelist prompt file listed below. Run all panelists in parallel. Each panelist has the same per-run deadline: use %d seconds. Prefer wrapping its command with:
  timeout --signal=TERM --kill-after=5 %d cmd...
when command -v timeout succeeds. Otherwise implement the same deadline with background execution, sleep, and kill (terminate, then force-kill if needed). Wait for every panelist and classify outcomes exactly as ok, failed, timed-out, or skipped-missing-cli.

Before launching a panelist, use command -v for its CLI. If unavailable, do not launch it; write status skipped-missing-cli. Preserve the CLI output in %s/<name>.out and write the fully expanded command (without inventing a different CLI contract) to %s/<name>.cmd. Create parent directories if needed.

Use these verified invocation contracts. MODEL is optional: omit the bracketed fragment when it is empty. Read <prompt> from that panelist's .prompt file and pass it as one shell-quoted argument.

codex:
  codex exec --cd <CWD> --sandbox read-only -c 'approval_policy="never"' --ephemeral --skip-git-repo-check [-m MODEL] <prompt>
devin:
  devin --permission-mode auto --respect-workspace-trust false [--model MODEL] -p <prompt>
claude:
  claude -p --output-format text --permission-mode plan --no-session-persistence [--model MODEL] <prompt>
cursor:
  agent --print --output-format text --mode ask --trust --force --approve-mcps --workspace <CWD> [--model MODEL] <prompt>
  (run cursor with stdin redirected from /dev/null)
opencode:
  opencode run --dir <CWD> [-m MODEL] <prompt>

Write %s/manifest.tsv with this exact header and tab-separated rows:
name<TAB>status<TAB>exit_code<TAB>duration_seconds<TAB>output<TAB>command
The output and command columns should identify the .out and .cmd artifact paths. Finally report the manifest path and each agent's status in your final response.
`

// shellOutDir mirrors the artifact default used by the driver so shell runs can
// also be invoked directly in a cell.
func shellOutDir(conf PanelConfig) string {
	if conf.OutDir != "" {
		return conf.OutDir
	}
	return ".agentic-consensus/consensus-go-" + string(conf.Mode)
}

// ManagerPrompt gives the native manager every static execution rule plus the
// concrete shell panel it must run. Prompt content remains in files, avoiding
// interpolation of user task text into this manager prompt.
func ManagerPrompt(conf PanelConfig) string {
	dir := shellOutDir(conf)
	deadline := conf.TimeoutSecs
	if deadline <= 0 {
		deadline = 180
	}

	var b strings.Builder
	fmt.Fprintf(&b, shellManagerInstructions, deadline, deadline, dir, dir, dir)
	b.WriteString("\nRun configuration:\n")
	fmt.Fprintf(&b, "CWD: %q\n", conf.CWD)
	fmt.Fprintf(&b, "OutDir: %q\n", dir)
	b.WriteString("Panelists:\n")
	for _, p := range conf.Shell {
		fmt.Fprintf(&b, "- name=%q cli=%q model=%q prompt=%q output=%q command=%q\n",
			p.Name, p.CLI, p.Model, dir+"/"+p.Name+".prompt",
			dir+"/"+p.Name+".out", dir+"/"+p.Name+".cmd")
	}
	return b.String()
}

// spawnManager starts the one native process manager. Its id is both the agent
// identifier and the Hub wait job id.
func spawnManager(conf PanelConfig) (string, error) {
	value, err := omp.AgentWith(ManagerPrompt(conf), map[string]interface{}{
		"agent": "task",
		"label": "manager",
		"async": true,
	})
	if err != nil {
		return "", err
	}

	response, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("%s", "manager spawn returned a non-object response")
	}
	details, ok := response["details"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("%s", "manager spawn response lacks details")
	}
	id, ok := details["id"].(string)
	if !ok || id == "" {
		return "", fmt.Errorf("%s", "manager spawn response lacks id")
	}
	return id, nil
}

func shellManifestStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "ok":
		return StatusOK
	case "failed":
		return StatusFailed
	case "timed-out":
		return StatusTimedOut
	case "skipped-missing-cli":
		return StatusSkipped
	default:
		return StatusFailed
	}
}

// shellWaitStatus separates an unrelated-message wakeup (no watched job in
// details) from a real job snapshot. Hub reports "running" on deadline expiry.
func shellWaitStatus(value interface{}, managerID string) string {
	response, ok := value.(map[string]interface{})
	if !ok {
		return ""
	}
	details, ok := response["details"].(map[string]interface{})
	if !ok {
		return ""
	}
	jobs, ok := details["jobs"].([]interface{})
	if !ok {
		return ""
	}
	for _, rawJob := range jobs {
		job, ok := rawJob.(map[string]interface{})
		if !ok {
			continue
		}
		id, _ := job["id"].(string)
		if id != managerID {
			continue
		}
		status, _ := job["status"].(string)
		return status
	}
	return ""
}

// parseManifestTSV accepts the manager's documented six-column manifest. The
// manager-owned exit code, duration, paths, and command stay in its artifacts;
// PanelistResult carries the portable outcome used by the Go orchestrator.
func parseManifestTSV(text string) []PanelistResult {
	var results []PanelistResult
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || strings.HasPrefix(line, "name\tstatus\t") {
			continue
		}
		columns := strings.Split(line, "\t")
		if len(columns) < 2 || strings.TrimSpace(columns[0]) == "" {
			continue
		}
		results = append(results, PanelistResult{
			Name:   columns[0],
			Kind:   KindShell,
			Status: shellManifestStatus(columns[1]),
		})
	}
	return results
}

func shellToolWrite(filePath string, content string) error {
	_, err := omp.Tool("write", map[string]interface{}{
		"path":    filePath,
		"content": content,
	})
	return err
}

func shellToolRead(filePath string) (string, error) {
	value, err := omp.Tool("read", map[string]interface{}{"path": filePath})
	if err != nil {
		return "", err
	}
	response, ok := value.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("%s", "read returned a non-object response")
	}
	text, ok := response["text"].(string)
	if !ok {
		return "", fmt.Errorf("%s", "read response lacks text")
	}
	return text, nil
}

func shellPanelist(conf PanelConfig, name string) (Panelist, bool) {
	for _, p := range conf.Shell {
		if p.Name == name {
			return p, true
		}
	}
	return Panelist{}, false
}

// RunShellPanel writes immutable work orders, then lets one manager own all
// processes. KeepGoing deliberately has no abort semantics: every manifest row
// is returned even when one CLI fails.
func RunShellPanel(conf PanelConfig) ([]PanelistResult, error) {
	dir := shellOutDir(conf)
	for _, p := range conf.Shell {
		if err := shellToolWrite(dir+"/"+p.Name+".prompt", PanelistPrompt(p, conf.Mode, conf.Prompt)); err != nil {
			return nil, fmt.Errorf("write shell prompt for %q: %w", p.Name, err)
		}
	}

	managerID, err := spawnManager(conf)
	if err != nil {
		return nil, fmt.Errorf("spawn shell manager: %w", err)
	}
	deadline := conf.TimeoutSecs
	if deadline <= 0 {
		deadline = 180
	}
	for {
		waitValue, waitErr := omp.Hub("wait", map[string]interface{}{
			"ids":       []interface{}{managerID},
			"timeoutMs": deadline*1000 + 60000,
		})
		if waitErr != nil {
			return nil, fmt.Errorf("wait for shell manager: %w", waitErr)
		}
		status := shellWaitStatus(waitValue, managerID)
		if status != "" && status != "running" {
			break
		}
		if status == "running" {
			_, _ = omp.Hub("cancel", map[string]interface{}{"ids": []interface{}{managerID}})
			return nil, fmt.Errorf("%s", "shell manager exceeded its join deadline")
		}
		// Hub can wake for an unrelated incoming message. Re-issue the same
		// one-job wait; the absence of time in Yaegi is why the host deadline
		// remains owned by this individual Hub call.
	}

	manifest, err := shellToolRead(dir + "/manifest.tsv")
	if err != nil {
		return nil, fmt.Errorf("read shell manifest: %w", err)
	}
	results := parseManifestTSV(manifest)
	for i := range results {
		if p, ok := shellPanelist(conf, results[i].Name); ok {
			results[i].AgentType = p.AgentType
			results[i].CLI = p.CLI
		}
		output, readErr := shellToolRead(dir + "/" + results[i].Name + ".out")
		if readErr != nil {
			results[i].Output = "-"
		} else {
			results[i].Output = output
		}
	}
	return results, nil
}
