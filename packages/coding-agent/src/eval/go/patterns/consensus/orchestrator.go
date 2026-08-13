
// orchestrator.go runs the native side of a consensus panel. The broker owns
// concurrency: agents are spawned asynchronously, then this cell synchronously
// joins them through Hub wait calls. No local goroutines or clocks are needed.
// No imports: fmt is host-injected; strings/encoding/json/path registered by panel.go.

// brokerDetails and brokerValue describe the JSON-shaped values returned by the
// injected facade. Keeping this conversion at the boundary avoids depending on
// Go map assertions throughout the orchestration code.
type brokerDetails struct {
	ID             string `json:"id"`
	ResultText     string `json:"resultText"`
	Text           string `json:"text"`
	DisplayContent struct {
		Text string `json:"text"`
	} `json:"displayContent"`
}

type brokerValue struct {
	ID             string        `json:"id"`
	Text           string        `json:"text"`
	Output         string        `json:"output"`
	ResultText     string        `json:"resultText"`
	Details        brokerDetails `json:"details"`
	DisplayContent struct {
		Text string `json:"text"`
	} `json:"displayContent"`
}

type brokerJob struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	ResultText string `json:"resultText"`
}

type brokerWait struct {
	Messages []interface{} `json:"messages"`
	Details  struct {
		Jobs     []brokerJob   `json:"jobs"`
		Messages []interface{} `json:"messages"`
	} `json:"details"`
}

// jobIDFrom extracts the nested async-agent id without assuming a concrete map
// implementation from the JSON-decoded OMP facade.
func jobIDFrom(v interface{}) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	var value brokerValue
	if json.Unmarshal(encoded, &value) != nil {
		return ""
	}
	if value.Details.ID != "" {
		return value.Details.ID
	}
	return value.ID
}

// jobOutput extracts text from direct, job, and tool-read facade responses.
func jobOutput(v interface{}) string {
	encoded, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	var text string
	if json.Unmarshal(encoded, &text) == nil {
		return text
	}
	var value brokerValue
	if json.Unmarshal(encoded, &value) != nil {
		return ""
	}
	for _, candidate := range []string{
		value.Text, value.Output, value.ResultText, value.Details.Text,
		value.Details.ResultText, value.DisplayContent.Text, value.Details.DisplayContent.Text,
	} {
		if candidate != "" {
			return candidate
		}
	}
	return ""
}
func settledJobs(v interface{}) []brokerJob {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var value brokerWait
	if json.Unmarshal(encoded, &value) != nil {
		return nil
	}
	return value.Details.Jobs
}

func readAgentOutput(id string) string {
	value, err := omp.Tool("read", map[string]interface{}{"path": "agent://" + id})
	if err != nil {
		return ""
	}
	return jobOutput(value)
}
func readConsensusContext(contextFile string) (string, error) {
	if contextFile == "" {
		return "", nil
	}
	value, err := omp.Tool("read", map[string]interface{}{"path": contextFile})
	if err != nil {
		return "", fmt.Errorf("read consensus context %q: %w", contextFile, err)
	}
	context := jobOutput(value)
	return context, nil
}
func effectiveAgentType(p Panelist) string {
	if p.AgentType == "" {
		return "task"
	}
	return p.AgentType
}
func failedNativeResult(p Panelist, id string, message string) PanelistResult {
	return PanelistResult{
		Name: p.Name, Kind: KindNative, Status: StatusFailed,
		AgentType: effectiveAgentType(p), AgentID: id, Output: message,
	}
}

func nativePanelistsWithIDs(results []PanelistResult) []Panelist {
	panelists := make([]Panelist, 0, len(results))
	for _, result := range results {
		if result.AgentID != "" {
			panelists = append(panelists, Panelist{
				Name: result.Name, Kind: KindNative, AgentType: result.AgentType,
			})
		}
	}
	return panelists
}
func assignPanelLenses(conf *PanelConfig) {
	panelists := append([]Panelist{}, conf.Native...)
	panelists = append(panelists, conf.Shell...)
	AssignLenses(panelists, conf.Lenses)
	for i := range conf.Native {
		conf.Native[i].Lens = panelists[i].Lens
	}
	for i := range conf.Shell {
		conf.Shell[i].Lens = panelists[len(conf.Native)+i].Lens
	}
}
func panelNames(conf PanelConfig) []string {
	names := make([]string, 0, len(conf.Native)+len(conf.Shell))
	for _, p := range conf.Native {
		names = append(names, p.Name)
	}
	for _, p := range conf.Shell {
		names = append(names, p.Name)
	}
	return names
}
func otherPanelDigest(results []PanelistResult, targetID string) string {
	var b strings.Builder
	for _, result := range results {
		if result.AgentID == targetID {
			continue
		}
		b.WriteString("## ")
		b.WriteString(result.Name)
		b.WriteString(" (")
		b.WriteString(result.Status)
		b.WriteString(")\n")
		b.WriteString(result.Output)
		b.WriteString("\n\n")
	}
	return b.String()
}
func synthesisLines(reply string, heading string) []string {
	lines := strings.Split(reply, "\n")
	collecting := false
	var result []string
	needle := strings.ToLower(heading)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			title := strings.ToLower(strings.TrimSpace(strings.TrimLeft(trimmed, "#")))
			if collecting {
				break
			}
			collecting = strings.Contains(title, needle)
			continue
		}
		if collecting && trimmed != "" {
			result = append(result, strings.TrimSpace(strings.TrimLeft(trimmed, "-* ")))
		}
	}
	return result
}
func renderSynthesisList(b *strings.Builder, values []string) {
	if len(values) == 0 {
		b.WriteString("- None recorded.\n")
		return
	}
	for _, value := range values {
		b.WriteString("- ")
		b.WriteString(value)
		b.WriteString("\n")
	}
}
func renderSynthesis(s Synthesis) string {
	var b strings.Builder
	b.WriteString("# Agentic Consensus\n\n## Panel\n")
	for _, name := range s.Panel {
		b.WriteString("- ")
		b.WriteString(name)
		b.WriteString("\n")
	}
	b.WriteString("\n## Mode\n")
	b.WriteString(string(s.Mode))
	b.WriteString("\n\n## Consensus\n")
	renderSynthesisList(&b, s.Consensus)
	b.WriteString("\n## Dissent / Disagreements\n")
	renderSynthesisList(&b, s.Dissent)
	b.WriteString("\n## Unique High-Value Findings\n")
	renderSynthesisList(&b, s.UniqueFindings)
	b.WriteString("\n## Low-Confidence / Unverified Claims\n")
	renderSynthesisList(&b, s.LowConfidence)
	b.WriteString("\n## Recommendation\n")
	b.WriteString(s.Recommendation)
	b.WriteString("\n\n## Raw Outputs\n")
	b.WriteString(s.RawOutputs)
	return b.String()
}

// spawnNativePanel materializes optional context once, then asks the host to
// start all native agents asynchronously. Successful entries are placeholders;
// collectNative replaces them with settled results after the host-side join.
func spawnNativePanel(conf PanelConfig) ([]PanelistResult, []string, error) {
	results := make([]PanelistResult, 0, len(conf.Native))
	ids := make([]string, 0, len(conf.Native))
	context, contextErr := readConsensusContext(conf.ContextFile)
	if contextErr != nil {
		for _, p := range conf.Native {
			results = append(results, PanelistResult{
				Name: p.Name, Kind: KindNative, Status: StatusSpawnFail,
				AgentType: p.AgentType, Output: contextErr.Error(),
			})
		}
		return results, ids, contextErr
	}

	for _, p := range conf.Native {
		agentType := p.AgentType
		if agentType == "" {
			agentType = "task"
		}
		prompt := PanelistPrompt(p, conf.Mode, conf.Prompt)
		if conf.ContextFile != "" {
			prompt += "\n\nContext:\n" + context
		}
		value, err := omp.AgentWith(prompt, map[string]interface{}{
			"agent": agentType,
			"label": p.Name,
			"async": true,
		})
		id := jobIDFrom(value)
		if err != nil || id == "" {
			message := "agent spawn returned no id"
			if err != nil {
				message = err.Error()
			}
			results = append(results, PanelistResult{
				Name: p.Name, Kind: KindNative, Status: StatusSpawnFail,
				AgentType: agentType, Output: message,
			})
			continue
		}
		results = append(results, PanelistResult{
			Name: p.Name, Kind: KindNative, Status: StatusOK,
			AgentType: agentType, AgentID: id,
		})
		ids = append(ids, id)
	}
	return results, ids, nil
}

// terminalJobStatus reports whether a job snapshot status means the job is no
// longer running. "running" and "pending" stay pending; anything else
// (completed, ok, success, failed, cancelled, timed-out, error, ...) settles.
func terminalJobStatus(status string) bool {
	switch status {
	case "", "running", "pending":
		return false
	default:
		return true
	}
}

// collectNative synchronously joins host-owned jobs. The broker applies the
// configured wait deadline. Response shapes: a settled or window-expired wait
// returns the watched-jobs snapshot (details.jobs, possibly still "running");
// a wait woken by an unrelated incoming IRC message returns a message result
// with NO jobs key. So: empty jobs ⇒ re-wait (never a deadline); a snapshot
// whose every watched job is still running ⇒ the window elapsed ⇒ cancel and
// report timed-out; terminal jobs ⇒ settle them.
func collectNative(conf PanelConfig, ids []string) ([]PanelistResult, error) {
	results := make([]PanelistResult, 0, len(ids))
	if len(ids) == 0 {
		return results, nil
	}
	panelistByID := make(map[string]Panelist, len(ids))
	for i, id := range ids {
		if i < len(conf.Native) {
			panelistByID[id] = conf.Native[i]
		}
	}
	pending := make(map[string]bool, len(ids))
	for _, id := range ids {
		pending[id] = true
	}
	for len(pending) > 0 {
		waiting := make([]string, 0, len(pending))
		for _, id := range ids {
			if pending[id] {
				waiting = append(waiting, id)
			}
		}
		values := make([]interface{}, 0, len(waiting))
		for _, id := range waiting {
			values = append(values, id)
		}
		value, err := omp.Hub("wait", map[string]interface{}{
			"ids":       values,
			"timeoutMs": conf.TimeoutSecs * 1000,
		})
		if err != nil {
			for _, id := range waiting {
				results = append(results, failedNativeResult(panelistByID[id], id, err.Error()))
			}
			return results, err
		}
		jobs := settledJobs(value)
		if len(jobs) == 0 {
			// Message wake (or no matching jobs): the watched agents are still
			// running and healthy. Re-issue the wait with the same window.
			continue
		}
		anySettled := false
		for _, job := range jobs {
			if !pending[job.ID] {
				continue
			}
			if !terminalJobStatus(job.Status) {
				continue
			}
			delete(pending, job.ID)
			anySettled = true
			p := panelistByID[job.ID]
			output := readAgentOutput(job.ID)
			if output == "" {
				output = job.ResultText
			}
			status := StatusOK
			if job.Status != "completed" && job.Status != "ok" && job.Status != "success" {
				status = StatusFailed
			}
			results = append(results, PanelistResult{
				Name: p.Name, Kind: KindNative, Status: status,
				AgentType: effectiveAgentType(p), AgentID: job.ID, Output: output,
			})
		}
		if !anySettled {
			// Snapshot returned with every watched job still running: the wait
			// window elapsed. Cancel the stragglers and report timed-out.
			_, cancelErr := omp.Hub("cancel", map[string]interface{}{"ids": values})
			for _, id := range waiting {
				p := panelistByID[id]
				output := "host deadline elapsed"
				if cancelErr != nil {
					output += "; cancel failed: " + cancelErr.Error()
				}
				results = append(results, PanelistResult{
					Name: p.Name, Kind: KindNative, Status: StatusTimedOut,
					AgentType: effectiveAgentType(p), AgentID: id, Output: output,
				})
			}
			break
		}
	}
	return results, nil
}

// deliberativeRound asks each completed native panelist to react to the other
// outputs. Hub send with await keeps the round host-mediated and sequential.
func deliberativeRound(conf PanelConfig, results []PanelistResult) ([]PanelistResult, error) {
	for i := range results {
		if results[i].Kind != KindNative || results[i].Status != StatusOK || results[i].AgentID == "" {
			continue
		}
		digest := otherPanelDigest(results, results[i].AgentID)
		value, err := omp.Hub("send", map[string]interface{}{
			"to":      results[i].AgentID,
			"message": "Review the other panelists' outputs below. State only corrections, disagreements, or strengthened conclusions.\n\n" + digest,
			"await":   true,
		})
		if err != nil {
			results[i].Output += "\n\nRound 2:\n[send failed: " + err.Error() + "]"
			continue
		}
		reply := jobOutput(value)
		if reply == "" {
			reply = "[no round-2 reply returned]"
		}
		results[i].Output += "\n\nRound 2:\n" + reply
	}
	return results, nil
}

// synthesize delegates semantic aggregation to an optional inline agent. Its
// raw reply remains available even when a lightweight section parser cannot
// recognize the agent's formatting.
func synthesize(conf PanelConfig, joined string) (Synthesis, error) {
	s := Synthesis{Mode: conf.Mode, Panel: panelNames(conf), RawOutputs: joined}
	if conf.Synthesizer == nil {
		s.Recommendation = "Structural synthesis: run with a Synthesizer panelist for LLM synthesis."
		return s, nil
	}
	agentType := conf.Synthesizer.AgentType
	if agentType == "" {
		agentType = "task"
	}
	prompt := "You are the synthesizer for an agentic consensus panel. Synthesize the panel outputs below into: Consensus (findings agreed by 2+), Dissent, Unique high-value findings, Low-confidence/unverified claims, Recommendation. Be concise.\n\n" + joined
	value, err := omp.AgentWith(prompt, map[string]interface{}{"agent": agentType, "async": false})
	if err != nil {
		s.Recommendation = "Synthesis failed: " + err.Error()
		return s, err
	}
	reply := jobOutput(value)
	if reply == "" {
		err = fmt.Errorf("%s", "synthesizer returned no text")
		s.Recommendation = "Synthesis failed: " + err.Error()
		return s, err
	}
	s.Consensus = synthesisLines(reply, "consensus")
	s.Dissent = synthesisLines(reply, "dissent")
	if len(s.Dissent) == 0 {
		s.Dissent = synthesisLines(reply, "disagreement")
	}
	s.UniqueFindings = synthesisLines(reply, "unique")
	s.LowConfidence = synthesisLines(reply, "low-confidence")
	if len(s.LowConfidence) == 0 {
		s.LowConfidence = synthesisLines(reply, "unverified")
	}
	recommendation := synthesisLines(reply, "recommendation")
	if len(recommendation) == 0 {
		s.Recommendation = reply
	} else {
		s.Recommendation = strings.Join(recommendation, "\n")
	}
	return s, nil
}

// writeArtifacts records the base task, manifest, synthesis, and each raw
// panelist output. omp.Tool write creates the output directory on demand.
func writeArtifacts(conf PanelConfig, rr RunResult) error {
	files := []struct {
		name    string
		content string
	}{
		{"prompt.md", conf.Prompt},
		{"manifest.tsv", rr.Manifest},
		{"synthesis.md", renderSynthesis(rr.Synthesis)},
	}
	for _, file := range files {
		if _, err := omp.Tool("write", map[string]interface{}{
			"path": path.Join(rr.OutDir, file.name), "content": file.content,
		}); err != nil {
			return fmt.Errorf("write consensus artifact %q: %w", file.name, err)
		}
	}
	for _, result := range rr.Results {
		name := strings.ReplaceAll(result.Name, "/", "-")
		if name == "" {
			name = "unnamed"
		}
		if _, err := omp.Tool("write", map[string]interface{}{
			"path": path.Join(rr.OutDir, name+".out"), "content": result.Output,
		}); err != nil {
			return fmt.Errorf("write panel output %q: %w", result.Name, err)
		}
	}
	return nil
}

// RunPanel runs every configured panelist. KeepGoing deliberately does not
// alter control flow: partial outcomes are always collected and returned so the
// caller can decide whether a partial panel is acceptable.
func RunPanel(conf PanelConfig) (RunResult, error) {
	if conf.Mode == "" {
		conf.Mode = ModeConvergent
	}
	if conf.TimeoutSecs <= 0 {
		conf.TimeoutSecs = 180
	}
	if conf.OutDir == "" {
		conf.OutDir = path.Join(".agentic-consensus", "consensus-go-"+string(conf.Mode))
	}
	if len(conf.Lenses) > 0 {
		assignPanelLenses(&conf)
	}

	rr := RunResult{OutDir: conf.OutDir}
	var firstErr error

	spawned, ids, spawnErr := spawnNativePanel(conf)
	if spawnErr != nil {
		firstErr = spawnErr
	}
	liveConf := conf
	liveConf.Native = nativePanelistsWithIDs(spawned)
	for _, result := range spawned {
		if result.AgentID == "" {
			rr.Results = append(rr.Results, result)
		}
	}

	collected, collectErr := collectNative(liveConf, ids)
	rr.Results = append(rr.Results, collected...)
	if firstErr == nil && collectErr != nil {
		firstErr = collectErr
	}
	if conf.Deliberative {
		var roundErr error
		rr.Results, roundErr = deliberativeRound(conf, rr.Results)
		if firstErr == nil && roundErr != nil {
			firstErr = roundErr
		}
	}

	if len(conf.Shell) > 0 {
		shellResults, shellErr := RunShellPanel(conf)
		rr.Results = append(rr.Results, shellResults...)
		if firstErr == nil && shellErr != nil {
			firstErr = shellErr
		}
	}

	panelists := append([]Panelist{}, conf.Native...)
	panelists = append(panelists, conf.Shell...)
	joined := JoinOutputs(panelists, rr.Results)
	synthesis, synthErr := synthesize(conf, joined)
	rr.Synthesis = synthesis
	if firstErr == nil && synthErr != nil {
		firstErr = synthErr
	}
	rr.Manifest = rr.ManifestTSV()
	if artifactErr := writeArtifacts(conf, rr); firstErr == nil && artifactErr != nil {
		firstErr = artifactErr
	}
	return rr, firstErr
}
