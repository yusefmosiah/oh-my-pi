
// driver.go is the entry fragment for the consensus experiment. Concatenate
// panel.go -> shellout.go -> orchestrator.go -> driver.go (+ your RunDriver
// invocation) into ONE eval cell: the omp bridge is per-cell, so functions
// using omp must execute in the cell that defined them.
// No imports: fmt is host-injected; strings is registered by panel.go.

// Example configuration. The invocation must be declaration-form: define a
// free function and trigger it with a package-level var initializer. File-scope
// statements break the fmt/omp rewrite (the parse candidate that enables it is
// declaration-only), and method declarations cannot live inside the alternative
// func-wrapped candidate:
//
//	func runConsensus() {
//		panel := DefaultNativePanel()
//		AssignLenses(panel, []string{"newcomer", "architect", "skeptic"})
//		result, err := RunDriver(PanelConfig{
//			Mode:        ModeDivergent,
//			Prompt:      "Review the proposed change and recommend a path forward.",
//			Native:      panel,
//			Synthesizer: &Panelist{Name: "synthesizer", Kind: KindNative, AgentType: "task"},
//			Lenses:      []string{"newcomer", "architect", "skeptic"},
//		})
//		fmt.Println("err:", err, "| results:", len(result.Results))
//	}
//
//	var _ = runConsensus()
//
// RunDriver demonstrates the RLM context-as-variable pattern before delegating
// orchestration and artifact writing to RunPanel.
func RunDriver(conf PanelConfig) (RunResult, error) {
	if conf.ContextFile != "" {
		value, err := omp.Tool("read", map[string]interface{}{"path": conf.ContextFile})
		if err != nil {
			return RunResult{}, fmt.Errorf("read consensus context %q: %w", conf.ContextFile, err)
		}

		response, ok := value.(map[string]interface{})
		if !ok {
			return RunResult{}, fmt.Errorf("read consensus context %q: unexpected response", conf.ContextFile)
		}
		context, ok := response["text"].(string)
		if !ok {
			return RunResult{}, fmt.Errorf("read consensus context %q: missing text", conf.ContextFile)
		}

		conf.Prompt += "\n\nContext:\n" + context
		// RunPanel also supports ContextFile. Clearing it prevents a duplicate read
		// and duplicate prompt context after the driver has materialized the value.
		conf.ContextFile = ""
	}

	result, err := RunPanel(conf)
	if err != nil {
		return result, err
	}

	statuses := make([]string, 0, len(result.Results))
	for _, panelist := range result.Results {
		statuses = append(statuses, panelist.Name+"="+panelist.Status)
	}
	recommendation := strings.ReplaceAll(result.Synthesis.Recommendation, "\n", " ")
	fmt.Println("Agentic consensus:", strings.Join(statuses, ", "), "| recommendation:", recommendation)

	return result, nil
}
