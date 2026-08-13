
// consensus: an agentic-consensus panel implemented as Go orchestration code
// for the OMP Yaegi eval kernel (experiment; see README.md).
//
// This file is the shared contract: types, mode preambles, defaults, lens
// assignment, and pure string helpers. It performs no I/O — the kernel's
// stdlib allowlist excludes os/time, so every file/hub/tool interaction goes
// through omp.Tool / omp.Agent / omp.Hub (see orchestrator.go, shellout.go).
//
// Loading: the kit is four ordered fragments (panel.go -> shellout.go ->
// orchestrator.go -> driver.go) that must be executed as ONE composite cell.
// The omp bridge identity is per-cell: a function that calls omp.* only works
// while its own cell is executing, so cross-cell function calls would hit a
// dead capability. Concatenate the fragments in order, append a RunDriver
// invocation, and eval once. Non-host imports live here (they register once
// per kernel session); later fragments use them without importing.
//
// Yaegi constraints honored here: no generics, no goroutines, no os, no time;
// only the allowlisted stdlib (bytes, encoding/base64, encoding/json, errors,
// fmt, io, math, path, regexp, sort, strconv, strings, unicode, utf8) plus the
// injected `omp` facade.

import (
	"encoding/json"
	"path"
	"strings"
)

var _ = json.Marshal
var _ = path.Join

// Mode is the consensus thinking mode. The preambles below are ported
// verbatim from the agentic-consensus runner script so behavior matches.
type Mode string

const (
	ModeConvergent Mode = "convergent"
	ModeDivergent  Mode = "divergent"
	ModeLateral    Mode = "lateral"
)

// Panelist statuses, mirroring the runner script's manifest statuses where
// they apply; native agents add spawn-failed.
const (
	StatusOK        = "ok"
	StatusFailed    = "failed"
	StatusTimedOut  = "timed-out"
	StatusSkipped   = "skipped-missing-cli"
	StatusSpawnFail = "spawn-failed"
)

// PanelistKind distinguishes native OMP agents from shelled-out CLIs.
type PanelistKind string

const (
	KindNative PanelistKind = "native" // spawned via omp.AgentWith (native omp config)
	KindShell  PanelistKind = "shell"  // external CLI, run by the manager agent
)

// Panelist is one panel member. For KindNative, AgentType selects the OMP
// agent roster (task|scout|reviewer|designer|security-reviewer|librarian|sonic);
// diversity across agent types is the native equivalent of the runner script's
// model diversity. For KindShell, CLI is one of codex|devin|claude|cursor|opencode
// and Model is an optional CLI model override ("" = configured default).
type Panelist struct {
	Name      string
	Kind      PanelistKind
	AgentType string
	CLI       string
	Model     string
	Lens      string // divergent lens; assigned by AssignLenses when configured
	Extra     string // panelist-specific context appended to the base prompt
}

// PanelConfig describes one consensus run. Prompt is the task; ContextFile,
// when set, is loaded from disk into a variable (the RLM pattern) and its
// content is appended as context. Native and Shell panels may be empty; an
// empty Synthesizer means structural assembly of the Synthesis struct.
type PanelConfig struct {
	Mode         Mode
	Prompt       string
	ContextFile  string
	CWD          string
	OutDir       string
	Native       []Panelist
	Shell        []Panelist
	Synthesizer  *Panelist
	Deliberative bool // peer-digest rebuttal round for native panelists
	TimeoutSecs  int  // overall run deadline; default 180
	KeepGoing    bool
	Lenses       []string
}

// PanelistResult is one panelist's outcome after the run. For shell
// panelists, CLI carries the CLI name (codex|devin|...); AgentType stays
// empty. For native panelists, AgentType carries the omp agent type.
type PanelistResult struct {
	Name      string
	Kind      PanelistKind
	Status    string
	AgentType string
	CLI       string
	AgentID   string
	Output    string
}

// Synthesis mirrors the skill's synthesis template.
type Synthesis struct {
	Panel          []string
	Mode           Mode
	Consensus      []string
	Dissent        []string
	UniqueFindings []string
	LowConfidence  []string
	Recommendation string
	RawOutputs     string
}

// RunResult is the full outcome of a panel run.
type RunResult struct {
	Results   []PanelistResult
	Synthesis Synthesis
	OutDir    string
	Manifest  string // TSV text
}

// ModePreamble returns the thinking-mode preamble injected ahead of the base
// prompt, ported from the agentic-consensus runner script.
func ModePreamble(m Mode) string {
	switch m {
	case ModeDivergent:
		return "MODE: DIVERGENT — expand the option space, do not converge.\n\n" +
			"You are one member of an independent agentic consensus panel in divergent mode.\n" +
			"Your job is to maximize the number of distinct, well-formed options or framings\n" +
			"you return. Do not seek agreement, do not collapse to a verdict, and do not rank\n" +
			"options as if choosing. Contradictory options are a feature: each should be\n" +
			"internally coherent and genuinely different from the others. Prefer breadth,\n" +
			"novelty, and sharply separated alternatives over a single polished answer.\n\n" +
			"Return a numbered list of distinct options/framings, each with the core idea,\n" +
			"why it is genuinely different from the others, and its sharpest trade-off or\n" +
			"failure mode. End with the dimensions along which these options differ."
	case ModeLateral:
		return "MODE: LATERAL — break the frame.\n\n" +
			"You are one member of an independent agentic consensus panel in lateral mode.\n" +
			"Your job is to find the hidden assumption or default frame that everyone else is\n" +
			"taking for granted and break it. Do not accept the question as posed. Identify\n" +
			"the implicit constraint, invert or sidestep it, and import a concrete analogy\n" +
			"from a distant domain if it sharpens the point.\n\n" +
			"Return: (1) the frame or assumption you rejected; (2) the reframed question or\n" +
			"alternative frame; (3) what that reframe would change in practice; (4) the\n" +
			"sharpest objection to your own reframe."
	default: // ModeConvergent
		return "MODE: CONVERGENT — decide.\n\n" +
			"You are one member of an independent agentic consensus panel in convergent mode.\n" +
			"Return a clear verdict or recommendation, the strongest supporting findings,\n" +
			"the dissent you are aware of, risks/edge cases, your evidence or assumptions,\n" +
			"and a confidence level. Prioritize decision-useful output over breadth."
	}
}

// PanelistPrompt builds the exact prompt sent to one panelist: mode preamble,
// lens orientation (when assigned), the base task, and panelist-specific extra
// context. Lenses orient a panelist; they do not confine them.
func PanelistPrompt(p Panelist, m Mode, base string) string {
	var b strings.Builder
	b.WriteString(ModePreamble(m))
	if p.Lens != "" {
		b.WriteString("\n\nLENS: " + p.Lens)
	}
	b.WriteString("\n\n" + base)
	if p.Extra != "" {
		b.WriteString("\n\nAdditional context for you:\n" + p.Extra)
	}
	return b.String()
}

// AssignLenses assigns lenses round-robin across panelists, one lens per
// panelist, so same-family agents do not cluster on one angle. Pass as many
// lenses as panelists for full spread. Repeats the lens list cyclically.
func AssignLenses(ps []Panelist, lenses []string) {
	if len(lenses) == 0 || len(ps) == 0 {
		return
	}
	for i := range ps {
		ps[i].Lens = lenses[i%len(lenses)]
	}
}

// DefaultNativePanel returns a diverse native panel: reviewer, scout, designer,
// task — four agent types with different model roles and tool sets.
func DefaultNativePanel() []Panelist {
	return []Panelist{
		{Name: "reviewer", Kind: KindNative, AgentType: "reviewer"},
		{Name: "scout", Kind: KindNative, AgentType: "scout"},
		{Name: "designer", Kind: KindNative, AgentType: "designer"},
		{Name: "task", Kind: KindNative, AgentType: "task"},
	}
}

// DefaultShellPanel returns the runner script's default external-CLI panel
// (claude intentionally excluded: lower token rate limits).
func DefaultShellPanel() []Panelist {
	return []Panelist{
		{Name: "codex", Kind: KindShell, CLI: "codex"},
		{Name: "devin", Kind: KindShell, CLI: "devin"},
		{Name: "cursor", Kind: KindShell, CLI: "cursor"},
		{Name: "opencode", Kind: KindShell, CLI: "opencode"},
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// TSVRow renders one result as a manifest row. The native rows carry no exit
// code or duration — those are host-owned for omp agents; empty fields are
// rendered as "-" for stable column count.
func (r PanelistResult) TSVRow() string {
	kind := string(r.Kind)
	if r.AgentType == "" && r.CLI == "" {
		kind = "-"
	}
	agent := r.AgentType
	if agent == "" {
		agent = r.CLI
	}
	if agent == "" {
		agent = "-"
	}
	return strings.Join([]string{
		r.Name,
		r.Status,
		kind,
		agent,
		orDash(r.AgentID),
	}, "\t")
}

// ManifestTSV renders the full manifest. Header matches the runner script's
// manifest.tsv columns where they apply; exit_code and duration_seconds are
// host-owned for native agents, so the columns differ deliberately (see
// README.md "Divergences").
func (rr RunResult) ManifestTSV() string {
	var b strings.Builder
	b.WriteString("agent\tstatus\tkind\tagent_type\tagent_id\n")
	for _, r := range rr.Results {
		b.WriteString(r.TSVRow())
		b.WriteString("\n")
	}
	return b.String()
}

// JoinOutputs concatenates the collected outputs for synthesis, one panelist
// per section.
func JoinOutputs(ps []Panelist, results []PanelistResult) string {
	var b strings.Builder
	for _, r := range results {
		label := r.Name
		if r.AgentType != "" {
			label += " [" + r.AgentType + "]"
		} else if r.AgentID != "" {
			label += " [" + r.AgentID + "]"
		}
		b.WriteString("### " + label + " (" + r.Status + ")\n")
		b.WriteString(orDash(r.Output))
		b.WriteString("\n\n")
	}
	return b.String()
}
