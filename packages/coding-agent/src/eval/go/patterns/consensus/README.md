# Go agentic-consensus panel experiment

This is an experiment that internalizes the `agentic-consensus` skill as Go
orchestration code running in the OMP Yaegi eval kernel. A panel of diverse
agents considers one prompt in **convergent**, **divergent**, or **lateral**
mode; lenses distribute perspectives; synthesis follows the consensus template.
One Go driver acts as the main agent. Native subagents are started through
`omp.AgentWith`; external CLIs are handled by a dedicated manager agent.

## Quick start

The kit is four ordered fragments that must be executed as **one composite
cell**: the omp bridge identity is per-cell, so a function that calls `omp.*`
only works while its own cell is executing. Concatenate the fragments in order,
append a declaration-form invocation, and eval once:

```text
panel.go -> shellout.go -> orchestrator.go -> driver.go
```

```sh
cat panel.go shellout.go orchestrator.go driver.go > cell.go
# append your invocation to cell.go (see driver.go's example), then eval cell.go
```

The invocation must be declaration-form: a free function plus a package-level
`var _ = runConsensus()` initializer (see the copyable example in `driver.go`).
File-scope statements would force the runner's rewrite fallback, leaving `fmt`
and `omp` unresolved.

### Substrate constraints

- **The omp bridge is per-cell**: `omp.*` calls only work from the cell being
  executed (each cell's execution goes inactive when it finishes). Cross-cell
  function calls that use omp hit a dead capability. Hence: one composite cell.
- A cell cannot contain forward references: define helpers before their callers
  (bottom-up). Cross-cell forward references fail the same way.
- Non-host stdlib packages (`strings`, `encoding/json`, `path`) are imported
  once in `panel.go` and reused without imports in later fragments — a package
  import registers once per kernel session, and later imports fail with
  "redeclared in this block".
- Do not import `fmt` in the fragments: multiple `import "fmt"` declarations in
  one composite break the runner's import stripping. `fmt` usage is rewritten
  by the runner when the cell parses as declarations.
- `import "errors"` is broken in the runner allowlist; use `fmt.Errorf`
  instead.

The eval runner binary is `omp-eval-go-runner`. Build it from
`packages/coding-agent/src/eval/go/runner` with:

```sh
go build -trimpath -buildvcs=false -o omp-eval-go-runner .
```

## Verified broker mechanics

| Need | Broker operation | Verified behavior |
| --- | --- | --- |
| Spawn a native panelist | `omp.AgentWith(prompt, map[string]interface{}{"agent": typ, "label": name, "async": true})` | Returns a map whose `details.id` is both the agent ID and job ID. |
| Join asynchronous panelists | `omp.Hub("wait", map[string]interface{}{"ids": []interface{}{...}, "timeoutMs": ms})` | Event-based, not a global join: it returns on the FIRST settled job, an unrelated incoming IRC message (response has no `jobs` key — only `details.waited`), the window expiring, or abort. Re-issue for unsettled IDs. |
| Read a settled result | `omp.Tool("read", map[string]interface{}{"path": "agent://<id>"})` | Returns a map with output in `text` (or `displayContent.text`). |
| Cancel an outstanding panelist | `omp.Hub("cancel", map[string]interface{}{"ids": []interface{}{...}})` | Cancels the listed agent/job IDs. |
| Run the synthesizer inline | `omp.AgentWith(prompt, map[string]interface{}{"async": false})` | Returns the final output directly. |

### Join protocol (why `collectNative` re-waits)

`hub wait` responses come in three shapes, and the kit must classify them:

- **Settled snapshot** — `details.jobs` with terminal statuses: settle those IDs.
- **Partial snapshot** — `details.jobs` mixing terminal and still-`running` jobs:
  settle only the terminal ones and re-wait for the rest.
- **Message wake** — a response with NO `jobs` key (`details.waited`): an
  unrelated IRC message satisfied the wait. Re-wait with the same window —
  this is never a deadline.
- **Window expiry** — a snapshot whose watched jobs are ALL still `running`:
  cancel the stragglers and report `timed-out`.

Treating a message wake as a deadline is a real failure mode (it cancels
healthy panelists), which is why `terminalJobStatus` + the empty-jobs re-wait
branch exist.

### Deadlines and transport caps

`TimeoutSecs` is the per-`hub wait` window, not a wall-clock run budget
(the kernel excludes `time`, so cells cannot track elapsed time). Set it
generously — longer than the slowest panelist — or panelists get cancelled
mid-thought. Panelists have reported a hard transport cap on individual
bridge calls; keep waits bounded (well under that cap) and rely on the
re-wait loop rather than one long wait.

## Design notes

### Host-mediated concurrency

The goroutine ban does not prevent orchestration concurrency. The cell starts
agents asynchronously through `omp.AgentWith` and joins synchronously through
`omp.Hub`; the host owns the concurrent work. In-cell goroutines would escape
host cancellation, caps, and auto-delivery. Routing concurrency through the
substrate is therefore the mechanism collective, deliberative, and critical
strategies need rather than a restriction to work around.

### RLM mapping

`ContextFile` is read from the filesystem into a Go variable and appended to
the panel prompt: the context-as-variable pattern. The run artifacts provide
the complementary result-as-variable pattern: subsequent cells or agents can
consume the persisted prompt, manifest, synthesis, and panel outputs.

### Code and tool calls

The Go cells contain deterministic orchestration skeletons. Agents retain
judgment over adaptive strategy: their analysis, disagreement, and synthesis
are not hard-coded. This keeps repeatable mechanics in code while leaving the
reasoning appropriate to the panelists.

## Divergences from the bash runner

- Native agents have no `exit_code` or `duration_seconds` manifest columns:
  those values are host-owned.
- The manifest column shape consequently differs where those native-only fields
  would otherwise appear.
- Prompts are Go string constants. Yaegi cells cannot import `.md` files, so
  this is a deliberate deviation from the repository prompt policy.
- The deliberative peer-digest round is native-only in this first version.
- There is no in-cell timing because `time` is excluded from the eval-kernel
  standard-library allowlist.

## Relationship to Choir

In `~/go-choir`, the durable home for this pattern is
`internal/actor`'s durable `Update` flow together with
`internal/agentprofile`'s `CanSpawn` and `CanMessage` policies. This kit is the
throwaway experiment substrate for testing the orchestration pattern before any
such durable integration.

## Limitations

- The kit relies on the OMP host for all I/O, agent lifecycle operations, and
  concurrency; cells do not use filesystem APIs, goroutines, or in-cell time.
- Native panel results intentionally expose host-visible status and output, not
  host-owned process exit codes or durations.
- External CLI panelists are not launched directly by the driver; they are
  delegated to the dedicated manager agent.
- Deliberation is available only for settled native panelists in this version.
- Without a synthesizer panelist, the panel produces structural synthesis
  rather than an LLM-authored synthesis.
