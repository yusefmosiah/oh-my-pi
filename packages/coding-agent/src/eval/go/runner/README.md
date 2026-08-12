# OMP Yaegi eval helper

`main.go` is a persistent, stdin/stdout NDJSON runner. It embeds Yaegi and exposes
only the `omp/omp` facade through Yaegi symbols:

- `omp.Tool(name, map[string]interface{})`
- `omp.Agent(prompt)` / `omp.AgentWith(prompt, options)`
- `omp.Hub(op, map[string]interface{})`

`omp.Agent` and `omp.AgentWith` spawn background agents by default and return
their agent/job metadata immediately. Use `omp.Hub("list", nil)` to discover
the live agent ID, then `omp.Hub("send", ...)` to coordinate with it. Pass
`map[string]interface{}{"async": false}` through `AgentWith` when an inline
result is explicitly needed.

The facade calls OMP's loopback capability broker using a per-cell session/run identity. The Bun host keeps the authenticated bearer and the retained helper never receives it. It never imports or exposes OMP's Bun objects, agent registry, or IRC bus. Protocol stdout is reserved for JSON frames; user
`fmt.Print`/`fmt.Println` output is framed as `stdout`.

Build locally with:

```sh
go build -trimpath -buildvcs=false -o omp-eval-go-runner .
```

Yaegi is an interpreter, not a security sandbox. This helper deliberately exposes
only a narrow deterministic standard-library allowlist, blocks goroutine syntax,
uses an EOF stdin, and keeps bridge credentials out of its post-startup process
environment; it still must not be advertised as hostile-code isolation.
