// Command omp-eval-go-runner hosts a persistent Yaegi interpreter for OMP's
// eval tool. It intentionally speaks the same small NDJSON kernel protocol as
// the Python/Ruby runners and reaches the Bun host only through the tokenless
// loopback capability broker.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing/fstest"
	"time"

	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
)

type request struct {
	ID            string             `json:"id"`
	Type          string             `json:"type,omitempty"`
	Code          string             `json:"code,omitempty"`
	Cwd           string             `json:"cwd,omitempty"`
	Env           map[string]*string `json:"env,omitempty"`
	BridgeURL     string             `json:"bridgeUrl,omitempty"`
	BridgeSession string             `json:"bridgeSession,omitempty"`
}

type frame struct {
	Type         string   `json:"type"`
	ID           string   `json:"id,omitempty"`
	Data         string   `json:"data,omitempty"`
	Ename        string   `json:"ename,omitempty"`
	Evalue       string   `json:"evalue,omitempty"`
	Traceback    []string `json:"traceback,omitempty"`
	Status       string   `json:"status,omitempty"`
	Cancelled    bool     `json:"cancelled,omitempty"`
	ExecutionCnt int      `json:"executionCount,omitempty"`
}

type execution struct {
	mu     sync.RWMutex
	id     string
	active bool
}

type frameWriter struct {
	runner  *runner
	kind    string
	current func() *execution
}

const maxFrameDataBytes = 1 << 20

func (w frameWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	originalLen := len(p)
	// Keep one user write from forcing an unbounded protocol frame. The host
	// output sink applies its own aggregate truncation policy.
	for len(p) > 0 {
		n := len(p)
		if n > maxFrameDataBytes {
			n = maxFrameDataBytes
		}
		exec := w.current()
		if exec != nil {
			exec.mu.RLock()
			active, id := exec.active, exec.id
			exec.mu.RUnlock()
			if active && id != "" && w.runner.currentExecution() == exec {
				w.runner.emit(frame{Type: w.kind, ID: id, Data: string(p[:n])})
			}
		}
		p = p[n:]
	}
	return originalLen, nil
}

type runner struct {
	encoder  *json.Encoder
	outMu    sync.Mutex
	stateMu  sync.Mutex
	id       string
	run      string
	cancel   context.CancelFunc
	bridge   *bridgeCapability
	poisoned bool
	// top-level names declared by prior cells. They can shadow the injected
	// fmt/omp facades and must be honored by later rewrites.
	shadowed map[string]bool
	exec     *execution
}

func (r *runner) currentExecution() *execution {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	return r.exec
}

func (r *runner) setExecution(exec *execution, cancel context.CancelFunc, bridge *bridgeCapability) {
	r.stateMu.Lock()
	r.id = exec.id
	r.cancel = cancel
	r.bridge = bridge
	r.exec = exec
	r.stateMu.Unlock()
}

func (r *runner) clearExecution(exec *execution, bridge *bridgeCapability) {
	exec.mu.Lock()
	exec.active = false
	exec.mu.Unlock()
	r.stateMu.Lock()
	if r.bridge == bridge && r.exec == exec {
		r.id = ""
		r.cancel = nil
		r.bridge = nil
		r.exec = nil
	}
	r.stateMu.Unlock()
}

func (r *runner) interrupt() {
	r.stateMu.Lock()
	cancel := r.cancel
	r.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *runner) emit(value frame) {
	r.outMu.Lock()
	defer r.outMu.Unlock()
	_ = r.encoder.Encode(value)
}

// The host capability exposed to interpreted Go is immutable per execution.
// A retained function therefore keeps the run id and cancellation context from
// its originating cell; it can never be retagged with a later cell's identity.
const bridgeTransportTimeout = 5 * time.Minute

type bridgeClient struct {
	client *http.Client
}

type bridgeCapability struct {
	baseURL string
	session string
	run     string
	ctx     context.Context
	client  *http.Client
	exec    *execution
}

func newBridgeClient() *bridgeClient {
	// Each eval request supplies only loopback bridge identity. The host-side bearer
	// never enters the helper's startup environment, user code, or child processes.
	return &bridgeClient{client: &http.Client{Timeout: bridgeTransportTimeout}}
}

func validBridgeURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host := u.Hostname()
	if host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback() && (u.Path == "" || u.Path == "/" || u.Path == "/v1/tool" || u.Path == "/v1/eval-tool")
}

func bridgeEndpoint(raw string) string {
	trimmed := strings.TrimRight(raw, "/")
	// Retained Go cells always use the host-side tokenless capability broker.
	// Normalize the historical root and authenticated paths rather than allowing
	// a request field to select the bearer-protected endpoint.
	if strings.HasSuffix(trimmed, "/v1/eval-tool") {
		return trimmed
	}
	if strings.HasSuffix(trimmed, "/v1/tool") {
		return strings.TrimSuffix(trimmed, "/v1/tool") + "/v1/eval-tool"
	}
	return trimmed + "/v1/eval-tool"
}

func (b *bridgeClient) capability(baseURL, session, run string, ctx context.Context, exec *execution) *bridgeCapability {
	if !validBridgeURL(baseURL) || session == "" || run == "" {
		baseURL, session, run = "", "", ""
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &bridgeCapability{
		baseURL: strings.TrimRight(baseURL, "/"),
		session: session,
		run:     run,
		ctx:     ctx,
		client:  b.client,
		exec:    exec,
	}
}

func (b *bridgeCapability) call(name string, args map[string]interface{}) (interface{}, error) {
	if b.baseURL == "" || b.session == "" {
		return nil, errors.New("OMP tool bridge is unavailable in this kernel")
	}
	if b.exec != nil {
		b.exec.mu.RLock()
		active := b.exec.active
		b.exec.mu.RUnlock()
		if !active {
			return nil, errors.New("OMP tool bridge is unavailable in this kernel")
		}
	}
	if b.run == "" {
		return nil, errors.New("OMP tool bridge is not active outside an eval cell")
	}
	if args == nil {
		args = map[string]interface{}{}
	}
	payload, err := json.Marshal(map[string]interface{}{
		"session": b.session,
		"run":     b.run,
		"name":    name,
		"args":    args,
	})
	if err != nil {
		return nil, fmt.Errorf("encode bridge call %q: %w", name, err)
	}
	req, err := http.NewRequestWithContext(b.ctx, http.MethodPost, bridgeEndpoint(b.baseURL), strings.NewReader(string(payload)))
	if err != nil {
		return nil, fmt.Errorf("create bridge call %q: %w", name, err)
	}
	// Authentication is held by the Bun host; retained Go never receives a bearer.
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("bridge call %q: %w", name, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, fmt.Errorf("read bridge call %q: %w", name, err)
	}
	var envelope struct {
		OK    bool            `json:"ok"`
		Value json.RawMessage `json:"value"`
		Error string          `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("bridge call %q returned invalid JSON: %w", name, err)
	}
	if !envelope.OK {
		if envelope.Error == "" {
			envelope.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return nil, errors.New(envelope.Error)
	}
	if len(envelope.Value) == 0 || string(envelope.Value) == "null" {
		return nil, nil
	}
	var value interface{}
	if err := json.Unmarshal(envelope.Value, &value); err != nil {
		return nil, fmt.Errorf("decode bridge result %q: %w", name, err)
	}
	return value, nil
}

func (b *bridgeCapability) tool(name string, args map[string]interface{}) (interface{}, error) {
	if args == nil {
		args = map[string]interface{}{}
	}
	if _, ok := args["i"]; !ok {
		args["i"] = "go prelude"
	}
	return b.call(name, args)
}

func (b *bridgeCapability) agent(prompt string) (interface{}, error) {
	return b.call("__agent__", map[string]interface{}{"prompt": prompt, "i": "go prelude", "async": true})
}

func (b *bridgeCapability) agentWith(prompt string, options map[string]interface{}) (interface{}, error) {
	args := map[string]interface{}{"prompt": prompt, "i": "go prelude", "async": true}
	for key, value := range options {
		args[key] = value
	}
	return b.call("__agent__", args)
}

func (b *bridgeCapability) hub(op string, args map[string]interface{}) (interface{}, error) {
	params := map[string]interface{}{"op": op, "i": "go prelude"}
	for key, value := range args {
		params[key] = value
	}
	return b.call("hub", params)
}

func symbolsFor(packagePath string, client *bridgeCapability) interp.Exports {
	return interp.Exports{packagePath + "/omp": {
		"Agent":     reflect.ValueOf(client.agent),
		"AgentWith": reflect.ValueOf(client.agentWith),
		"Hub":       reflect.ValueOf(client.hub),
		"Tool":      reflect.ValueOf(client.tool),
	}}
}

func fmtSymbols(packagePath string, writer io.Writer) interp.Exports {
	symbols := make(map[string]reflect.Value, len(stdlib.Symbols["fmt/fmt"]))
	for name, value := range stdlib.Symbols["fmt/fmt"] {
		symbols[name] = value
	}
	symbols["Print"] = reflect.ValueOf(func(a ...interface{}) (int, error) { return fmt.Fprint(writer, a...) })
	symbols["Printf"] = reflect.ValueOf(func(format string, a ...interface{}) (int, error) {
		return fmt.Fprintf(writer, format, a...)
	})
	symbols["Println"] = reflect.ValueOf(func(a ...interface{}) (int, error) { return fmt.Fprintln(writer, a...) })
	stdin := strings.NewReader("")
	symbols["Scan"] = reflect.ValueOf(func(a ...interface{}) (int, error) { return fmt.Fscan(stdin, a...) })
	symbols["Scanf"] = reflect.ValueOf(func(format string, a ...interface{}) (int, error) {
		return fmt.Fscanf(stdin, format, a...)
	})
	symbols["Scanln"] = reflect.ValueOf(func(a ...interface{}) (int, error) { return fmt.Fscanln(stdin, a...) })
	return interp.Exports{packagePath + "/fmt": symbols}
}

type rewriteSpan struct {
	start, end int
	text       string
}

func rewriteCellImports(source, fmtAlias, ompAlias string, persistentShadowed ...map[string]bool) (string, error) {
	// Cells can combine imports and statements, so use Go's lexer rather than
	// line/Fields parsing. This handles parenthesized imports, grouped imports,
	// semicolons, and comments between an alias and its path.
	type scanned struct {
		tok        token.Token
		lit        string
		start, end int
	}
	fileSet := token.NewFileSet()
	file := fileSet.AddFile("cell.go", fileSet.Base(), len(source))
	var scan scanner.Scanner
	scan.Init(file, []byte(source), nil, scanner.ScanComments)
	var tokens []scanned
	for {
		pos, tok, lit := scan.Scan()
		if tok == token.EOF {
			break
		}
		start := file.Offset(pos)
		end := start + len(lit)
		if lit == "" && tok != token.SEMICOLON && tok != token.EOF {
			end = start + len(tok.String())
		}
		tokens = append(tokens, scanned{tok, lit, start, end})
	}
	aliases := map[string]string{"fmt": fmtAlias, "omp": ompAlias}
	blocked := map[string]bool{}
	if len(persistentShadowed) > 0 {
		for name, value := range persistentShadowed[0] {
			if value {
				blocked[name] = true
			}
		}
	}
	var replacements []rewriteSpan
	var importSpans []rewriteSpan
	isComment := func(tok token.Token) bool { return tok == token.COMMENT }
	nextSignificant := func(i int) int {
		for i < len(tokens) && isComment(tokens[i].tok) {
			i++
		}
		return i
	}
	aliasFor := func(start, pathIndex int, defaultAlias string) (string, error) {
		for k := start; k < pathIndex; k++ {
			if isComment(tokens[k].tok) {
				continue
			}
			if tokens[k].tok == token.IDENT {
				return tokens[k].lit, nil
			}
			if tokens[k].tok == token.PERIOD {
				return ".", nil
			}
		}
		return defaultAlias, nil
	}
	process := func(specStart, pathIndex int) (bool, error) {
		path, err := strconv.Unquote(tokens[pathIndex].lit)
		if err != nil {
			return false, nil
		}
		defaultAlias := path[strings.LastIndex(path, "/")+1:]
		local, err := aliasFor(specStart, pathIndex, defaultAlias)
		if err != nil {
			return false, err
		}
		replacementPath, host := aliases[path]
		if !host {
			// A user import may intentionally use the names fmt/omp. It must
			// shadow the injected facade rather than being rewritten to it.
			if local != "." && local != "_" {
				blocked[local] = true
			}
			return false, nil
		}
		switch local {
		case ".":
			return false, fmt.Errorf("dot-importing %q is not supported", path)
		case "_":
		default:
			aliases[local] = replacementPath
		}
		return true, nil
	}

	for i := 0; i < len(tokens); i++ {
		if tokens[i].tok != token.IMPORT {
			continue
		}
		j := nextSignificant(i + 1)
		if j >= len(tokens) {
			continue
		}
		// import("path") is a parenthesized single import, not a grouped block.
		if tokens[j].tok == token.LPAREN {
			k := nextSignificant(j + 1)
			if k < len(tokens) && tokens[k].tok == token.STRING {
				close := k + 1
				for close < len(tokens) && tokens[close].tok != token.RPAREN {
					close++
				}
				if close >= len(tokens) {
					continue
				}
				importEnd := tokens[close].end
				if close+1 < len(tokens) && tokens[close+1].tok == token.SEMICOLON {
					importEnd = tokens[close+1].end
				}
				importSpans = append(importSpans, rewriteSpan{tokens[i].start, importEnd, ""})
				host, err := process(j+1, k)
				if err != nil {
					return "", err
				}
				if host {
					end := tokens[close].end
					if close+1 < len(tokens) && tokens[close+1].tok == token.SEMICOLON {
						end = tokens[close+1].end
					}
					replacements = append(replacements, rewriteSpan{tokens[i].start, end, ""})
					i = close
				}
				continue
			}
			// Grouped import declaration. Track each spec's source span and remove
			// the whole group when every spec is host-injected.
			depth, specStartIndex, specStart := 1, j+1, tokens[j].end
			allHost, anySpec := true, false
			var specs []rewriteSpan
			for k := j + 1; k < len(tokens) && depth > 0; k++ {
				if tokens[k].tok == token.LPAREN {
					depth++
					continue
				}
				if tokens[k].tok == token.RPAREN {
					depth--
					if depth == 0 {
						importEnd := tokens[k].end
						if k+1 < len(tokens) && tokens[k+1].tok == token.SEMICOLON {
							importEnd = tokens[k+1].end
						}
						importSpans = append(importSpans, rewriteSpan{tokens[i].start, importEnd, ""})
						if anySpec && allHost {
							replacements = append(replacements, rewriteSpan{tokens[i].start, tokens[k].end, ""})
						} else {
							replacements = append(replacements, specs...)
						}
						i = k
						break
					}
				}
				if depth != 1 {
					continue
				}
				if tokens[k].tok == token.SEMICOLON {
					specStartIndex, specStart = k+1, tokens[k].end
					continue
				}
				if tokens[k].tok != token.STRING {
					continue
				}
				host, err := process(specStartIndex, k)
				if err != nil {
					return "", err
				}
				anySpec = true
				end := tokens[k].end
				if host {
					specs = append(specs, rewriteSpan{specStart, end, ""})
				} else {
					allHost = false
				}
				m := k + 1
				for m < len(tokens) && tokens[m].tok != token.SEMICOLON && tokens[m].tok != token.RPAREN {
					m++
				}
				specStartIndex, specStart = m, end
				k = m - 1
			}
			continue
		}
		// Ordinary single import declaration. Include an explicit semicolon in
		// the removed span so a following statement remains valid.
		k := j
		for k < len(tokens) && tokens[k].tok != token.STRING && tokens[k].tok != token.SEMICOLON {
			k++
		}
		if k >= len(tokens) || tokens[k].tok != token.STRING {
			continue
		}
		importEnd := tokens[k].end
		if k+1 < len(tokens) && tokens[k+1].tok == token.SEMICOLON {
			importEnd = tokens[k+1].end
		}
		importSpans = append(importSpans, rewriteSpan{tokens[i].start, importEnd, ""})
		host, err := process(i+1, k)
		if err != nil {
			return "", err
		}
		if host {
			end := tokens[k].end
			if k+1 < len(tokens) && tokens[k+1].tok == token.SEMICOLON {
				end = tokens[k+1].end
			}
			replacements = append(replacements, rewriteSpan{tokens[i].start, end, ""})
			i = k
		}
	}
	out := rewriteQualifiedIdentifiers(source, aliases, blocked, importSpans, replacements)
	out = strings.ReplaceAll(out, "import (;", "import (")
	out = strings.ReplaceAll(out, "import ( ;", "import (")
	out = strings.ReplaceAll(out, "; )", " )")
	out = strings.ReplaceAll(out, ";)", ")")
	return out, nil
}

func rewriteQualifiedIdentifiers(source string, aliases map[string]string, blocked map[string]bool, importSpans, hostSpans []rewriteSpan) string {
	// A lexical replacement is tempting here, but it rewrites a package-looking
	// identifier even when a cell has shadowed it with a parameter, local, type,
	// or function. Parse the cell in both forms accepted by the Yaegi evaluator:
	// declarations at file scope and statements inside a synthetic function.
	// The parser's resolver annotates references to declarations with Obj, while
	// unresolved package names (including the injected aliases) remain nil.
	type parseCandidate struct {
		file   *ast.File
		set    *token.FileSet
		prefix int
	}
	analysisSource := source
	for _, span := range importSpans {
		if span.start < 0 || span.end > len(analysisSource) || span.start >= span.end {
			continue
		}
		// Preserve line breaks and byte offsets while hiding import declarations
		// from the synthetic statement parser.
		masked := []byte(analysisSource)
		for i := span.start; i < span.end; i++ {
			if masked[i] != '\n' && masked[i] != '\r' {
				masked[i] = ' '
			}
		}
		analysisSource = string(masked)
	}
	parse := func(prefix, suffix string) *parseCandidate {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, "cell.go", prefix+analysisSource+suffix, parser.AllErrors)
		if err != nil || file == nil {
			return nil
		}
		return &parseCandidate{file: file, set: set, prefix: len(prefix)}
	}
	candidate := parse("package main\n", "\n")
	if candidate == nil {
		candidate = parse("package main\nfunc __omp_cell__() {\n", "\n}\n")
	}
	if candidate == nil {
		// The interpreter will report the syntax error. Do not make a potentially
		// unsafe rewrite when we cannot establish lexical scopes.
		return source
	}
	insideImport := func(offset int) bool {
		for _, span := range importSpans {
			if offset >= span.start && offset < span.end {
				return true
			}
		}
		return false
	}
	var replacements []rewriteSpan
	file := candidate.set.File(candidate.file.Pos())
	ast.Inspect(candidate.file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if !ok || ident.Obj != nil || blocked[ident.Name] {
			return true
		}
		start := file.Offset(ident.Pos()) - candidate.prefix
		end := file.Offset(ident.End()) - candidate.prefix
		if start < 0 || end > len(source) || start >= end || insideImport(start) {
			return true
		}
		replacement, ok := aliases[ident.Name]
		if !ok {
			return true
		}
		replacements = append(replacements, rewriteSpan{start: start, end: end, text: replacement})
		return true
	})
	// Apply selector rewrites and host-import removals as one reverse-offset
	// pass. Rewriting an identifier can change its byte length; doing import
	// removals afterward with original offsets would otherwise target the wrong
	// bytes when a cell contains an import after a selector.
	replacements = append(replacements, hostSpans...)
	sort.SliceStable(replacements, func(i, j int) bool {
		return replacements[i].start > replacements[j].start
	})
	out := source
	for _, r := range replacements {
		if r.start < 0 || r.end > len(out) || r.start >= r.end {
			continue
		}
		out = out[:r.start] + r.text + out[r.end:]
	}
	return out
}

func isGoIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isGoIdentifierPart(ch byte) bool {
	return isGoIdentifierStart(ch) || ch >= '0' && ch <= '9'
}

// safeStdlibSymbols intentionally exposes only deterministic, non-I/O standard
// library packages. In particular, os/os is excluded: Yaegi otherwise wires
// os.Stdin/Stdout/Stderr to the runner's real descriptors and interpreted code
// can consume or corrupt the NDJSON control stream. The host facade is the only
// supported route to tools, agents, and hub operations.
func safeStdlibSymbols() interp.Exports {
	const allowed = "bytes bytes/bytes encoding/base64 encoding/base64/base64 encoding/json encoding/json/json errors errors fmt/fmt io/io math/math path/path regexp/regexp regexp/syntax/syntax sort/sort strconv/strconv strings/strings unicode/unicode unicode/utf8/utf8"
	allow := map[string]struct{}{}
	for _, key := range strings.Fields(allowed) {
		allow[key] = struct{}{}
	}
	exports := make(interp.Exports, len(allow))
	for key, symbols := range stdlib.Symbols {
		if _, ok := allow[key]; ok {
			clone := make(map[string]reflect.Value, len(symbols))
			for name, value := range symbols {
				clone[name] = value
			}
			exports[key] = clone
		}
	}
	return exports
}

func configureEnvironment(cwd string, env map[string]*string) error {
	if cwd != "" {
		if err := os.Chdir(cwd); err != nil {
			return fmt.Errorf("change working directory: %w", err)
		}
	}
	for key, value := range env {
		// Never mirror bridge identity into the helper's real environment on each cell;
		// this prevents descendants (if an unsafe symbol ever slips through) from
		// inheriting host bridge credentials.
		if strings.HasPrefix(key, "PI_TOOL_BRIDGE_") {
			continue
		}
		var err error
		if value == nil {
			err = os.Unsetenv(key)
		} else {
			err = os.Setenv(key, *value)
		}
		if err != nil {
			return fmt.Errorf("update environment %q: %w", key, err)
		}
	}
	return nil
}

func containsGoStatement(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			found = true
			return false
		}
		return !found
	})
	return found
}

// validateCellSource rejects goroutine statements. Yaegi has no public way to
// join or cancel interpreted goroutines at a normal cell boundary; allowing
// them would let output and work outlive the request and poison the persistent
// helper. The host bridge remains available for bounded asynchronous work.
func validateCellSource(source string) error {
	set := token.NewFileSet()
	if file, _ := parser.ParseFile(set, "cell.go", source, parser.AllErrors); file != nil && containsGoStatement(file) {
		return errors.New("Go eval cells cannot start goroutines; use omp.Agent or omp.Hub for host-mediated async work")
	}
	wrapped := "package main\nfunc __omp_cell__() {\n" + source + "\n}"
	if file, _ := parser.ParseFile(token.NewFileSet(), "cell-statement.go", wrapped, parser.AllErrors); file != nil && containsGoStatement(file) {
		return errors.New("Go eval cells cannot start goroutines; use omp.Agent or omp.Hub for host-mediated async work")
	}
	return nil
}

func formatEvalError(err error) (string, string, []string) {
	if err == nil {
		return "", "", nil
	}
	if panicValue, ok := err.(interp.Panic); ok {
		return "panic", panicValue.Error(), strings.Split(strings.TrimSpace(string(panicValue.Stack)), "\n")
	}
	return "Error", err.Error(), nil
}

func evalSource(code string) string {
	return code
}

// cellPart is one package declaration or top-level statement that can be
// evaluated independently by Yaegi. Yaegi accepts a package declaration or a
// sequence of statements in one Eval call, but rejects a cell that mixes both
// forms (for example `var n = 1\nfmt.Println(n)`). Splitting at the AST
// boundary preserves Go's source order while retaining package-scope state.
type cellPart struct {
	start       int
	end         int
	declaration bool
}

func sourcePartOffset(fileSet *token.FileSet, pos token.Pos, prefixLen, sourceLen int) (int, bool) {
	file := fileSet.File(pos)
	if file == nil {
		return 0, false
	}
	offset := file.Offset(pos) - prefixLen
	if offset < 0 || offset >= sourceLen {
		return 0, false
	}
	return offset, true
}

func sourcePartEnd(fileSet *token.FileSet, pos token.Pos, prefixLen, sourceLen int) (int, bool) {
	file := fileSet.File(pos)
	if file == nil {
		return 0, false
	}
	offset := file.Offset(pos) - prefixLen
	if offset <= 0 || offset > sourceLen {
		return 0, false
	}
	return offset, true
}

// splitCellSource identifies direct package declarations and top-level
// statements without interpreting arbitrary source. Declarations are parsed
// in a synthetic package, then masked before parsing statements in a synthetic
// function. This keeps the real source spans (including multiline functions,
// loops, and comments) and lets execute evaluate each part in source order.
func splitCellSource(source string) ([]cellPart, error) {
	if strings.TrimSpace(source) == "" {
		return nil, nil
	}
	const packagePrefix = "package main\n"
	packageSet := token.NewFileSet()
	packageFile, packageErr := parser.ParseFile(packageSet, "cell.go", packagePrefix+source, parser.AllErrors)
	if packageFile == nil {
		if packageErr != nil {
			return nil, packageErr
		}
		return nil, errors.New("unable to parse Go eval cell")
	}

	parts := make([]cellPart, 0, len(packageFile.Decls))
	masked := []byte(source)
	for _, declaration := range packageFile.Decls {
		if _, bad := declaration.(*ast.BadDecl); bad {
			continue
		}
		start, ok := sourcePartOffset(packageSet, declaration.Pos(), len(packagePrefix), len(source))
		if !ok {
			continue
		}
		end, ok := sourcePartEnd(packageSet, declaration.End(), len(packagePrefix), len(source))
		if !ok || start >= end {
			continue
		}
		parts = append(parts, cellPart{start: start, end: end, declaration: true})
		// Keep line/column offsets stable for the statement parse. Explicit
		// semicolons are intentionally retained; Go accepts empty statements.
		for i := start; i < end; i++ {
			if masked[i] != '\n' && masked[i] != '\r' {
				masked[i] = ' '
			}
		}
	}

	const functionPrefix = "package main\nfunc __omp_cell__() {\n"
	statementSet := token.NewFileSet()
	statementFile, statementErr := parser.ParseFile(
		statementSet,
		"cell-statement.go",
		functionPrefix+string(masked)+"\n}\n",
		parser.AllErrors,
	)
	if statementFile == nil {
		if statementErr != nil {
			return nil, statementErr
		}
		return nil, errors.New("unable to parse Go eval statements")
	}
	var body *ast.BlockStmt
	for _, declaration := range statementFile.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok && function.Body != nil {
			body = function.Body
			break
		}
	}
	if body == nil {
		if statementErr != nil {
			return nil, statementErr
		}
		return nil, errors.New("unable to locate Go eval statement body")
	}
	for _, statement := range body.List {
		start, ok := sourcePartOffset(statementSet, statement.Pos(), len(functionPrefix), len(source))
		if !ok {
			continue
		}
		end, ok := sourcePartEnd(statementSet, statement.End(), len(functionPrefix), len(source))
		if !ok || start >= end {
			continue
		}
		parts = append(parts, cellPart{start: start, end: end})
	}
	if statementErr != nil {
		// The package parse is expected to complain when statements are present,
		// but the function parse must be clean after declarations are masked.
		return nil, statementErr
	}
	if len(parts) == 0 {
		return nil, errors.New("Go eval cell contains no executable declaration or statement")
	}
	sort.SliceStable(parts, func(i, j int) bool {
		if parts[i].start == parts[j].start {
			return parts[i].declaration && !parts[j].declaration
		}
		return parts[i].start < parts[j].start
	})
	return parts, nil
}

func execute(r *runner, interpreter *interp.Interpreter, req request) {
	r.emit(frame{Type: "started", ID: req.ID})
	if err := validateCellSource(req.Code); err != nil {
		name, value, trace := formatEvalError(err)
		r.emit(frame{Type: "error", ID: req.ID, Ename: name, Evalue: value, Traceback: trace})
		r.emit(frame{Type: "done", ID: req.ID, Status: "error"})
		return
	}
	if err := configureEnvironment(req.Cwd, req.Env); err != nil {
		name, value, trace := formatEvalError(err)
		r.emit(frame{Type: "error", ID: req.ID, Ename: name, Evalue: value, Traceback: trace})
		r.emit(frame{Type: "done", ID: req.ID, Status: "error"})
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Give each cell an independent capability object. Retained interpreted
	// functions from an earlier cell therefore keep their own credentials/context
	// instead of inheriting the next cell's bridge identity.
	exec := &execution{id: req.ID, active: true}
	cellBridge := newBridgeClient().capability(req.BridgeURL, req.BridgeSession, req.ID, ctx, exec)
	cellPackage := "omp_cell_" + hex.EncodeToString([]byte(req.ID))
	cellAlias := "ompCell" + hex.EncodeToString([]byte(req.ID))
	fmtPackage := "omp_fmt_" + hex.EncodeToString([]byte(req.ID))
	fmtAlias := "ompFmt" + hex.EncodeToString([]byte(req.ID))
	err := interpreter.Use(symbolsFor(cellPackage, cellBridge))
	if err == nil {
		err = interpreter.Use(fmtSymbols(fmtPackage, frameWriter{runner: r, kind: "stdout", current: func() *execution { return exec }}))
	}
	if err != nil {
		name, value, trace := formatEvalError(err)
		r.emit(frame{Type: "error", ID: req.ID, Ename: name, Evalue: value, Traceback: trace})
		r.emit(frame{Type: "done", ID: req.ID, Status: "error"})
		cancel()
		return
	}
	r.setExecution(exec, cancel, cellBridge)
	defer func() {
		cancel()
		r.clearExecution(exec, cellBridge)
	}()
	r.stateMu.Lock()
	shadowed := make(map[string]bool, len(r.shadowed))
	for name, value := range r.shadowed {
		shadowed[name] = value
	}
	r.stateMu.Unlock()
	code, rewriteErr := rewriteCellImports(req.Code, fmtAlias, cellAlias, shadowed)
	if rewriteErr != nil {
		err = rewriteErr
	} else {
		parts, splitErr := splitCellSource(code)
		if splitErr != nil {
			err = splitErr
		} else {
			_, err = interpreter.EvalWithContext(ctx, fmt.Sprintf("import %s %q; import %s %q", cellAlias, cellPackage, fmtAlias, fmtPackage))
			for _, part := range parts {
				if err != nil {
					break
				}
				partCode := code[part.start:part.end]
				_, err = interpreter.EvalWithContext(ctx, partCode)
				if err == nil && part.declaration {
					for name := range topLevelDeclarations(partCode) {
						r.stateMu.Lock()
						if r.shadowed == nil {
							r.shadowed = make(map[string]bool)
						}
						r.shadowed[name] = true
						r.stateMu.Unlock()
					}
				}
			}
		}
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			r.stateMu.Lock()
			r.poisoned = true
			r.stateMu.Unlock()
			r.emit(frame{Type: "done", ID: req.ID, Status: "error", Cancelled: true})
			return
		}
		name, value, trace := formatEvalError(err)
		r.emit(frame{Type: "error", ID: req.ID, Ename: name, Evalue: value, Traceback: trace})
		r.emit(frame{Type: "done", ID: req.ID, Status: "error"})
		return
	}
	for name := range topLevelDeclarations(code) {
		r.stateMu.Lock()
		if r.shadowed == nil {
			r.shadowed = make(map[string]bool)
		}
		r.shadowed[name] = true
		r.stateMu.Unlock()
	}
	r.emit(frame{Type: "done", ID: req.ID, Status: "ok"})
}

// topLevelDeclarations returns names that survive at package scope in the
// persistent Yaegi interpreter. Function-body locals are intentionally omitted:
// they are resolved by the AST scope pass for that cell only.
func topLevelDeclarations(source string) map[string]bool {
	set := token.NewFileSet()
	file, _ := parser.ParseFile(set, "cell.go", "package main\n"+source, parser.AllErrors)
	if file == nil {
		return nil
	}
	declared := make(map[string]bool)
	for _, decl := range file.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if decl.Name != nil {
				declared[decl.Name.Name] = true
			}
		case *ast.GenDecl:
			switch decl.Tok {
			case token.IMPORT:
				for _, spec := range decl.Specs {
					imp, ok := spec.(*ast.ImportSpec)
					if !ok || imp.Name == nil || imp.Name.Name == "." || imp.Name.Name == "_" {
						continue
					}
					path, err := strconv.Unquote(imp.Path.Value)
					if err == nil && (path == "fmt" || path == "omp") {
						// Host imports are rewritten to per-cell aliases and do not
						// become user declarations in the Yaegi package scope.
						continue
					}
					declared[imp.Name.Name] = true
				}
			case token.CONST, token.VAR:
				for _, spec := range decl.Specs {
					values, ok := spec.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for _, name := range values.Names {
						if name != nil {
							declared[name.Name] = true
						}
					}
				}
			case token.TYPE:
				for _, spec := range decl.Specs {
					if typeSpec, ok := spec.(*ast.TypeSpec); ok && typeSpec.Name != nil {
						declared[typeSpec.Name.Name] = true
					}
				}
			}
		}
	}
	return declared
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Println("omp-eval-go-runner yaegi")
		return
	}
	// The host injects bridge identity per eval request, not at process startup.
	// This keeps credentials out of the helper environment and makes a runner
	// startup handshake possible before a ToolSession is attached.
	client := newBridgeClient()
	for _, key := range []string{"PI_TOOL_BRIDGE_URL", "PI_TOOL_BRIDGE_TOKEN", "PI_TOOL_BRIDGE_SESSION"} {
		_ = os.Unsetenv(key)
	}
	r := &runner{encoder: json.NewEncoder(os.Stdout), shadowed: make(map[string]bool)}
	interpreter := interp.New(interp.Options{
		// Never let interpreted fmt.Scan or similar consume NDJSON requests.
		Stdin:  strings.NewReader(""),
		Stdout: frameWriter{runner: r, kind: "stdout", current: r.currentExecution},
		Stderr: frameWriter{runner: r, kind: "stderr", current: r.currentExecution},
		// Do not resolve source imports from cwd/GOPATH. All supported packages
		// are explicitly injected above; an empty FS prevents relative imports
		// from smuggling goroutines or callbacks past cell preflight.
		SourcecodeFilesystem: fstest.MapFS{},
	})
	if err := interpreter.Use(safeStdlibSymbols()); err != nil {
		fmt.Fprintln(os.Stderr, "omp-eval-go-runner: load standard library:", err)
		os.Exit(2)
	}
	// Bootstrap only establishes the package namespace. It must never carry real
	// credentials: every cell gets an immutable session/run-scoped capability below.
	bootstrap := client.capability("", "", "", context.Background(), nil)
	if err := interpreter.Use(symbolsFor("omp", bootstrap)); err != nil {
		fmt.Fprintln(os.Stderr, "omp-eval-go-runner: load omp bridge:", err)
		os.Exit(2)
	}
	if _, err := interpreter.Eval(`import "omp"`); err != nil {
		fmt.Fprintln(os.Stderr, "omp-eval-go-runner: initialize omp package:", err)
		os.Exit(2)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer signal.Stop(signals)
	go func() {
		for range signals {
			r.interrupt()
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var req request
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			r.emit(frame{Type: "error", Ename: "ProtocolError", Evalue: err.Error()})
			continue
		}
		if req.Type == "ready" {
			r.emit(frame{Type: "ready"})
			continue
		}
		if req.Type == "exit" || (req.ID == "" && req.Code == "") {
			return
		}
		if req.ID == "" {
			r.emit(frame{Type: "error", Ename: "ProtocolError", Evalue: "missing request id"})
			continue
		}
		execute(r, interpreter, req)
		r.stateMu.Lock()
		poisoned := r.poisoned
		r.stateMu.Unlock()
		if poisoned {
			return
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "omp-eval-go-runner: stdin:", err)
	}
}
