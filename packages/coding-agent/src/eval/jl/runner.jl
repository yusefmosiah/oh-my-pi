# OMP Julia runner — subprocess wrapper used by the coding-agent host.
# Persistent Julia process that speaks NDJSON over stdout and a custom TSV protocol on stdin.

using Base64

# Force GR (the default Plots.jl backend) into a headless workstation so a plot
# never pops up a native gksqt GUI window — the harness renders the inline PNG
# from `show(io, MIME"image/png", plt)` itself. `get!` keeps an explicit
# user-provided value, mirroring the Python runner's MPLBACKEND=Agg default.
get!(ENV, "GKSwstype", "100")

# Bridge credentials are delivered in each request and held in runner-owned
# state, not ENV. Scrub inherited values before any user code or child process
# can observe them.
const BRIDGE_ENV_KEYS = ("PI_TOOL_BRIDGE_URL", "PI_TOOL_BRIDGE_TOKEN", "PI_TOOL_BRIDGE_SESSION")
for key in BRIDGE_ENV_KEYS
    delete!(ENV, key)
end

const ORIGINAL_STDOUT = stdout
const ORIGINAL_STDERR = stderr
const ORIGINAL_STDIN = stdin

# Redirect stdin/stdout/stderr to prevent cell prints from corrupting NDJSON
out_rd, out_wr = redirect_stdout()
err_rd, err_wr = redirect_stderr()
redirect_stdin(devnull)

# Keep the active run identity task-local. Julia tasks may outlive a cell; a
# process-global run id would let a stale task adopt the next owner's identity.
const OMP_RUN_ID_KEY = :__omp_eval_run_id
function __omp_current_run_id()
    value = try
        Base.task_local_storage(OMP_RUN_ID_KEY)
    catch
        nothing
    end
    value isa String ? value : nothing
end
function __omp_set_run_id(value)
    Base.task_local_storage(OMP_RUN_ID_KEY, value)
    nothing
end

# Keep credential values and bridge transport in lexical runner closures. The
# value-only hook is nulled after prelude initialization; only the write-only
# setter remains global.
const OMP_RUNNER_BRIDGE_TIMEOUT_SECONDS = 5 * 60
let state = Ref{Any}(nothing)
    global __omp_bridge_set_credentials = values -> (state[] = values)
    global __omp_bridge_call = (name::String, args::Dict{String, Any}) -> begin
        credentials = state[]
        base_url, _, session = credentials === nothing ? (nothing, nothing, nothing) : credentials
        if base_url === nothing || session === nothing
            error("Tool bridge is not available in this cell.")
        end
        # Retained runners receive the complete tokenless broker URL. Keep
        # accepting a root URL for old callers, but never require/emit a bearer.
        url = base_url
        if !endswith(url, "/v1/eval-tool") && !endswith(url, "/v1/tool")
            url = endswith(url, "/") ? (url * "v1/eval-tool") : (url * "/v1/eval-tool")
        end
        payload = Main.json_serialize(Dict(
            "session" => session,
            "run" => __omp_current_run_id(),
            "name" => name,
            "args" => args,
        ))
        io_out = IOBuffer()
        response = Downloads.request(
            url,
            method="POST",
            headers=["Content-Type" => "application/json"],
            input=IOBuffer(payload),
            output=io_out,
            timeout=OMP_RUNNER_BRIDGE_TIMEOUT_SECONDS,
        )
        resp_str = String(take!(io_out))
        if response.status != 200
            error("Tool bridge call failed with status $(response.status): $resp_str")
        end
        parsed = Main.json_parse(resp_str)
        if !get(parsed, "ok", false)
            error("Tool bridge error: $(get(parsed, "error", "Unknown error"))")
        end
        get(parsed, "value", nothing)
    end
end
const write_lock = ReentrantLock()
const drain_state_lock = ReentrantLock()

mutable struct DrainBarrier
    marker::Vector{UInt8}
    remaining::Int
    done::Channel{Nothing}
end

const active_drain_barrier = Ref{Union{Nothing, DrainBarrier}}(nothing)
const drain_marker_counter = Ref{UInt}(0)

function json_parse(s::String)
    chars = collect(s)
    pos = 1
    len = length(chars)
    
    function skip_whitespace()
        while pos <= len && isspace(chars[pos])
            pos += 1
        end
    end
    
    function parse_value()
        skip_whitespace()
        if pos > len
            error("Unexpected EOF")
        end
        c = chars[pos]
        if c == '"'
            return parse_string()
        elseif c == '{'
            return parse_object()
        elseif c == '['
            return parse_array()
        elseif (c == 't' || c == 'f')
            return parse_boolean()
        elseif c == 'n'
            return parse_null()
        elseif c == '-' || isdigit(c)
            return parse_number()
        else
            error("Unexpected character at $pos: $c")
        end
    end
    
    function parse_string()
        pos += 1 # skip '"'
        res = IOBuffer()
        while pos <= len
            c = chars[pos]
            if c == '"'
                pos += 1 # skip '"'
                return String(take!(res))
            elseif c == '\\'
                pos += 1
                if pos > len; error("Unexpected EOF in string escape"); end
                esc = chars[pos]
                if esc == '"'
                    write(res, '"')
                elseif esc == '\\'
                    write(res, '\\')
                elseif esc == '/'
                    write(res, '/')
                elseif esc == 'b'
                    write(res, '\b')
                elseif esc == 'f'
                    write(res, '\f')
                elseif esc == 'n'
                    write(res, '\n')
                elseif esc == 'r'
                    write(res, '\r')
                elseif esc == 't'
                    write(res, '\t')
                elseif esc == 'u'
                    # parse 4 hex digits
                    hex = ""
                    for i in 1:4
                        pos += 1
                        hex *= chars[pos]
                    end
                    write(res, Char(parse(Int, hex, base=16)))
                else
                    write(res, esc)
                end
            else
                write(res, c)
            end
            pos += 1
        end
        error("Unterminated string")
    end
    
    function parse_object()
        pos += 1 # skip '{'
        obj = Dict{String, Any}()
        skip_whitespace()
        if pos <= len && chars[pos] == '}'
            pos += 1
            return obj
        end
        while true
            skip_whitespace()
            if pos > len || chars[pos] != '"'
                error("Expected string key in object at $pos")
            end
            key = parse_string()
            skip_whitespace()
            if pos > len || chars[pos] != ':'
                error("Expected ':' at $pos")
            end
            pos += 1 # skip ':'
            val = parse_value()
            obj[key] = val
            skip_whitespace()
            if pos > len
                error("Expected ',' or '}' in object at $pos")
            end
            c = chars[pos]
            if c == '}'
                pos += 1
                return obj
            elseif c == ','
                pos += 1
            else
                error("Expected ',' or '}' in object at $pos, got '$c'")
            end
        end
    end
    
    function parse_array()
        pos += 1 # skip '['
        arr = Any[]
        skip_whitespace()
        if pos <= len && chars[pos] == ']'
            pos += 1
            return arr
        end
        while true
            push!(arr, parse_value())
            skip_whitespace()
            if pos > len
                error("Expected ',' or ']' in array")
            end
            c = chars[pos]
            if c == ']'
                pos += 1
                return arr
            elseif c == ','
                pos += 1
            else
                error("Expected ',' or ']' in array at $pos, got '$c'")
            end
        end
    end
    
    function parse_boolean()
        s_slice = String(chars[pos:min(len, pos+4)])
        if startswith(s_slice, "true")
            pos += 4
            return true
        elseif startswith(s_slice, "false")
            pos += 5
            return false
        else
            error("Expected boolean at $pos")
        end
    end
    
    function parse_null()
        s_slice = String(chars[pos:min(len, pos+3)])
        if startswith(s_slice, "null")
            pos += 4
            return nothing
        else
            error("Expected null at $pos")
        end
    end
    
    function parse_number()
        start_pos = pos
        while pos <= len
            c = chars[pos]
            if isdigit(c) || c in ['.', '-', '+', 'e', 'E']
                pos += 1
            else
                break
            end
        end
        num_str = String(chars[start_pos:pos-1])
        val = tryparse(Int, num_str)
        if val !== nothing
            return val
        end
        val_f = tryparse(Float64, num_str)
        if val_f !== nothing
            return val_f
        end
        error("Invalid number format: $num_str")
    end
    
    val = parse_value()
    skip_whitespace()
    if pos <= len
        error("Extra data after JSON value at $pos")
    end
    return val
end

function json_serialize_string(s::AbstractString)
    res = IOBuffer()
    write(res, '"')
    for c in s
        if c == '"'
            write(res, "\\\"")
        elseif c == '\\'
            write(res, "\\\\")
        elseif c == '\n'
            write(res, "\\n")
        elseif c == '\r'
            write(res, "\\r")
        elseif c == '\t'
            write(res, "\\t")
        elseif c == '\f'
            write(res, "\\f")
        elseif c == '\b'
            write(res, "\\b")
        elseif UInt32(c) < 32
            d1 = div(UInt32(c), 16)
            d2 = rem(UInt32(c), 16)
            hex_chars = "0123456789abcdef"
            write(res, "\\u00" * hex_chars[d1 + 1] * hex_chars[d2 + 1])
        else
            write(res, c)
        end
    end
    write(res, '"')
    return String(take!(res))
end

function json_serialize(v)
    if v === nothing
        return "null"
    elseif v isa Bool
        return v ? "true" : "false"
    elseif v isa Number
        return string(v)
    elseif v isa AbstractString
        return json_serialize_string(v)
    elseif v isa Symbol
        return json_serialize_string(string(v))
    elseif v isa AbstractVector
        return "[" * join([json_serialize(x) for x in v], ",") * "]"
    elseif v isa AbstractDict
        parts = String[]
        for (k, val) in v
            push!(parts, json_serialize_string(string(k)) * ":" * json_serialize(val))
        end
        return "{" * join(parts, ",") * "}"
    else
        return json_serialize_string(repr(v))
    end
end

function emit_frame(frame)
    lock(write_lock) do
        println(ORIGINAL_STDOUT, json_serialize(frame))
        flush(ORIGINAL_STDOUT)
    end
end

function find_subsequence(haystack::Vector{UInt8}, needle::Vector{UInt8})
    needle_len = length(needle)
    if needle_len == 0 || length(haystack) < needle_len
        return nothing
    end
    last_start = length(haystack) - needle_len + 1
    for start in 1:last_start
        matched = true
        @inbounds for offset in 1:needle_len
            if haystack[start + offset - 1] != needle[offset]
                matched = false
                break
            end
        end
        if matched
            return start
        end
    end
    return nothing
end

function marker_overlap(haystack::Vector{UInt8}, needle::Vector{UInt8})
    max_overlap = min(length(haystack), length(needle) - 1)
    for overlap in max_overlap:-1:1
        matched = true
        @inbounds for offset in 1:overlap
            if haystack[length(haystack) - overlap + offset] != needle[offset]
                matched = false
                break
            end
        end
        if matched
            return overlap
        end
    end
    return 0
end

function emit_stream_bytes(kind, bytes::Vector{UInt8})
    rid = __omp_current_run_id()
    if rid === nothing || isempty(bytes)
        return
    end
    emit_frame(Dict("type" => kind, "id" => rid, "data" => String(copy(bytes))))
end

function signal_drain_barrier!(marker::Vector{UInt8})
    done_channel = lock(drain_state_lock) do
        barrier = active_drain_barrier[]
        if barrier === nothing || barrier.marker != marker
            return nothing
        end
        barrier.remaining -= 1
        if barrier.remaining == 0
            active_drain_barrier[] = nothing
            return barrier.done
        end
        return nothing
    end
    if done_channel !== nothing
        put!(done_channel, nothing)
    end
    return nothing
end

function process_stream_buffer!(buffer::Vector{UInt8}, kind; flush_all::Bool=false)
    while true
        barrier = lock(drain_state_lock) do
            active_drain_barrier[]
        end
        marker = barrier === nothing ? nothing : barrier.marker
        if marker === nothing
            if !isempty(buffer)
                emit_stream_bytes(kind, buffer)
                empty!(buffer)
            end
            return
        end

        marker_index = find_subsequence(buffer, marker)
        if marker_index !== nothing
            emit_len = marker_index - 1
            if emit_len > 0
                emit_stream_bytes(kind, buffer[1:emit_len])
            end
            deleteat!(buffer, 1:(marker_index + length(marker) - 1))
            signal_drain_barrier!(marker)
            continue
        end

        keep_len = flush_all ? 0 : marker_overlap(buffer, marker)
        emit_len = length(buffer) - keep_len
        if emit_len > 0
            emit_stream_bytes(kind, buffer[1:emit_len])
            deleteat!(buffer, 1:emit_len)
        end
        return
    end
end

function next_drain_marker()
    drain_marker_counter[] += UInt(1)
    return Vector{UInt8}(codeunits("\0__OMP_DRAIN__:" * string(drain_marker_counter[]) * ":" * string(time_ns()) * "\0"))
end

function await_stream_drains()
    flush(stdout)
    flush(stderr)
    barrier = DrainBarrier(next_drain_marker(), 2, Channel{Nothing}(1))
    lock(drain_state_lock) do
        if active_drain_barrier[] !== nothing
            error("Drain barrier already active")
        end
        active_drain_barrier[] = barrier
    end
    try
        Base.write(out_wr, barrier.marker)
        flush(out_wr)
        Base.write(err_wr, barrier.marker)
        flush(err_wr)
        take!(barrier.done)
    finally
        lock(drain_state_lock) do
            if active_drain_barrier[] === barrier
                active_drain_barrier[] = nothing
            end
        end
    end
    return nothing
end

function drain_stream(rd, kind)
    buffer = UInt8[]
    try
        while true
            data = readavailable(rd)
            if !isempty(data)
                append!(buffer, data)
                process_stream_buffer!(buffer, kind)
            elseif eof(rd)
                break
            else
                sleep(0.001)
            end
        end
    catch
        # ignore
    finally
        process_stream_buffer!(buffer, kind, flush_all=true)
    end
end

@async drain_stream(out_rd, "stdout")
@async drain_stream(err_rd, "stderr")

function build_mime_bundle(value)
    bundle = Dict{String, Any}()

    # text/plain — every mime probe below uses `Base.invokelatest` because this
    # function runs from the frozen-world `main()` loop: `show`/`showable`
    # methods that a package adds when it is `using`-ed *inside* a cell (e.g.
    # Plots/Makie/GraphRecipes registering rich `show` for their plot types) are
    # invisible to direct dispatch here and fall back to the default struct show,
    # which can itself throw. Guard text/plain too so a failing repr never aborts
    # the whole bundle before the image mime is reached.
    try
        io_plain = IOBuffer()
        Base.invokelatest(show, io_plain, MIME"text/plain"(), value)
        bundle["text/plain"] = String(take!(io_plain))
    catch
        bundle["text/plain"] = try
            summary(value)
        catch
            string(typeof(value))
        end
    end

    # rich mime types
    for mime_str in ["text/html", "text/markdown", "image/png", "image/jpeg"]
        m = MIME(Symbol(mime_str))
        if Base.invokelatest(showable, m, value)
            try
                io = IOBuffer()
                if mime_str in ["image/png", "image/jpeg"]
                    b64_io = Base64EncodePipe(io)
                    Base.invokelatest(show, b64_io, m, value)
                    close(b64_io)
                else
                    Base.invokelatest(show, io, m, value)
                end
                bundle[mime_str] = String(take!(io))
            catch
                # ignore
            end
        end
    end

    if value isa AbstractDict || value isa AbstractVector
        try
            bundle["application/json"] = value
        catch
            # ignore
        end
    end

    return bundle
end

struct OmpDisplay <: AbstractDisplay end

function Base.display(d::OmpDisplay, value)
    rid = __omp_current_run_id()
    if rid !== nothing
        bundle = build_mime_bundle(value)
        emit_frame(Dict("type" => "display", "id" => rid, "bundle" => bundle))
    end
    return nothing
end

pushdisplay(OmpDisplay())

function emit_error(rid, err, bt)
    io = IOBuffer()
    # invokelatest + guard: custom error types from packages loaded inside the
    # cell define `showerror` methods invisible to this frozen-world function.
    try
        Base.invokelatest(showerror, io, err)
    catch
        print(io, string(err))
    end
    err_str = String(take!(io))
    
    # Seed the traceback with the rendered exception text so the array is a
    # self-contained error display, matching the Python and Ruby runners. The
    # host shows `traceback` verbatim when present and only falls back to
    # `ename: evalue` when it is empty, so a frames-only traceback would hide
    # the real error. Julia's `showerror` output already embeds the exception
    # type for nearly every error and mirrors what the REPL prints after `ERROR: `.
    tb = isempty(err_str) ? String[] : String[err_str]
    for frame in stacktrace(bt)
        file = string(frame.file)
        line = frame.line
        func = string(frame.func)
        push!(tb, "  at $func ($file:$line)")
    end
    
    emit_frame(Dict(
        "type" => "error",
        "id" => rid,
        "ename" => string(typeof(err)),
        "evalue" => err_str,
        "traceback" => tb
    ))
end

function should_display_result(parsed_expr)
    if parsed_expr isa Expr && parsed_expr.head === :block
        args = parsed_expr.args
        if !isempty(args)
            last_arg = args[end]
            if last_arg isa Expr
                if last_arg.head in [Symbol("="), :function, :struct, :using, :import, :const, :global, :local, :macro]
                    return false
                end
            end
        end
    end
    return true
end

function apply_request_runtime(cwd, env_pairs)
    # Clear the previous generation first; sparse env patches must not retain a
    # bearer token after a bridged cell finishes.
    __omp_bridge_set_credentials((nothing, nothing, nothing))
    for key in BRIDGE_ENV_KEYS
        delete!(ENV, key)
    end

    try
        if !isempty(cwd)
            cd(cwd)
        end
    catch
        # ignore
    end
    
    managed_env_keys = [
        "PI_SESSION_FILE",
        "PI_ARTIFACTS_DIR",
        "PI_TOOL_BRIDGE_URL",
        "PI_TOOL_BRIDGE_TOKEN",
        "PI_TOOL_BRIDGE_SESSION",
        "PI_EVAL_LOCAL_ROOTS"
    ]
    for k in managed_env_keys
        delete!(ENV, k)
    end
    
    bridge_values = Union{Nothing, String}[nothing, nothing, nothing]
    if !isempty(env_pairs)
        for pair in split(env_pairs, ' ')
            if !isempty(pair)
                try
                    k_b64, v_b64 = split(pair, ':', limit=2)
                    k = String(base64decode(string(k_b64)))
                    v = String(base64decode(string(v_b64)))
                    if k == "PI_TOOL_BRIDGE_URL" || k == "PI_TOOL_BRIDGE_TOKEN" || k == "PI_TOOL_BRIDGE_SESSION"
                        if k == "PI_TOOL_BRIDGE_URL"
                            bridge_values[1] = v
                        elseif k == "PI_TOOL_BRIDGE_TOKEN"
                            # Bearers remain in the host-side broker. Ignore
                            # legacy/request-supplied copies in this process.
                            bridge_values[2] = nothing
                        else
                            bridge_values[3] = v
                        end
                        if bridge_values[1] !== nothing && bridge_values[3] !== nothing
                            __omp_bridge_set_credentials((bridge_values[1]::String, nothing, bridge_values[3]::String))
                        end
                    else
                        ENV[k] = v
                    end
                catch
                    # ignore
                end
            end
        end
    end
end

# Main loop
function main()
    while !eof(ORIGINAL_STDIN)
        line = readline(ORIGINAL_STDIN)
        if isempty(line)
            continue
        end
        parts = split(line, '\t')
        cmd = string(parts[1])
        if cmd == "exit"
            break
        elseif cmd == "run"
            if length(parts) < 7
                continue
            end
            rid = string(parts[2])
            cwd = String(base64decode(string(parts[3])))
            silent = string(parts[4]) == "1"
            store_history = string(parts[5]) == "1"
            env_pairs = string(parts[6])
            code = String(base64decode(string(parts[7])))
            
            __omp_set_run_id(rid)
            emit_frame(Dict("type" => "started", "id" => rid))
            
            apply_request_runtime(cwd, env_pairs)
            
            exec_status = "ok"
            try
                parsed = Meta.parse("begin\n" * code * "\nend")
                if parsed isa Expr && parsed.head === :error
                    # Syntax error from parser
                    exec_status = "error"
                    emit_frame(Dict(
                        "type" => "error",
                        "id" => rid,
                        "ename" => "ParseError",
                        "evalue" => string(parsed.args[1]),
                        "traceback" => String[]
                    ))
                else
                    ans = Core.eval(Main, parsed)
                    # The prelude has captured the value-only hook in its
                    # bridge closure. Remove it before user cells run.
                    if isdefined(Main, :__omp_prelude_loaded)
                        try
                            Base.delete_binding!(Main, :__omp_bridge_call)
                        catch
                            global __omp_bridge_call = nothing
                        end
                    end
                    if ans !== nothing && !silent && should_display_result(parsed)
                        bundle = build_mime_bundle(ans)
                        emit_frame(Dict("type" => "result", "id" => rid, "bundle" => bundle))
                    end
                end
            catch err
                exec_status = "error"
                emit_error(rid, err, catch_backtrace())
            end
            
            await_stream_drains()
            
            emit_frame(Dict(
                "type" => "done",
                "id" => rid,
                "status" => exec_status,
                "executionCount" => 1,
                "cancelled" => false
            ))
            __omp_set_run_id(nothing)
        end
    end
end

main()
