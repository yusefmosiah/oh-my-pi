/**
 * Subprocess-backed Python runner.
 *
 * Speaks NDJSON with `runner.py` over stdin/stdout. One subprocess per kernel
 * instance; sessions reuse a single subprocess across executions. Cancellation
 * is `kill("SIGINT")` which raises a real `KeyboardInterrupt` inside user
 * code. Shutdown writes `{"type":"exit"}` and escalates to SIGTERM/SIGKILL on
 * timeout.
 */
import * as path from "node:path";
import { $flag, isBunTestRuntime, logger, Snowflake } from "@oh-my-pi/pi-utils";
import { Settings } from "../../config/settings";
import {
	BaseKernel,
	cacheKernelAvailabilityProbe,
	createAbortError,
	getCachedKernelAvailability,
	getRemainingTimeMs,
	type KernelAvailabilityCacheEntry,
	type KernelStartOptions,
	runKernelProbe,
	throwIfAborted,
} from "../kernel-base";
import { stageRunnerScript } from "../runner-cache";
import { PYTHON_PRELUDE } from "./prelude";
import RUNNER_SCRIPT from "./runner.py" with { type: "text" };
import {
	enumeratePythonRuntimes,
	filterEnv,
	type PythonRuntime,
	resolveExplicitPythonRuntime,
	resolvePythonRuntime,
} from "./runtime";
import { hostHasInheritableConsole, shouldDetachKernel, shouldHideKernelWindow } from "./spawn-options";

export type {
	KernelExecuteOptions,
	KernelExecuteResult,
	KernelRuntimeEnv,
	KernelShutdownOptions,
	KernelShutdownResult,
} from "../kernel-base";

export type { KernelDisplayOutput, PythonStatusEvent } from "./display";
export { renderKernelDisplay } from "./display";

const TRACE_IPC = $flag("PI_PYTHON_IPC_TRACE");

const SHUTDOWN_GRACE_MS = 1_000;
const STARTUP_TIMEOUT_MS = 10_000;
// How long to wait after SIGINT for the runner to emit `done`. If the cell is
// stuck in code that ignores Python signals (e.g. a C extension holding the
// GIL), we escalate to a full subprocess shutdown so the host queue unblocks
// instead of hanging the session forever. The grace window is intentionally
// generous: a clean interrupt is far preferable to losing the persistent
// kernel's state, so we only kill as a last-resort recovery path.
const INTERRUPT_ESCALATION_MS = 5_000;
const BRIDGE_ENV_KEYS = ["PI_TOOL_BRIDGE_URL", "PI_TOOL_BRIDGE_TOKEN", "PI_TOOL_BRIDGE_SESSION"] as const;

export interface PythonKernelAvailability {
	ok: boolean;
	pythonPath?: string;
	reason?: string;
	/** The probed-working runtime, when one was found. */
	runtime?: PythonRuntime;
}

// Cache successful probes per resolved cwd + explicit interpreter: every cell
// otherwise pays one (or two — backend.isAvailable + ensureKernelAvailable)
// interpreter spawns even when the kernel is already hot. Failures are not
// cached so installing a Python mid-session is picked up on the next attempt.
const availabilityCache = new Map<string, KernelAvailabilityCacheEntry<PythonKernelAvailability>>();

export async function checkPythonKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { forceProbe?: boolean; signal?: AbortSignal; deadlineMs?: number },
): Promise<PythonKernelAvailability> {
	throwIfAborted(options?.signal, "Python kernel availability probe cancelled");
	if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now())
		throw createAbortError("TimeoutError", "Python kernel availability probe timed out");
	if (!options?.forceProbe && (isBunTestRuntime() || $flag("PI_PYTHON_SKIP_CHECK"))) {
		return { ok: true };
	}
	const resolvedCwd = path.resolve(cwd);
	const key = `${resolvedCwd}\0${interpreter ?? ""}`;
	// A caller-scoped signal/deadline must never poison a shared cached probe.
	// Only signal-independent checks are reusable by later callers.
	const cacheable = !options?.forceProbe && options?.signal === undefined && options?.deadlineMs === undefined;
	const cached = cacheable ? getCachedKernelAvailability(availabilityCache, key) : undefined;
	if (cached) return await cached;
	const probe = probePythonKernelAvailability(resolvedCwd, interpreter, options);
	if (cacheable) cacheKernelAvailabilityProbe(availabilityCache, key, probe);
	return await probe;
}

async function probePythonKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { signal?: AbortSignal; deadlineMs?: number },
): Promise<PythonKernelAvailability> {
	try {
		const settings = await Settings.init();
		const { env } = settings.getShellConfig();
		const baseEnv = filterEnv(env);
		const runtimes = interpreter
			? [resolveExplicitPythonRuntime(interpreter, cwd, baseEnv)]
			: enumeratePythonRuntimes(cwd, baseEnv);
		if (runtimes.length === 0) {
			return { ok: false, reason: "Python executable not found on PATH" };
		}
		// Probe each candidate in priority order and use the first that actually
		// runs. A managed env left behind by a removed `uv` install can exist on
		// disk yet fail to execute; falling through to the next candidate lets a
		// working system Python take over instead of failing the whole session.
		const failures: string[] = [];
		for (const runtime of runtimes) {
			try {
				// Availability must not hang on a PATH shim. Caller deadlines remain
				// authoritative; the five-second guard is an internal candidate failure.
				const probeDeadline = Math.min(options?.deadlineMs ?? Number.POSITIVE_INFINITY, Date.now() + 5_000);
				const exitCode = await runKernelProbe([runtime.pythonPath, "-c", "import sys;sys.exit(0)"], {
					cwd,
					env: runtime.env,
					signal: options?.signal,
					deadlineMs: probeDeadline,
					label: "Python",
				});
				if (exitCode === 0) {
					return { ok: true, pythonPath: runtime.pythonPath, runtime };
				}
				failures.push(`${runtime.pythonPath} (exit code ${exitCode})`);
			} catch (err) {
				if (options?.signal?.aborted) throwIfAborted(options.signal, "Python kernel probe cancelled");
				throwIfAborted(options?.signal, "Python kernel probe cancelled");
				if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
					throw createAbortError("TimeoutError", "Python kernel probe timed out");
				}
				failures.push(`${runtime.pythonPath} (${err instanceof Error ? err.message : String(err)})`);
			}
		}
		throwIfAborted(options?.signal, "Python kernel probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Python kernel probe timed out");
		}
		return {
			ok: false,
			pythonPath: runtimes[0].pythonPath,
			reason: `No working Python interpreter found. Tried: ${failures.join("; ")}`,
		};
	} catch (err) {
		if (options?.signal?.aborted) throwIfAborted(options.signal, "Python kernel probe cancelled");
		throwIfAborted(options?.signal, "Python kernel probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Python kernel probe timed out");
		}
		return { ok: false, reason: err instanceof Error ? err.message : String(err) };
	}
}

export class PythonKernel extends BaseKernel {
	private constructor(id: string) {
		super(id, {
			languageName: "Python",
			traceIpc: TRACE_IPC,
			exitPayload: JSON.stringify({ type: "exit" }),
			interruptEscalationMs: INTERRUPT_ESCALATION_MS,
			shutdownGraceMs: SHUTDOWN_GRACE_MS,
			buildPayload: (code, msgId, opts) =>
				JSON.stringify({
					id: msgId,
					code,
					cwd: opts?.cwd,
					env: opts?.env,
					silent: opts?.silent ?? false,
					storeHistory: opts?.storeHistory ?? !(opts?.silent ?? false),
				}),
		});
	}

	static async start(options: KernelStartOptions): Promise<PythonKernel> {
		const availability = await logger.time(
			"PythonKernel.start:availabilityCheck",
			checkPythonKernelAvailability,
			options.cwd,
			options.interpreter,
			{ signal: options.signal, deadlineMs: options.deadlineMs },
		);
		if (!availability.ok) {
			throw new Error(availability.reason ?? "Python kernel unavailable");
		}
		throwIfAborted(options.signal, "Python kernel startup cancelled");
		if (options.deadlineMs !== undefined && options.deadlineMs <= Date.now())
			throw createAbortError("TimeoutError", "Python kernel startup timed out");

		let runtime = availability.runtime;
		if (!runtime) {
			const { env: shellEnv } = (await Settings.init()).getShellConfig();
			runtime = options.interpreter
				? resolveExplicitPythonRuntime(options.interpreter, options.cwd, filterEnv(shellEnv))
				: resolvePythonRuntime(options.cwd, filterEnv(shellEnv));
		}
		const spawnEnv: Record<string, string> = {};
		for (const [key, value] of Object.entries(runtime.env)) {
			if (typeof value === "string") spawnEnv[key] = value;
		}
		for (const [key, value] of Object.entries(options.env ?? {})) {
			if (typeof value === "string") spawnEnv[key] = value;
		}
		// Bridge credentials arrive on the request wire and are captured by the runner;
		// never inherit them in the process environment.
		for (const key of BRIDGE_ENV_KEYS) delete spawnEnv[key];
		spawnEnv.PYTHONUNBUFFERED = "1";
		spawnEnv.PYTHONIOENCODING = "utf-8";

		throwIfAborted(options.signal, "Python kernel startup cancelled");
		const scriptPath = await stageRunnerScript("omp-python-runner", "py", RUNNER_SCRIPT);
		const kernel = new PythonKernel(Snowflake.next());

		const proc = Bun.spawn([runtime.pythonPath, "-u", scriptPath], {
			cwd: options.cwd,
			detached: shouldDetachKernel(process.platform),
			env: spawnEnv,
			stdin: "pipe",
			stdout: "pipe",
			stderr: "pipe",
			windowsHide: shouldHideKernelWindow({
				platform: process.platform,
				hostHasInheritableConsole: hostHasInheritableConsole(),
			}),
		});

		kernel.setProcess(proc);

		const startup = { signal: options.signal, deadlineMs: options.deadlineMs };

		try {
			const initScript = buildInitScript(options.cwd, options.env);
			const phaseBudget = () =>
				Math.min(getRemainingTimeMs(startup.deadlineMs) ?? STARTUP_TIMEOUT_MS, STARTUP_TIMEOUT_MS);
			await kernel.executeWithBudget(initScript, startup.signal, phaseBudget(), "Python kernel init");
			await kernel.executeWithBudget(PYTHON_PRELUDE, startup.signal, phaseBudget(), "Python kernel prelude");
			return kernel;
		} catch (err) {
			await kernel.shutdown({ timeoutMs: SHUTDOWN_GRACE_MS }).catch(() => {});
			throw err;
		}
	}
}
function buildInitScript(cwd: string, env?: Record<string, string | undefined>): string {
	const envEntries = Object.entries(env ?? {}).filter(
		([key, value]) => value !== undefined && !BRIDGE_ENV_KEYS.includes(key as (typeof BRIDGE_ENV_KEYS)[number]),
	);
	const envPayload = Object.fromEntries(envEntries);
	return [
		"import os, sys",
		`__omp_cwd = ${JSON.stringify(cwd)}`,
		"os.chdir(__omp_cwd)",
		`__omp_env = ${JSON.stringify(envPayload)}`,
		"for __omp_key, __omp_val in __omp_env.items():\n    os.environ[__omp_key] = __omp_val",
		"if __omp_cwd not in sys.path:\n    sys.path.insert(0, __omp_cwd)",
	].join("\n");
}
