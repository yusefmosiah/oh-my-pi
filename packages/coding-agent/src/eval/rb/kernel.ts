/**
 * Subprocess-backed Ruby runner.
 *
 * Speaks NDJSON with `runner.rb` over stdin/stdout. One subprocess per kernel
 * instance; sessions reuse a single subprocess across executions. Cancellation
 * is delivered as SIGINT (clean interrupt, kernel state preserved) and escalates
 * to a full shutdown only when the runner ignores it. Mirrors the Python kernel
 * (eval/py/kernel.ts); the IPC loop, lifecycle, and display rendering are shared
 * with it via BaseKernel.
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
	type KernelRuntimeEnv,
	type KernelStartOptions,
	runKernelProbe,
	throwIfAborted,
} from "../kernel-base";
import type { KernelDisplayOutput } from "../py/display";
import { hostHasInheritableConsole, shouldDetachKernel, shouldHideKernelWindow } from "../py/spawn-options";
import { stageRunnerScript } from "../runner-cache";
import { RUBY_PRELUDE } from "./prelude";
import RUNNER_SCRIPT from "./runner.rb" with { type: "text" };
import {
	enumerateRubyRuntimes,
	filterEnv,
	type RubyRuntime,
	resolveExplicitRubyRuntime,
	resolveRubyRuntime,
} from "./runtime";

export type { KernelExecuteResult, KernelRuntimeEnv, KernelShutdownResult } from "../kernel-base";
export type { KernelDisplayOutput, PythonStatusEvent } from "../py/display";
export { renderKernelDisplay } from "../py/display";

const TRACE_IPC = $flag("PI_RUBY_IPC_TRACE");

const SHUTDOWN_GRACE_MS = 1_000;
const STARTUP_TIMEOUT_MS = 10_000;
// How long to wait after SIGINT for the runner to emit `done` before escalating
// to a full subprocess shutdown so the host queue unblocks instead of hanging.
const INTERRUPT_ESCALATION_MS = 5_000;
const BRIDGE_ENV_KEYS = ["PI_TOOL_BRIDGE_URL", "PI_TOOL_BRIDGE_TOKEN", "PI_TOOL_BRIDGE_SESSION"] as const;

export interface KernelExecuteOptions {
	id?: string;
	/** Runtime working directory applied immediately before this request executes. */
	cwd?: string;
	/** Managed runtime environment variables applied immediately before this request executes. */
	env?: KernelRuntimeEnv;
	signal?: AbortSignal;
	onChunk?: (text: string) => Promise<void> | void;
	onDisplay?: (output: KernelDisplayOutput) => Promise<void> | void;
	timeoutMs?: number;
	silent?: boolean;
	storeHistory?: boolean;
}

export interface RubyKernelAvailability {
	ok: boolean;
	rubyPath?: string;
	reason?: string;
	/** The probed-working runtime, when one was found. */
	runtime?: RubyRuntime;
}

// Cache successful probes per resolved cwd + explicit interpreter. Failures are
// not cached so installing Ruby mid-session is picked up on the next attempt.
const availabilityCache = new Map<string, KernelAvailabilityCacheEntry<RubyKernelAvailability>>();

export async function checkRubyKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { forceProbe?: boolean; signal?: AbortSignal; deadlineMs?: number },
): Promise<RubyKernelAvailability> {
	throwIfAborted(options?.signal, "Ruby kernel availability probe cancelled");
	if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now())
		throw createAbortError("TimeoutError", "Ruby kernel availability probe timed out");
	if (!options?.forceProbe && (isBunTestRuntime() || $flag("PI_RUBY_SKIP_CHECK"))) {
		return { ok: true };
	}
	const resolvedCwd = path.resolve(cwd);
	const key = `${resolvedCwd}\0${interpreter ?? ""}`;
	const cacheable = !options?.forceProbe && options?.signal === undefined && options?.deadlineMs === undefined;
	const cached = cacheable ? getCachedKernelAvailability(availabilityCache, key) : undefined;
	if (cached) return await cached;
	const probe = probeRubyKernelAvailability(resolvedCwd, interpreter, options);
	if (cacheable) cacheKernelAvailabilityProbe(availabilityCache, key, probe);
	return await probe;
}

async function probeRubyKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { signal?: AbortSignal; deadlineMs?: number },
): Promise<RubyKernelAvailability> {
	try {
		const settings = await Settings.init();
		const { env } = settings.getShellConfig();
		const baseEnv = filterEnv(env);
		const runtimes = enumerateRubyRuntimes(cwd, baseEnv, interpreter);
		if (runtimes.length === 0) {
			return { ok: false, reason: "Ruby executable not found on PATH" };
		}
		const failures: string[] = [];
		for (const runtime of runtimes) {
			try {
				// Availability must not hang on a PATH shim. Caller deadlines remain
				// authoritative; the five-second guard is an internal candidate failure.
				const probeDeadline = Math.min(options?.deadlineMs ?? Number.POSITIVE_INFINITY, Date.now() + 5_000);
				const exitCode = await runKernelProbe([runtime.rubyPath, "-e", "exit 0"], {
					cwd,
					env: runtime.env,
					signal: options?.signal,
					deadlineMs: probeDeadline,
					label: "Ruby",
				});
				if (exitCode === 0) {
					return { ok: true, rubyPath: runtime.rubyPath, runtime };
				}
				failures.push(`${runtime.rubyPath} (exit code ${exitCode})`);
			} catch (err) {
				if (options?.signal?.aborted) throwIfAborted(options.signal, "Ruby kernel probe cancelled");
				throwIfAborted(options?.signal, "Ruby kernel probe cancelled");
				if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
					throw createAbortError("TimeoutError", "Ruby kernel probe timed out");
				}
				failures.push(`${runtime.rubyPath} (${err instanceof Error ? err.message : String(err)})`);
			}
		}
		throwIfAborted(options?.signal, "Ruby kernel probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Ruby kernel probe timed out");
		}
		return {
			ok: false,
			rubyPath: runtimes[0].rubyPath,
			reason: `No working Ruby interpreter found. Tried: ${failures.join("; ")}`,
		};
	} catch (err) {
		if (options?.signal?.aborted) throwIfAborted(options.signal, "Ruby kernel probe cancelled");
		throwIfAborted(options?.signal, "Ruby kernel probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Ruby kernel probe timed out");
		}
		return { ok: false, reason: err instanceof Error ? err.message : String(err) };
	}
}

export class RubyKernel extends BaseKernel<KernelExecuteOptions> {
	private constructor(id: string) {
		super(id, {
			languageName: "Ruby",
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

	static async start(options: KernelStartOptions): Promise<RubyKernel> {
		const availability = await logger.time(
			"RubyKernel.start:availabilityCheck",
			checkRubyKernelAvailability,
			options.cwd,
			options.interpreter,
			{ signal: options.signal, deadlineMs: options.deadlineMs },
		);
		if (!availability.ok) {
			throw new Error(availability.reason ?? "Ruby kernel unavailable");
		}
		throwIfAborted(options.signal, "Ruby kernel startup cancelled");
		if (options.deadlineMs !== undefined && options.deadlineMs <= Date.now())
			throw createAbortError("TimeoutError", "Ruby kernel startup timed out");

		// Reuse the interpreter the availability probe selected. The fallback
		// computes a runtime only for the skip-check fast path (test runtime /
		// PI_RUBY_SKIP_CHECK), where no candidate was probed.
		let runtime = availability.runtime;
		if (!runtime) {
			const { env: shellEnv } = (await Settings.init()).getShellConfig();
			runtime = options.interpreter
				? resolveExplicitRubyRuntime(options.interpreter, options.cwd, filterEnv(shellEnv))
				: resolveRubyRuntime(options.cwd, filterEnv(shellEnv));
		}
		const spawnEnv: Record<string, string> = {};
		for (const key in runtime.env) {
			const value = runtime.env[key];
			if (typeof value === "string") spawnEnv[key] = value;
		}
		for (const key in options.env) {
			const value = options.env[key];
			if (typeof value === "string") spawnEnv[key] = value;
		}
		// Bridge credentials arrive on the request wire and are captured by the runner;
		// never inherit them in the process environment.
		for (const key of BRIDGE_ENV_KEYS) delete spawnEnv[key];

		throwIfAborted(options.signal, "Ruby kernel startup cancelled");
		const scriptPath = await stageRunnerScript("omp-ruby-runner", "rb", RUNNER_SCRIPT);
		const kernel = new RubyKernel(Snowflake.next());

		const proc = Bun.spawn([runtime.rubyPath, scriptPath], {
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
			await kernel.executeWithBudget(initScript, startup.signal, phaseBudget(), "Ruby kernel init");
			await kernel.executeWithBudget(RUBY_PRELUDE, startup.signal, phaseBudget(), "Ruby kernel prelude");
			return kernel;
		} catch (err) {
			await kernel.shutdown({ timeoutMs: SHUTDOWN_GRACE_MS }).catch(() => {});
			throw err;
		}
	}
}

function buildInitScript(cwd: string, env?: Record<string, string | undefined>): string {
	const envPayload: Record<string, string> = {};
	for (const key in env) {
		const value = env[key];
		if (value !== undefined && !BRIDGE_ENV_KEYS.includes(key as (typeof BRIDGE_ENV_KEYS)[number]))
			envPayload[key] = value;
	}
	// JSON string literals are valid Ruby string literals. Emit one
	// `ENV["k"] = "v"` per key — a `{"k":"v"}` object literal would parse as a
	// SYMBOL-keyed hash in Ruby (`:"k" => "v"`), which `ENV[]=` rejects.
	const lines = [`__omp_init_cwd = ${JSON.stringify(cwd)}`, "Dir.chdir(__omp_init_cwd) rescue nil"];
	for (const key in envPayload) {
		lines.push(`ENV[${JSON.stringify(key)}] = ${JSON.stringify(envPayload[key])}`);
	}
	lines.push("$LOAD_PATH.delete(__omp_init_cwd)", "$LOAD_PATH.unshift(__omp_init_cwd)");
	return lines.join("\n");
}
