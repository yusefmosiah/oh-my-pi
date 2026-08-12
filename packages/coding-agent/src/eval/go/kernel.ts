/** Persistent subprocess kernel for the optional Yaegi helper. */
import * as path from "node:path";
import { $flag, isBunTestRuntime, logger, Snowflake } from "@oh-my-pi/pi-utils";
import { Settings } from "../../config/settings";
import {
	BaseKernel,
	cacheKernelAvailabilityProbe,
	createAbortError,
	getCachedKernelAvailability,
	getRemainingTimeMs,
	isKernelCancellationError,
	isTimeoutReason,
	type KernelAvailabilityCacheEntry,
	type KernelExecuteResult,
	type KernelStartOptions,
	runKernelProbe,
	throwIfAborted,
} from "../kernel-base";
import type { KernelDisplayOutput } from "../py/display";
import { hostHasInheritableConsole, shouldDetachKernel, shouldHideKernelWindow } from "../py/spawn-options";
import { enumerateGoRuntimes, filterEnv, type GoRuntime, resolveExplicitGoRuntime } from "./runtime";

const TRACE_IPC = $flag("PI_GO_IPC_TRACE");
const SHUTDOWN_GRACE_MS = 1_000;
const STARTUP_TIMEOUT_MS = 10_000;
const INTERRUPT_ESCALATION_MS = 5_000;
const BRIDGE_ENV_KEYS = ["PI_TOOL_BRIDGE_URL", "PI_TOOL_BRIDGE_TOKEN", "PI_TOOL_BRIDGE_SESSION"] as const;

export interface GoKernelExecuteOptions {
	id?: string;
	cwd?: string;
	env?: Record<string, string | undefined> | Record<string, string | null>;
	signal?: AbortSignal;
	onChunk?: (text: string) => Promise<void> | void;
	onDisplay?: (output: KernelDisplayOutput) => Promise<void> | void;
	timeoutMs?: number;
	silent?: boolean;
	storeHistory?: boolean;
	onRequestWriteStarted?: () => void;
	onRequestWritten?: () => void;
}

export interface GoKernelAvailability {
	ok: boolean;
	runnerPath?: string;
	reason?: string;
	runtime?: GoRuntime;
}

const availabilityCache = new Map<string, KernelAvailabilityCacheEntry<GoKernelAvailability>>();

export async function checkGoKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { forceProbe?: boolean; signal?: AbortSignal; deadlineMs?: number },
): Promise<GoKernelAvailability> {
	throwIfAborted(options?.signal, "Go/Yaegi helper probe cancelled");
	if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now())
		throw createAbortError("TimeoutError", "Go/Yaegi helper probe timed out");
	if (!options?.forceProbe && (isBunTestRuntime() || $flag("PI_GO_SKIP_CHECK"))) return { ok: true };
	const key = `${path.resolve(cwd)}\0${interpreter ?? ""}`;
	// Caller-scoped cancellation/deadlines must never poison a shared probe.
	// An options object with only undefined fields is still unscoped and cacheable.
	const cacheable = !options?.forceProbe && options?.signal === undefined && options?.deadlineMs === undefined;
	const cached = cacheable ? getCachedKernelAvailability(availabilityCache, key) : undefined;
	if (cached) return await cached;
	const probe = probeGoKernelAvailability(cwd, interpreter, options);
	if (cacheable) cacheKernelAvailabilityProbe(availabilityCache, key, probe);
	return await probe;
}

async function probeGoKernelAvailability(
	cwd: string,
	interpreter?: string,
	options?: { signal?: AbortSignal; deadlineMs?: number },
): Promise<GoKernelAvailability> {
	try {
		const settings = await Settings.init();
		const { env: shellEnv } = settings.getShellConfig();
		const baseEnv = filterEnv(shellEnv);
		const runtimes = enumerateGoRuntimes(cwd, baseEnv, interpreter);
		if (runtimes.length === 0) {
			return {
				ok: false,
				reason: "Yaegi helper not found. Build omp-eval-go-runner and set go.interpreter to its executable path.",
			};
		}
		const failures: string[] = [];
		for (const runtime of runtimes) {
			try {
				// Keep each candidate probe bounded even without a caller deadline.
				const probeDeadline = Math.min(options?.deadlineMs ?? Number.POSITIVE_INFINITY, Date.now() + 5_000);
				const exitCode = await runKernelProbe([runtime.runnerPath, "--version"], {
					cwd,
					env: runtime.env,
					signal: options?.signal,
					deadlineMs: probeDeadline,
					label: "Go/Yaegi",
				});
				if (exitCode === 0) return { ok: true, runnerPath: runtime.runnerPath, runtime };
				failures.push(`${runtime.runnerPath} (exit code ${exitCode})`);
			} catch (err) {
				// Only caller-owned cancellation/deadlines escape the candidate loop.
				// The five-second per-candidate guard is an availability failure and
				// must not reject the whole check or poison the shared cache.
				if (options?.signal?.aborted) throwIfAborted(options.signal, "Go/Yaegi helper probe cancelled");
				if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
					throw createAbortError("TimeoutError", "Go/Yaegi helper probe timed out");
				}
				if (!isKernelCancellationError(err)) {
					throwIfAborted(options?.signal, "Go/Yaegi helper probe cancelled");
				}

				failures.push(`${runtime.runnerPath} (${err instanceof Error ? err.message : String(err)})`);
			}
		}
		throwIfAborted(options?.signal, "Go/Yaegi helper probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Go/Yaegi helper probe timed out");
		}
		return {
			ok: false,
			runnerPath: runtimes[0].runnerPath,
			reason: `No working Yaegi helper found. Tried: ${failures.join("; ")}`,
		};
	} catch (err) {
		if (options?.signal?.aborted) throwIfAborted(options.signal, "Go/Yaegi helper probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Go/Yaegi helper probe timed out");
		}
		// Internal candidate timeouts and other launch errors are reported as an
		// unavailable helper rather than escaping as cancellation.
		if (!isKernelCancellationError(err)) throwIfAborted(options?.signal, "Go/Yaegi helper probe cancelled");
		if (options?.deadlineMs !== undefined && options.deadlineMs <= Date.now()) {
			throw createAbortError("TimeoutError", "Go/Yaegi helper probe timed out");
		}
		return { ok: false, reason: err instanceof Error ? err.message : String(err) };
	}
}

export class GoKernel extends BaseKernel<GoKernelExecuteOptions> {
	// The NDJSON Go runner executes one request synchronously. Serialize calls
	// here so aborting a queued cell cannot send SIGINT to an earlier active cell.
	#executionTail: Promise<void> = Promise.resolve();

	override async execute(code: string, options?: GoKernelExecuteOptions): Promise<KernelExecuteResult> {
		// A Go request waits behind the previous synchronous Yaegi request. Keep
		// cancellation and timeout semantics tied to the call (not to the moment
		// it finally reaches BaseKernel), otherwise a queued cell can run long
		// after its wall-clock budget has expired.
		const sourceSignal = options?.signal;
		const hasTimeout = typeof options?.timeoutMs === "number" && options.timeoutMs > 0;
		const controller = sourceSignal || hasTimeout ? new AbortController() : undefined;
		let removeSourceAbort: (() => void) | undefined;
		let timeoutId: ReturnType<typeof setTimeout> | undefined;
		let requestOptions = options;
		if (controller) {
			const abortFromSource = () => {
				if (!controller.signal.aborted) controller.abort(sourceSignal?.reason);
			};
			if (sourceSignal) {
				if (sourceSignal.aborted) abortFromSource();
				else {
					sourceSignal.addEventListener("abort", abortFromSource, { once: true });
					removeSourceAbort = () => sourceSignal.removeEventListener("abort", abortFromSource);
				}
			}
			if (hasTimeout) {
				timeoutId = setTimeout(() => {
					if (!controller.signal.aborted) {
						controller.abort(
							createAbortError(
								"TimeoutError",
								`Go/Yaegi execution timed out after ${options?.timeoutMs ?? 0}ms`,
							),
						);
					}
				}, options?.timeoutMs);
				timeoutId.unref?.();
			}
			// BaseKernel owns the actual interrupt and must observe the same
			// controller that also cancels a request while it is queued. Its own
			// timeout timer is disabled to avoid starting a second budget later.
			requestOptions = {
				...options,
				signal: controller.signal,
				timeoutMs: undefined,
				onRequestWriteStarted: () => {
					dispatched = true;
					options?.onRequestWriteStarted?.();
				},
				onRequestWritten: () => {
					options?.onRequestWritten?.();
				},
			};
		}

		const previous = this.#executionTail;
		let release!: () => void;
		const turn = new Promise<void>(resolve => {
			release = resolve;
		});
		this.#executionTail = previous.then(
			() => turn,
			() => turn,
		);
		let dispatched = false;
		const run = previous.then(async () => {
			if (controller?.signal.aborted) {
				return {
					status: "error" as const,
					cancelled: true,
					timedOut: isTimeoutReason(controller.signal.reason),
					stdinRequested: false,
				};
			}
			if (sourceSignal?.aborted) {
				return {
					status: "error" as const,
					cancelled: true,
					timedOut: isTimeoutReason(sourceSignal.reason),
					stdinRequested: false,
				};
			}
			const result = await super.execute(code, requestOptions);
			if (dispatched && result.cancelled) {
				// The Yaegi runner marks itself poisoned and exits after a cancelled
				// cell. Do not release the serialization tail on its `done` frame:
				// a following request sent in that window is silently discarded by
				// the runner and can leave the registry with a dead kernel that still
				// reports itself alive.
				await this.waitForProcessExit(INTERRUPT_ESCALATION_MS);
				// A poisoned Yaegi runner exits after its cancelled done frame. Always
				// await shutdown, even when the reader has already marked the kernel
				// dead: `isAlive()` is host lifecycle state, not proof that the OS child
				// has been reaped. Keeping this in the Go tail prevents a following
				// request from racing a late exit or stale stdin write.
				await this.shutdown({ timeoutMs: SHUTDOWN_GRACE_MS }).catch(() => undefined);
			}
			return result;
		});
		let removeControllerAbort: (() => void) | undefined;
		try {
			if (!controller) return await run;
			const aborted = new Promise<KernelExecuteResult>(resolve => {
				const onAbort = () => {
					removeControllerAbort?.();
					resolve({
						status: "error",
						cancelled: true,
						timedOut: isTimeoutReason(controller.signal.reason),
						stdinRequested: false,
					});
				};
				if (controller.signal.aborted) onAbort();
				else {
					controller.signal.addEventListener("abort", onAbort, { once: true });
					removeControllerAbort = () => controller.signal.removeEventListener("abort", onAbort);
				}
			});
			return await Promise.race([run, aborted]);
		} finally {
			removeControllerAbort?.();
			removeSourceAbort?.();
			if (timeoutId) clearTimeout(timeoutId);
			void run.then(release, release);
		}
	}

	private constructor(id: string) {
		super(id, {
			languageName: "Go/Yaegi",
			traceIpc: TRACE_IPC,
			exitPayload: JSON.stringify({ type: "exit" }),
			interruptEscalationMs: INTERRUPT_ESCALATION_MS,
			shutdownGraceMs: SHUTDOWN_GRACE_MS,
			readyPayload: JSON.stringify({ type: "ready" }),
			buildPayload: (code, msgId, opts) => {
				const env = opts?.env;
				const bridgeUrl = env?.PI_TOOL_BRIDGE_URL;
				const bridgeSession = env?.PI_TOOL_BRIDGE_SESSION;
				const requestEnv = env
					? Object.fromEntries(
							Object.entries(env).filter(
								([key]) => !BRIDGE_ENV_KEYS.includes(key as (typeof BRIDGE_ENV_KEYS)[number]),
							),
						)
					: undefined;
				return JSON.stringify({
					id: msgId,
					code,
					cwd: opts?.cwd,
					env: requestEnv,
					bridgeUrl: typeof bridgeUrl === "string" ? bridgeUrl : undefined,
					bridgeSession: typeof bridgeSession === "string" ? bridgeSession : undefined,
					silent: opts?.silent ?? false,
					storeHistory: opts?.storeHistory ?? !(opts?.silent ?? false),
				});
			},
		});
	}

	static async start(options: KernelStartOptions): Promise<GoKernel> {
		const availability = await logger.time(
			"GoKernel.start:availabilityCheck",
			checkGoKernelAvailability,
			options.cwd,
			options.interpreter,
			{ signal: options.signal, deadlineMs: options.deadlineMs },
		);
		if (!availability.ok) {
			if (getRemainingTimeMs(options.deadlineMs) === 0) {
				throw createAbortError("TimeoutError", "Go/Yaegi kernel startup timed out");
			}
			throw new Error(availability.reason ?? "Yaegi helper unavailable");
		}
		let runtime = availability.runtime;
		if (!runtime) {
			const { env: shellEnv } = (await Settings.init()).getShellConfig();
			runtime = options.interpreter
				? resolveExplicitGoRuntime(options.interpreter, options.cwd, filterEnv(shellEnv))
				: enumerateGoRuntimes(options.cwd, filterEnv(shellEnv))[0];
		}
		if (!runtime) throw new Error("Yaegi helper unavailable");
		throwIfAborted(options.signal, "Go/Yaegi kernel startup cancelled");
		if (getRemainingTimeMs(options.deadlineMs) === 0) {
			throw createAbortError("TimeoutError", "Go/Yaegi kernel startup timed out");
		}
		const spawnEnv: Record<string, string> = {};
		for (const [key, value] of Object.entries(runtime.env))
			if (typeof value === "string" && !BRIDGE_ENV_KEYS.includes(key as (typeof BRIDGE_ENV_KEYS)[number]))
				spawnEnv[key] = value;
		for (const [key, value] of Object.entries(options.env ?? {}))
			if (typeof value === "string" && !BRIDGE_ENV_KEYS.includes(key as (typeof BRIDGE_ENV_KEYS)[number]))
				spawnEnv[key] = value;
		throwIfAborted(options.signal, "Go/Yaegi kernel startup cancelled");
		if (getRemainingTimeMs(options.deadlineMs) === 0) {
			throw createAbortError("TimeoutError", "Go/Yaegi kernel startup timed out");
		}
		const kernel = new GoKernel(Snowflake.next());
		const proc = Bun.spawn([runtime.runnerPath], {
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
		const startupBudget = Math.min(getRemainingTimeMs(options.deadlineMs) ?? STARTUP_TIMEOUT_MS, STARTUP_TIMEOUT_MS);
		try {
			await kernel.waitUntilReady(options.signal, startupBudget);
			return kernel;
		} catch (error) {
			const remaining = getRemainingTimeMs(options.deadlineMs);
			await kernel
				.shutdown({ timeoutMs: Math.max(0, Math.min(SHUTDOWN_GRACE_MS, remaining ?? SHUTDOWN_GRACE_MS)) })
				.catch(() => {});
			throw error;
		}
	}
}
