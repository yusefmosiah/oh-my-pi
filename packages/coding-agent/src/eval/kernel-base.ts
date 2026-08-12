import { logger, Snowflake } from "@oh-my-pi/pi-utils";
import type { Subprocess } from "bun";
import { type KernelDisplayOutput, renderKernelDisplay } from "./py/display";

export type KernelRuntimeEnv = Record<string, string | null>;

export interface KernelExecuteOptions {
	id?: string;
	/** Runtime working directory applied immediately before this request executes. */
	cwd?: string;
	/** Managed runtime environment variables applied immediately before this request executes. */
	env?: Record<string, string | undefined> | Record<string, string | null>;
	signal?: AbortSignal;
	onChunk?: (text: string) => Promise<void> | void;
	onDisplay?: (output: KernelDisplayOutput) => Promise<void> | void;
	/** Non-positive values are treated as an already-expired timeout. */
	timeoutMs?: number;
	silent?: boolean;
	storeHistory?: boolean;
	allowStdin?: boolean;
	/** Internal lifecycle hook fired immediately before bytes for this request are written. */
	onRequestWriteStarted?: () => void;
	/** Internal lifecycle hook fired once this request is committed to stdin. */
	onRequestWritten?: () => void;
}

export interface KernelExecuteResult {
	status: "ok" | "error";
	executionCount?: number;
	error?: { name: string; value: string; traceback: string[] };
	cancelled: boolean;
	timedOut: boolean;
	stdinRequested: boolean;
	/**
	 * True when the kernel subprocess was killed as part of settling this
	 * execution (e.g. SIGINT was ignored and we escalated to shutdown, or the
	 * kernel died unexpectedly). When false, the kernel remains reusable.
	 */
	kernelKilled?: boolean;
}

export interface KernelShutdownResult {
	confirmed: boolean;
}

export interface KernelShutdownOptions {
	signal?: AbortSignal;
	timeoutMs?: number;
}

/** Per-language lifecycle configuration consumed by each kernel's `start()`. */
export interface KernelStartOptions {
	cwd: string;
	env?: Record<string, string | undefined>;
	/** Explicit interpreter path; skips discovery when set. */
	interpreter?: string;
	signal?: AbortSignal;
	deadlineMs?: number;
}

/** Per-language configuration handed to {@link BaseKernel} by each subclass. */
export interface BaseKernelOptions<TExecuteOptions extends KernelExecuteOptions = KernelExecuteOptions> {
	/** Human-readable language label used in log messages and errors. */
	languageName: string;
	/** When true, every IPC frame is logged at debug level. */
	traceIpc: boolean;
	/** Wire payload asking the runner to exit cleanly. */
	exitPayload: string;
	/** How long to wait after SIGINT before escalating to subprocess termination. */
	interruptEscalationMs: number;
	/** Default grace period applied by {@link BaseKernel.shutdown}. */
	shutdownGraceMs: number;
	/** Serializes an execution request into the runner's wire protocol. */
	buildPayload: (code: string, msgId: string, options?: TExecuteOptions) => string;
	/** Optional request used to prove that a persistent runner finished initialization. */
	readyPayload?: string;
}

export type FrameType = "ready" | "started" | "stdout" | "stderr" | "display" | "result" | "error" | "done";

export interface Frame {
	type: FrameType;
	id?: string;
	data?: string;
	bundle?: Record<string, unknown>;
	ename?: string;
	evalue?: string;
	traceback?: string[];
	status?: "ok" | "error";
	executionCount?: number;
	cancelled?: boolean;
}

interface PendingExecution {
	id: string;
	resolve: (result: KernelExecuteResult) => void;
	options?: KernelExecuteOptions;
	status: "ok" | "error";
	executionCount?: number;
	error?: { name: string; value: string; traceback: string[] };
	cancelled: boolean;
	timedOut: boolean;
	stdinRequested: boolean;
	kernelKilled: boolean;
	settled: boolean;
	escalationTimer?: NodeJS.Timeout;
	finalize?: () => void;
}

export function getRemainingTimeMs(deadlineMs?: number): number | undefined {
	if (deadlineMs === undefined) return undefined;
	return Math.max(0, deadlineMs - Date.now());
}

export function createAbortError(name: "AbortError" | "TimeoutError", message: string): Error {
	const err = new Error(message);
	err.name = name;
	return err;
}

export function throwIfAborted(signal: AbortSignal | undefined, fallbackReason: string): void {
	if (!signal?.aborted) return;
	const reason = signal.reason;
	if (reason instanceof Error) throw reason;
	throw createAbortError("AbortError", typeof reason === "string" ? reason : fallbackReason);
}

/**
 * Successful interpreter availability probes are cheap to refresh but should
 * still be shared between the backend check and kernel startup. Keep the
 * success cache bounded so replacing/removing a runtime is eventually picked
 * up without requiring every cell to spawn a subprocess.
 */
export const KERNEL_AVAILABILITY_CACHE_TTL_MS = 5 * 60 * 1000;

export interface KernelAvailabilityCacheEntry<T extends { ok: boolean }> {
	promise: Promise<T>;
	/** Monotonic wall-clock expiry; Infinity while the probe is in flight. */
	expiresAt: number;
}

/**
 * Return an unexpired availability probe. In-flight probes are always shared;
 * expiry starts only after a successful result so a slow interpreter startup
 * cannot cause a thundering herd of duplicate probes.
 */
export function getCachedKernelAvailability<T extends { ok: boolean }>(
	cache: Map<string, KernelAvailabilityCacheEntry<T>>,
	key: string,
): Promise<T> | undefined {
	const entry = cache.get(key);
	if (!entry) return undefined;
	if (entry.expiresAt <= Date.now()) {
		cache.delete(key);
		return undefined;
	}
	return entry.promise;
}

/**
 * Store a probe while it is in flight, then retain successful results for the
 * bounded TTL. Failed/rejected probes are removed immediately so a runtime
 * installed or repaired during a session is discovered on the next attempt.
 */
export function cacheKernelAvailabilityProbe<T extends { ok: boolean }>(
	cache: Map<string, KernelAvailabilityCacheEntry<T>>,
	key: string,
	probe: Promise<T>,
	ttlMs = KERNEL_AVAILABILITY_CACHE_TTL_MS,
): KernelAvailabilityCacheEntry<T> {
	const entry: KernelAvailabilityCacheEntry<T> = { promise: probe, expiresAt: Number.POSITIVE_INFINITY };
	cache.set(key, entry);
	void probe.then(
		result => {
			if (cache.get(key) !== entry) return;
			if (result.ok) entry.expiresAt = Date.now() + ttlMs;
			else cache.delete(key);
		},
		() => {
			if (cache.get(key) === entry) cache.delete(key);
		},
	);
	return entry;
}

/** Options for a short-lived interpreter availability probe. */
export interface KernelProbeOptions {
	cwd?: string;
	env?: Record<string, string | undefined>;
	signal?: AbortSignal;
	/** Absolute wall-clock deadline in milliseconds since epoch. */
	deadlineMs?: number;
	label?: string;
}

/**
 * Run an interpreter probe without routing it through Bun's `$` shell helper.
 *
 * `$` has no cancellation hook: a version-manager shim or a broken runtime can
 * leave an availability check pending forever even after the owning eval turn
 * is aborted.  Bun.spawn gives us a process handle that can be killed on either
 * caller cancellation or an absolute deadline.  stdio is ignored deliberately;
 * availability only needs the exit status and must not buffer arbitrary runtime
 * output in the host.
 */
export async function runKernelProbe(command: string[], options: KernelProbeOptions = {}): Promise<number> {
	throwIfAborted(options.signal, `${options.label ?? "Kernel"} probe cancelled`);
	const remainingMs = getRemainingTimeMs(options.deadlineMs);
	if (remainingMs !== undefined && remainingMs <= 0) {
		throw createAbortError("TimeoutError", `${options.label ?? "Kernel"} probe timed out`);
	}

	const proc = Bun.spawn(command, {
		cwd: options.cwd,
		env: options.env,
		stdin: "ignore",
		stdout: "ignore",
		stderr: "ignore",
	});

	return await new Promise<number>((resolve, reject) => {
		let settled = false;
		let timer: ReturnType<typeof setTimeout> | undefined;
		let removeAbort: (() => void) | undefined;
		const label = options.label ?? "Kernel";

		const cleanup = (): void => {
			if (timer) clearTimeout(timer);
			removeAbort?.();
		};
		const finish = (callback: () => void): void => {
			if (settled) return;
			settled = true;
			cleanup();
			callback();
		};
		const cancel = (error: Error): void => {
			if (settled) return;
			try {
				proc.kill("SIGKILL");
			} catch {
				// The process may have exited between the race and kill().
			}
			// Do not leave a rejected/unobserved exited promise behind if a
			// platform takes a moment to reap the killed child.
			void proc.exited.catch(() => undefined);
			finish(() => reject(error));
		};

		void proc.exited.then(
			code => finish(() => resolve(code)),
			error => finish(() => reject(error)),
		);

		if (options.signal) {
			const onAbort = (): void => {
				const reason = options.signal?.reason;
				cancel(reason instanceof Error ? reason : createAbortError("AbortError", `${label} probe cancelled`));
			};
			if (options.signal.aborted) onAbort();
			else {
				options.signal.addEventListener("abort", onAbort, { once: true });
				removeAbort = () => options.signal?.removeEventListener("abort", onAbort);
			}
		}
		if (remainingMs !== undefined && !settled) {
			timer = setTimeout(() => cancel(createAbortError("TimeoutError", `${label} probe timed out`)), remainingMs);
			timer.unref?.();
		}
	});
}

export function isTimeoutReason(reason: unknown): boolean {
	if (reason instanceof DOMException) return reason.name === "TimeoutError";
	if (reason instanceof Error) return reason.name === "TimeoutError";
	return false;
}

/** True for errors produced by an AbortSignal or a probe deadline. */
export function isKernelCancellationError(error: unknown): boolean {
	if (!error || typeof error !== "object" || !("name" in error)) return false;
	const name = (error as { name?: unknown }).name;
	return name === "AbortError" || name === "TimeoutError";
}

/**
 * Shared subprocess-backed kernel machinery for the language runners. Each
 * language subclasses this, supplying its binary/runner via a static `start()`
 * and its wire protocol via {@link BaseKernelOptions.buildPayload}. The IPC loop
 * speaks NDJSON: the runner emits one JSON {@link Frame} per line; outbound
 * requests are serialized by `buildPayload` (which may itself be NDJSON or any
 * other line-delimited encoding).
 *
 * `TExecuteOptions` is the language's own execute-options type so each runner's
 * `buildPayload` sees its precise option shape (e.g. environment-map variants).
 */
const REDACTED_IPC_VALUE = "[REDACTED]";
const SENSITIVE_IPC_KEYS = new Set([
	"PI_TOOL_BRIDGE_URL",
	"PI_TOOL_BRIDGE_TOKEN",
	"PI_TOOL_BRIDGE_SESSION",
	"bridgeUrl",
	"bridgeToken",
	"bridgeSession",
]);

function redactIpcPayload(line: string): string {
	try {
		const value: unknown = JSON.parse(line);
		const redact = (node: unknown): unknown => {
			if (Array.isArray(node)) return node.map(redact);
			if (!node || typeof node !== "object") return node;
			const out: Record<string, unknown> = {};
			for (const [key, child] of Object.entries(node))
				out[key] = SENSITIVE_IPC_KEYS.has(key) ? REDACTED_IPC_VALUE : redact(child);
			return out;
		};
		return JSON.stringify(redact(value));
	} catch {
		return line.replace(
			/(PI_TOOL_BRIDGE_(?:URL|TOKEN|SESSION)|bridge(?:Url|Token|Session))("?\s*[:=]\s*"?)[^,}\s]+/gi,
			`$1$2${REDACTED_IPC_VALUE}`,
		);
	}
}

export abstract class BaseKernel<TExecuteOptions extends KernelExecuteOptions = KernelExecuteOptions> {
	readonly id: string;
	#proc: Subprocess | null = null;
	#stdin: Bun.FileSink | null = null;
	#alive = true;
	#disposed = false;
	#shutdownConfirmed = false;
	#shutdownPromise: Promise<KernelShutdownResult> | null = null;
	#exitedPromise: Promise<number> | null = null;
	#readyPromise: Promise<void> | null = null;
	#resolveReady: (() => void) | null = null;
	#rejectReady: ((error: unknown) => void) | null = null;
	#ready = false;
	#pending = new Map<string, PendingExecution>();
	#readBuffer = "";
	// A subprocess runner owns process-global interpreter state (cwd, env,
	// namespace, signal handler, and output capture). Serialize requests here,
	// while allowing cancellation of a request that is still waiting in line.
	#executionTail: Promise<void> = Promise.resolve();
	readonly #options: BaseKernelOptions<TExecuteOptions>;

	constructor(id: string, options: BaseKernelOptions<TExecuteOptions>) {
		this.id = id;
		this.#options = options;
		if (options.readyPayload) {
			const ready = Promise.withResolvers<void>();
			this.#readyPromise = ready.promise;
			this.#resolveReady = ready.resolve;
			this.#rejectReady = ready.reject;
		} else {
			this.#ready = true;
		}
	}

	setProcess(proc: Subprocess<"pipe", "pipe", "pipe">) {
		this.#proc = proc;
		this.#stdin = proc.stdin;
		this.#exitedPromise = proc.exited;
		void this.#exitedPromise.then(code => {
			this.#markDead(`${this.#options.languageName} kernel exited with code ${code}`, { kernelKilled: true });
		});

		this.#startReader(proc.stdout as ReadableStream<Uint8Array>);
		this.#startStderrDrain(proc.stderr as ReadableStream<Uint8Array>);
		if (this.#options.readyPayload) {
			void this.#writeLine(this.#options.readyPayload).catch(error => {
				this.#rejectReady?.(error);
			});
		}
	}

	async waitUntilReady(signal?: AbortSignal, timeoutMs?: number): Promise<void> {
		throwIfAborted(signal, `${this.#options.languageName} kernel startup cancelled`);
		if (timeoutMs !== undefined && timeoutMs <= 0) {
			throw createAbortError("TimeoutError", `${this.#options.languageName} kernel startup timed out`);
		}
		if (!this.isAlive()) {
			throw new Error(`${this.#options.languageName} kernel is not running`);
		}
		if (this.#ready) return;
		const readyPromise = this.#readyPromise;
		if (!readyPromise) return;
		let timer: ReturnType<typeof setTimeout> | undefined;
		let removeAbort: (() => void) | undefined;
		const timeout =
			timeoutMs === undefined
				? undefined
				: new Promise<never>((_, reject) => {
						timer = setTimeout(
							() =>
								reject(
									createAbortError("TimeoutError", `${this.#options.languageName} kernel startup timed out`),
								),
							timeoutMs,
						);
						timer.unref?.();
					});
		const aborted =
			signal === undefined
				? undefined
				: new Promise<never>((_, reject) => {
						const onAbort = () => {
							const reason = signal.reason;
							reject(
								reason instanceof Error ? reason : createAbortError("AbortError", "Kernel startup cancelled"),
							);
						};
						if (signal.aborted) onAbort();
						else {
							signal.addEventListener("abort", onAbort, { once: true });
							removeAbort = () => signal.removeEventListener("abort", onAbort);
						}
					});
		try {
			const races: Array<Promise<void>> = [readyPromise];
			if (timeout) races.push(timeout);
			if (aborted) races.push(aborted);
			await Promise.race(races);
			if (!this.#alive || this.#disposed) {
				throw new Error(`${this.#options.languageName} kernel exited during startup`);
			}
			this.#ready = true;
		} finally {
			if (timer) clearTimeout(timer);
			removeAbort?.();
		}
	}

	isAlive(): boolean {
		return this.#alive && !this.#disposed;
	}

	async execute(code: string, options?: TExecuteOptions): Promise<KernelExecuteResult> {
		// Cancellation is caller-owned state and takes precedence over the kernel
		// liveness check: an already-aborted request must settle as cancellation even
		// when teardown won the race and the old kernel is no longer running.
		if (options?.signal?.aborted) {
			return {
				status: "error",
				cancelled: true,
				timedOut: isTimeoutReason(options.signal.reason),
				stdinRequested: false,
			};
		}
		if (typeof options?.timeoutMs === "number" && options.timeoutMs <= 0) {
			return {
				status: "error",
				cancelled: true,
				timedOut: true,
				stdinRequested: false,
			};
		}
		if (!this.isAlive()) {
			throw new Error(`${this.#options.languageName} kernel is not running`);
		}

		// Keep direct callers consistent with executeWithBudget and the public
		// evaluators: a finite non-positive budget is already exhausted. Return a
		// cancellation result before queueing or writing a request so `0` cannot
		// accidentally mean "wait forever".

		const sourceSignal = options?.signal;
		const hasTimeout = typeof options?.timeoutMs === "number" && options.timeoutMs > 0;
		const controller = sourceSignal || hasTimeout ? new AbortController() : undefined;
		let timeoutId: ReturnType<typeof setTimeout> | undefined;
		let removeSourceAbort: (() => void) | undefined;
		let started = false;
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
					if (!controller.signal.aborted)
						controller.abort(
							createAbortError("TimeoutError", `${this.#options.languageName} execution timed out`),
						);
				}, options?.timeoutMs);
				timeoutId.unref?.();
			}
		}

		let release!: () => void;
		const turn = new Promise<void>(resolve => {
			release = resolve;
		});
		const previous = this.#executionTail;
		this.#executionTail = previous.then(
			() => turn,
			() => turn,
		);
		const requestOptions = {
			...options,
			signal: controller?.signal ?? options?.signal,
			timeoutMs: controller ? undefined : options?.timeoutMs,
			onRequestWritten: () => {
				started = true;
				options?.onRequestWritten?.();
			},
		} as TExecuteOptions;
		const run = previous
			.then(
				() => {
					started = true;
					return this.#executeOne(code, requestOptions);
				},
				() => {
					started = true;
					return this.#executeOne(code, requestOptions);
				},
			)
			.finally(() => {
				if (timeoutId) clearTimeout(timeoutId);
				removeSourceAbort?.();
				release();
			});
		if (!controller) return await run;
		let removeQueuedAbort: (() => void) | undefined;
		const queuedAbort = new Promise<KernelExecuteResult>(resolve => {
			const onAbort = () => {
				if (started) return;
				removeQueuedAbort?.();
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
				removeQueuedAbort = () => controller.signal.removeEventListener("abort", onAbort);
			}
		});
		try {
			return await Promise.race([run, queuedAbort]);
		} finally {
			removeQueuedAbort?.();
		}
	}

	async #executeOne(code: string, options?: TExecuteOptions): Promise<KernelExecuteResult> {
		if (!this.isAlive()) {
			throw new Error(`${this.#options.languageName} kernel is not running`);
		}

		const msgId = options?.id ?? Snowflake.next();
		const { promise, resolve } = Promise.withResolvers<KernelExecuteResult>();
		const pending: PendingExecution = {
			id: msgId,
			resolve,
			options,
			status: "ok",
			cancelled: false,
			timedOut: false,
			stdinRequested: false,
			settled: false,
			kernelKilled: false,
		};
		this.#pending.set(msgId, pending);

		let cleanup = () => {};
		const finalize = () => {
			if (pending.settled) return;
			pending.settled = true;
			this.#pending.delete(msgId);
			cleanup();
			resolve({
				status: pending.status,
				executionCount: pending.executionCount,
				error: pending.error,
				cancelled: pending.cancelled,
				timedOut: pending.timedOut,
				stdinRequested: pending.stdinRequested,
				kernelKilled: pending.kernelKilled,
			});
		};

		let requestWriteStarted = false;
		let requestWriteCommitted = false;
		const requestCancel = () => {
			if (pending.settled || pending.escalationTimer) return;
			if (!requestWriteStarted) {
				finalize();
				return;
			}
			// The write-start hook is deliberately before the pipe write so a
			// language-specific runner can fence cancellation against a request
			// which may already be visible to the child. If abort fires from that
			// hook itself, SIGINT must wait until the bytes are committed: sending
			// it first can be consumed while the runner is still between requests.
			if (!requestWriteCommitted) return;
			void this.interrupt();
			const escalation = setTimeout(() => {
				if (pending.settled) return;
				logger.warn(`${this.#options.languageName} runner did not respond to SIGINT; terminating subprocess`, {
					kernelId: this.id,
				});
				pending.kernelKilled = true;
				void this.shutdown();
			}, this.#options.interruptEscalationMs);
			escalation.unref?.();
			pending.escalationTimer = escalation;
		};

		const onAbort = () => {
			pending.cancelled = true;
			pending.timedOut = pending.timedOut || isTimeoutReason(options?.signal?.reason);
			requestCancel();
		};
		const timeoutId =
			typeof options?.timeoutMs === "number" && options.timeoutMs > 0
				? setTimeout(() => {
						pending.timedOut = true;
						pending.cancelled = true;
						requestCancel();
					}, options.timeoutMs)
				: undefined;

		cleanup = () => {
			clearTimeout(timeoutId);
			clearTimeout(pending.escalationTimer);
			pending.escalationTimer = undefined;
			options?.signal?.removeEventListener("abort", onAbort);
		};

		if (options?.signal) {
			if (options.signal.aborted) {
				onAbort();
			} else {
				options.signal.addEventListener("abort", onAbort, { once: true });
				if (options.signal.aborted) {
					options.signal.removeEventListener("abort", onAbort);
					onAbort();
				}
			}
		}

		pending.finalize = finalize;

		const payload = this.#options.buildPayload(code, msgId, options);

		if (pending.settled) {
			return promise;
		}

		requestWriteStarted = true;
		try {
			// Notify language-specific serializers before awaiting the pipe write.
			// Cancellation can arrive in this await window; a runner may already
			// have observed the request and become poisoned even though the older
			// post-write hook has not run yet.
			options?.onRequestWriteStarted?.();
			await this.#writeLine(payload);
			requestWriteCommitted = true;
			options?.onRequestWritten?.();
			// An abort may have fired synchronously from onRequestWriteStarted (or
			// while the pipe write was yielding). Retry the interrupt now that the
			// child has definitely received the request.
			if (pending.cancelled) requestCancel();
		} catch (err) {
			pending.cancelled = true;
			pending.error = {
				name: "TransportError",
				value: err instanceof Error ? err.message : String(err),
				traceback: [],
			};
			finalize();
		}

		return promise;
	}

	async interrupt(): Promise<void> {
		if (!this.#proc || this.#disposed) return;
		try {
			this.#proc.kill("SIGINT");
		} catch (err) {
			logger.warn(`Failed to interrupt ${this.#options.languageName.toLowerCase()} runner`, {
				error: err instanceof Error ? err.message : String(err),
			});
		}
	}

	async shutdown(options?: KernelShutdownOptions): Promise<KernelShutdownResult> {
		if (this.#shutdownConfirmed) return { confirmed: true };
		if (this.#shutdownPromise) return await this.#shutdownPromise;
		const shutdown = this.#shutdownImpl(options);
		this.#shutdownPromise = shutdown;
		try {
			const result = await shutdown;
			// A timed-out shutdown is not terminal: the process may still be
			// alive, and a later lifecycle pass must be able to retry with a
			// different grace period rather than receiving the stale false result.
			if (!result.confirmed && this.#shutdownPromise === shutdown) this.#shutdownPromise = null;
			return result;
		} catch (error) {
			if (this.#shutdownPromise === shutdown) this.#shutdownPromise = null;
			throw error;
		}
	}

	async #shutdownImpl(options?: KernelShutdownOptions): Promise<KernelShutdownResult> {
		if (this.#shutdownConfirmed) return { confirmed: true };

		this.#alive = false;
		if (!this.#ready) {
			this.#rejectReady?.(new Error(`${this.#options.languageName} kernel shutdown during startup`));
			this.#rejectReady = null;
		}
		this.#abortPendingExecutions(`${this.#options.languageName} kernel shutdown`, {
			kernelKilled: true,
		});

		const timeoutMs = options?.timeoutMs ?? this.#options.shutdownGraceMs;
		const proc = this.#proc;
		if (!proc) {
			this.#shutdownConfirmed = true;
			this.#disposed = true;
			return { confirmed: true };
		}

		try {
			await this.#writeLine(this.#options.exitPayload).catch(() => {});
		} catch {
			/* writer may already be closed */
		}

		try {
			this.#stdin?.end();
		} catch {
			/* ignore */
		}

		const exited = this.#waitForExitWithTimeout(timeoutMs);
		let result = await exited;
		if (result === null) {
			try {
				proc.kill("SIGTERM");
			} catch {
				/* ignore */
			}
			result = await this.#waitForExitWithTimeout(timeoutMs);
		}
		if (result === null) {
			try {
				proc.kill("SIGKILL");
			} catch {
				/* ignore */
			}
			result = await this.#waitForExitWithTimeout(timeoutMs);
		}

		const confirmed = result !== null;
		this.#shutdownConfirmed = confirmed;
		this.#disposed = true;
		return { confirmed };
	}

	#abortPendingExecutions(_reason: string, options?: { kernelKilled?: boolean }): void {
		if (this.#pending.size === 0) return;
		const pending = Array.from(this.#pending.values());
		const kernelKilledDefault = options?.kernelKilled ?? false;
		for (const entry of pending) {
			if (entry.settled) continue;
			entry.kernelKilled ||= kernelKilledDefault;
			entry.cancelled = true;
			// Do not emit through the cell sink here. The subprocess may have
			// exited after the caller already observed cancellation, and a late
			// callback would be attributed to a subsequent cell.
			if (entry.finalize) {
				// Route through finalize so abort listeners, timeout timers, and
				// escalation timers are removed even when the subprocess dies.
				entry.finalize();
			} else {
				entry.settled = true;
				this.#pending.delete(entry.id);
				entry.resolve({
					status: "error",
					cancelled: true,
					timedOut: entry.timedOut,
					stdinRequested: entry.stdinRequested,
					executionCount: entry.executionCount,
					error: entry.error,
					kernelKilled: entry.kernelKilled,
				});
			}
		}
	}

	async #writeLine(line: string): Promise<void> {
		if (!this.#stdin) {
			throw new Error(`${this.#options.languageName} kernel stdin is not open`);
		}
		if (this.#options.traceIpc) {
			logger.debug(`${this.#options.languageName}Kernel send`, { preview: redactIpcPayload(line).slice(0, 120) });
		}
		this.#stdin.write(`${line}\n`);
		this.#stdin.flush();
	}

	#markDead(reason: string, options?: { kernelKilled?: boolean }): void {
		this.#alive = false;
		this.#ready = false;
		if (this.#rejectReady) {
			this.#rejectReady?.(new Error(reason));
			this.#resolveReady = null;
			this.#rejectReady = null;
		}
		this.#abortPendingExecutions(reason, { kernelKilled: options?.kernelKilled ?? true });
	}

	#startReader(stream: ReadableStream<Uint8Array>): void {
		const reader = stream.getReader();
		const decoder = new TextDecoder();
		const loop = async () => {
			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					this.#readBuffer += decoder.decode(value, { stream: true });
					await this.#flushFrames();
				}
				this.#readBuffer += decoder.decode();
				await this.#flushFrames();
			} catch (err) {
				logger.warn(`${this.#options.languageName} kernel reader failed`, {
					error: err instanceof Error ? err.message : String(err),
				});
				// A display/chunk callback failure is a transport failure from the
				// kernel's point of view. Do not leave pending executions hanging or
				// reuse a process whose reader can no longer consume frames.
				this.#markDead(`${this.#options.languageName} kernel reader failed`, { kernelKilled: true });
				try {
					this.#proc?.kill("SIGTERM");
				} catch {
					/* ignore */
				}
			} finally {
				// A clean stdout EOF is still a dead runner. Without this transition,
				// startup waiters and executions with a missing `done` frame hang
				// forever when the subprocess closes stdout before `exited` settles.
				if (this.#alive) {
					this.#markDead(`${this.#options.languageName} kernel reader reached EOF`, { kernelKilled: true });
					try {
						this.#proc?.kill("SIGTERM");
					} catch {
						/* ignore */
					}
				}
				try {
					reader.releaseLock();
				} catch {
					/* ignore */
				}
			}
		};
		void loop();
	}

	#startStderrDrain(stream: ReadableStream<Uint8Array>): void {
		const reader = stream.getReader();
		const decoder = new TextDecoder();
		const loop = async () => {
			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					const text = decoder.decode(value);
					if (text.trim()) {
						logger.warn(`${this.#options.languageName} runner stderr`, { text });
					}
				}
			} catch {
				/* ignore */
			} finally {
				try {
					reader.releaseLock();
				} catch {
					/* ignore */
				}
			}
		};
		void loop();
	}

	async #flushFrames(): Promise<void> {
		while (true) {
			const nl = this.#readBuffer.indexOf("\n");
			if (nl < 0) return;
			const line = this.#readBuffer.slice(0, nl);
			this.#readBuffer = this.#readBuffer.slice(nl + 1);
			if (!line.trim()) continue;
			let frame: Frame;
			try {
				frame = JSON.parse(line) as Frame;
			} catch (err) {
				logger.warn(`${this.#options.languageName} runner emitted invalid JSON`, {
					line: line.slice(0, 200),
					error: err instanceof Error ? err.message : String(err),
				});
				continue;
			}
			if (this.#options.traceIpc) {
				logger.debug(`${this.#options.languageName}Kernel recv`, { type: frame.type, id: frame.id });
			}
			await this.#handleFrame(frame);
		}
	}

	async #handleFrame(frame: Frame): Promise<void> {
		if (frame.type === "ready") {
			// A queued frame can still be delivered after process exit/reader EOF.
			// Never let that stale frame resurrect startup. `exited` may not have
			// settled yet, so also inspect the subprocess's exit code directly.
			const exitCode = this.#proc?.exitCode;
			if (!this.#alive || this.#disposed || (exitCode !== null && exitCode !== undefined)) return;
			this.#ready = true;
			this.#resolveReady?.();
			this.#resolveReady = null;
			this.#rejectReady = null;
			return;
		}
		const rid = frame.id;
		if (!rid) return;
		const pending = this.#pending.get(rid);
		if (!pending) return;

		switch (frame.type) {
			case "started":
				return;
			case "stdout":
			case "stderr": {
				const text = frame.data ?? "";
				if (text && !pending.cancelled && !pending.settled && pending.options?.onChunk) {
					await pending.options.onChunk(text);
				}
				return;
			}
			case "display":
			case "result": {
				const bundle = frame.bundle ?? {};
				const { text, outputs } = await renderKernelDisplay(bundle);
				if (text && !pending.cancelled && !pending.settled && pending.options?.onChunk) {
					await pending.options.onChunk(text);
				}
				if (outputs.length > 0 && !pending.cancelled && !pending.settled && pending.options?.onDisplay) {
					for (const output of outputs) {
						await pending.options.onDisplay(output);
					}
				}
				return;
			}
			case "error": {
				const traceback = Array.isArray(frame.traceback) ? frame.traceback.map(String) : [];
				pending.status = "error";
				pending.error = {
					name: String(frame.ename ?? "Error"),
					value: String(frame.evalue ?? ""),
					traceback,
				};
				const message =
					traceback.length > 0 ? `${traceback.join("\n")}\n` : `${pending.error.name}: ${pending.error.value}\n`;
				if (!pending.cancelled && !pending.settled && pending.options?.onChunk) {
					await pending.options.onChunk(message);
				}
				return;
			}
			case "done": {
				if (typeof frame.executionCount === "number") {
					pending.executionCount = frame.executionCount;
				}
				if (frame.status === "error" && pending.status === "ok") {
					pending.status = "error";
				}
				if (frame.cancelled) {
					pending.cancelled = true;
				}
				pending.finalize?.();
				return;
			}
		}
	}

	async executeWithBudget(
		code: string,
		signal: AbortSignal | undefined,
		timeoutMs: number,
		label: string,
	): Promise<void> {
		const controller = new AbortController();
		const cleanups: Array<() => void> = [];
		if (signal) {
			if (signal.aborted) {
				controller.abort(signal.reason);
			} else {
				const onAbort = () => controller.abort(signal.reason);
				signal.addEventListener("abort", onAbort, { once: true });
				cleanups.push(() => signal.removeEventListener("abort", onAbort));
			}
		}
		const timer =
			timeoutMs > 0
				? setTimeout(() => controller.abort(createAbortError("TimeoutError", `${label} timed out`)), timeoutMs)
				: undefined;
		if (timer) cleanups.push(() => clearTimeout(timer));
		try {
			throwIfAborted(controller.signal, label);
			if (timeoutMs <= 0) throw createAbortError("TimeoutError", `${label} timed out`);
			const result = await this.execute(code, {
				signal: controller.signal,
				silent: true,
				storeHistory: false,
			} as TExecuteOptions);
			if (result.cancelled) {
				throw createAbortError(result.timedOut ? "TimeoutError" : "AbortError", `${label} cancelled`);
			}
			if (result.status === "error") {
				const reason = result.error?.value ?? `${this.#options.languageName} kernel init failed`;
				throw new Error(`${label} failed: ${reason}`);
			}
		} finally {
			for (const cleanup of cleanups) cleanup();
		}
	}

	/** Wait until the child process has exited. Subclasses that know a
	 * cancellation poisons their runner can use this to keep their execution
	 * queue from admitting another request before the child is gone. */
	protected async waitForProcessExit(timeoutMs?: number): Promise<boolean> {
		if (!this.#exitedPromise) return true;
		if (timeoutMs === undefined) {
			await this.#exitedPromise;
			return true;
		}
		return (await this.#waitForExitWithTimeout(timeoutMs)) !== null;
	}

	#waitForExitWithTimeout(timeoutMs: number): Promise<number | null> {
		if (!this.#exitedPromise) return Promise.resolve(0);
		const exitedPromise = this.#exitedPromise;
		const timeout = new Promise<null>(resolve => {
			const timer = setTimeout(() => resolve(null), Math.max(0, timeoutMs));
			timer.unref?.();
		});
		return Promise.race([exitedPromise.then(code => code as number | null), timeout]);
	}
}
