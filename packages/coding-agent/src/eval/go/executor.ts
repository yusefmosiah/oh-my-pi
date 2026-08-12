import * as path from "node:path";
import { getProjectDir } from "@oh-my-pi/pi-utils";
import type { ToolSession } from "../../tools";
import {
	buildManagedKernelEnv,
	buildManagedKernelEnvPatch,
	createCancelledKernelResult,
	executeWithKernelBase,
	getExecutionDeadlineMs,
	isCancellationError,
	isTimedOutCancellation,
	waitForPromiseWithCancellation,
} from "../executor-base";
import {
	createKernelSessionRegistry,
	type KernelSession,
	normalizeKernelSessionCwd,
	requireRemainingKernelTimeoutMs,
} from "../kernel-session-registry";
import { ensureToolBridge as ensureToolBridgeServer, type ToolBridgeInfo } from "../tool-bridge";
import type { EvalDisplayOutput, EvalStatusEvent } from "../types";
import { checkGoKernelAvailability, GoKernel, type GoKernelAvailability } from "./kernel";

const SHUTDOWN_GRACE_MS = 1_000;

export interface GoExecutorOptions {
	cwd?: string;
	sessionId?: string;
	sessionFile?: string;
	artifactsDir?: string;
	localRoots?: Record<string, string>;
	interpreter?: string;
	onChunk?: (text: string) => void | Promise<void>;
	onStatus?: (event: EvalStatusEvent) => void;
	signal?: AbortSignal;
	timeoutMs?: number;
	deadlineMs?: number;
	idleTimeoutMs?: number;
	kernelOwnerId?: string;
	reset?: boolean;
	toolSession?: ToolSession;
	bridge?: ToolBridgeInfo;
	bridgeSessionId?: string;
	artifactId?: string;
	artifactPath?: string;
}

export interface GoResult {
	output: string;
	exitCode: number | undefined;
	cancelled: boolean;
	truncated: boolean;
	artifactId: string | undefined;
	totalLines: number;
	totalBytes: number;
	outputLines: number;
	outputBytes: number;
	displayOutputs: EvalDisplayOutput[];
	stdinRequested: boolean;
}

class GoExecutionCancelledError extends Error {
	readonly timedOut: boolean;

	constructor(timedOut: boolean) {
		super(timedOut ? "Go/Yaegi execution timed out" : "Go/Yaegi execution cancelled");
		this.name = timedOut ? "TimeoutError" : "AbortError";
		this.timedOut = timedOut;
	}
}

function normalizeExplicitInterpreter(cwd: string, interpreter: string | undefined): string {
	return interpreter ? path.resolve(cwd, interpreter) : "";
}

function requireRemainingTimeoutMs(deadlineMs?: number): number | undefined {
	return requireRemainingKernelTimeoutMs(deadlineMs, GoExecutionCancelledError);
}

function formatTimeoutAnnotation(timeoutMs?: number): string | undefined {
	if (timeoutMs === undefined) return undefined;
	return `[cell timed out after ${(timeoutMs / 1000).toFixed(0)}s]`;
}

function formatKernelTimeoutAnnotation(timeoutMs: number | undefined, kernelKilled: boolean): string {
	const explanation = kernelKilled ? "; helper terminated to recover" : "; helper is still running";
	if (timeoutMs === undefined) return `[execution timed out${explanation}]`;
	return `[execution timed out after ${(timeoutMs / 1000).toFixed(0)}s${explanation}]`;
}

function createCancelledGoResult(timedOut: boolean, timeoutMs?: number): GoResult {
	return {
		...createCancelledKernelResult(formatTimeoutAnnotation(timedOut ? timeoutMs : undefined) ?? ""),
		stdinRequested: false,
	};
}

async function startKernel(cwd: string, options: GoExecutorOptions): Promise<GoKernel> {
	requireRemainingTimeoutMs(options.deadlineMs);
	return await GoKernel.start({
		cwd,
		env: buildManagedKernelEnv(options),
		signal: options.signal,
		deadlineMs: options.deadlineMs,
		interpreter: options.interpreter,
	});
}

async function executeWithKernel(
	kernel: GoKernel,
	code: string,
	options: GoExecutorOptions | undefined,
): Promise<GoResult> {
	const result = await executeWithKernelBase<GoExecutorOptions>({
		kernel,
		code,
		options,
		runIdPrefix: "go",
		errorLogLabel: "Go/Yaegi",
		cancelledErrorClass: GoExecutionCancelledError,
		buildKernelEnvPatch: buildManagedKernelEnvPatch,
		formatKernelTimeoutAnnotation,
		formatTimeoutAnnotation,
	});
	// The runner poisons and exits itself when an active Yaegi cell is cancelled.
	// Do not call shutdown here: a queued cancellation must not interrupt the
	// earlier cell that owns the subprocess.
	return result;
}

async function waitForGo<T>(
	promise: Promise<T>,
	options: Pick<GoExecutorOptions, "signal" | "deadlineMs">,
): Promise<T> {
	return await waitForPromiseWithCancellation(promise, options, GoExecutionCancelledError);
}

async function ensureKernelAvailable(cwd: string, options: GoExecutorOptions): Promise<GoKernelAvailability> {
	const availability = await waitForGo(
		checkGoKernelAvailability(cwd, options.interpreter, { signal: options.signal, deadlineMs: options.deadlineMs }),
		options,
	);
	if (!availability.ok) {
		requireRemainingTimeoutMs(options.deadlineMs);
		throw new Error(availability.reason ?? "Yaegi helper unavailable");
	}
	return availability;
}

async function ensureToolBridge(options: GoExecutorOptions): Promise<void> {
	if (!options.toolSession && !options.bridge) {
		throw new Error("Go/Yaegi eval requires an owning ToolSession for the authenticated host bridge");
	}
	if (options.bridge) return;
	try {
		options.bridge = await ensureToolBridgeServer();
	} catch (err) {
		throw new Error(`Failed to start Go/Yaegi tool bridge: ${err instanceof Error ? err.message : String(err)}`);
	}
}

const sessionRegistry = createKernelSessionRegistry<GoKernel, GoExecutorOptions, GoResult, KernelSession<GoKernel>>({
	languageLabel: "Go/Yaegi",
	cancelledErrorClass: GoExecutionCancelledError,
	buildSessionKey: (sessionId, cwd, interpreter) =>
		`${sessionId}\0${normalizeKernelSessionCwd(cwd)}\0${normalizeExplicitInterpreter(cwd, interpreter)}`,
	startKernel,
	createSession: session => session,
	executeWithKernel,
	validateKernel: (session, kernel) => session.kernel === kernel,
	shutdownSession: (session, resetting) =>
		resetting ? session.kernel.shutdown({ timeoutMs: SHUTDOWN_GRACE_MS }) : session.kernel.shutdown(),
	isCancellation: error => isCancellationError(error, GoExecutionCancelledError),
	isTimedOutCancellation: (error, signal) => isTimedOutCancellation(error, GoExecutionCancelledError, signal),
});

export async function disposeAllGoKernelSessions(): Promise<void> {
	await sessionRegistry.disposeAll();
}

export async function disposeGoKernelSessionsByOwner(ownerId: string): Promise<void> {
	await sessionRegistry.disposeByOwner(ownerId);
}

export async function executeGo(code: string, options?: GoExecutorOptions): Promise<GoResult> {
	const cwd = normalizeKernelSessionCwd(options?.cwd ?? getProjectDir());
	const deadlineMs = getExecutionDeadlineMs(options);
	const executionOptions: GoExecutorOptions = { ...(options ?? {}), cwd, deadlineMs };
	try {
		requireRemainingTimeoutMs(deadlineMs);
		if (executionOptions.signal?.aborted) {
			throw new GoExecutionCancelledError(
				isTimedOutCancellation(executionOptions.signal.reason, GoExecutionCancelledError, executionOptions.signal),
			);
		}
		await ensureKernelAvailable(cwd, executionOptions);
		await ensureToolBridge(executionOptions);
		return await sessionRegistry.executeOnSession(code, cwd, executionOptions);
	} catch (err) {
		if (isCancellationError(err, GoExecutionCancelledError) || executionOptions.signal?.aborted) {
			return createCancelledGoResult(
				isTimedOutCancellation(err, GoExecutionCancelledError, executionOptions.signal),
				executionOptions.idleTimeoutMs,
			);
		}
		throw err;
	}
}
