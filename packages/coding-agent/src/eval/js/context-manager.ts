import { logger, postmortem, Snowflake, workerHostEntry } from "@oh-my-pi/pi-utils";
import {
	createWorkerHandle,
	createWorkerSubprocess,
	resolveWorkerSpawnCmd,
	workerEnvFromParent,
} from "../../subprocess/worker-client";
import type { ToolSession } from "../../tools";
import { ToolAbortError, ToolError } from "../../tools/tool-errors";
import { safeSend as safeSendIpc } from "../../utils/ipc";
import { EVAL_TIMEOUT_PAUSE_OP, EVAL_TIMEOUT_RESUME_OP } from "../bridge-timeout";
import { attachSessionOwner, resolveOwnerScopedSessionKey, type SessionOwners } from "../executor-base";
import { shouldDetachKernel } from "../py/spawn-options";
import { callSessionTool, type JsStatusEvent } from "./tool-bridge";
import { WorkerCore } from "./worker-core";
// Coding-agent binary/bundle workers route through the CLI entrypoint with a
// hidden argv mode, so compiled/npm builds only need one JavaScript entry.
import type {
	JsDisplayOutput,
	RunErrorPayload,
	SessionSnapshot,
	Transport,
	WorkerInbound,
	WorkerOutbound,
} from "./worker-protocol";

export { rewriteImports, wrapCode } from "./shared/rewrite-imports";
export type { JsDisplayOutput } from "./worker-protocol";

export interface VmRunState {
	signal?: AbortSignal;
	onText?: (chunk: string) => void;
	onDisplay?: (output: JsDisplayOutput) => void;
}

interface WorkerHandle {
	mode: "process" | "worker" | "inline";
	send(msg: WorkerInbound): void;
	onMessage(handler: (msg: WorkerOutbound) => void): () => void;
	onError(handler: (error: Error) => void): () => void;
	close(): Promise<boolean>;
	terminate(): Promise<void>;
}

interface PendingRun {
	runId: string;
	/** The exact worker retained by this run, even after teardown removes its key. */
	session: JsSession;
	/** Owner of this cell; used to detach runs when an owner-scoped session is disposed. */
	ownerId?: string;
	runState: VmRunState;
	toolSession: ToolSession;
	resolve(value: { value: unknown }): void;
	reject(error: Error): void;
	toolCalls: Map<string, AbortController>;
	/**
	 * Host calls currently inside a `deferExternalAbort` phase — `agent()`
	 * isolation worktree setup and merge/cherry-pick, which ignore their abort
	 * once started. Settling the run while one is live would return the cell on
	 * top of a git operation still rewriting the repo, so the abort path drains
	 * this first. Mirrors the Python bridge's shielded-signal contract.
	 */
	deferDepth: number;
	/** Resolves once {@link deferDepth} falls back to zero. */
	deferDrained?: PromiseWithResolvers<void>;
	/** Set once the turn was cancelled; blocks new bridge calls during the drain. */
	aborted: boolean;
	/** Set when the bounded drain grace expires; teardown must not wait again. */
	deferTimedOut: boolean;
	/**
	 * A worker `result` withheld because the cell still has bridge calls in
	 * flight. `#runOne` reports a finished run without awaiting its pending
	 * tools, so a floated or caught `agent()` would otherwise settle the run —
	 * tearing down the abort listener — while the subagent kept going with
	 * nothing left able to cancel it. Delivered once the last call drains.
	 */
	heldResult?: Extract<WorkerOutbound, { type: "result" }>;
	settled: boolean;
	/** Set once runOnce has unwound; host bridge calls may still be draining. */
	runFinished: boolean;
	/** Prevent repeated owner-disposal aborts while a request is queued. */
	ownerAbortRequested: boolean;
}

interface JsSession {
	sessionKey: string;
	sessionId: string;
	cwd: string;
	worker: WorkerHandle;
	state: "alive" | "draining" | "dead";
	pending: Map<string, PendingRun>;
	ownerIds: Set<string>;
	hasFallbackOwner: boolean;
}

interface StartingJsSession extends SessionOwners {
	sessionId: string;
	promise: Promise<JsSession>;
	cancelled?: boolean;
	settled?: boolean;
	waiters: Map<string, number>;
	completedWaiters: Map<string, number>;
	/** Shared startup cancellation; one caller's abort must not cancel co-owners. */
	controller: AbortController;
}

/**
 * Runs outlive their session-key registration during deferred teardown. Keep
 * owner-indexed references until both the worker turn and every host bridge
 * call have drained so direct owner disposal cannot miss a removed session.
 */
const activeOwnerRuns = new Map<string, Set<PendingRun>>();

function trackOwnerRun(pending: PendingRun): void {
	if (pending.ownerId === undefined) return;
	let runs = activeOwnerRuns.get(pending.ownerId);
	if (!runs) {
		runs = new Set();
		activeOwnerRuns.set(pending.ownerId, runs);
	}
	runs.add(pending);
}

function forgetOwnerRun(pending: PendingRun): void {
	if (pending.ownerId === undefined) return;
	const runs = activeOwnerRuns.get(pending.ownerId);
	if (!runs) return;
	runs.delete(pending);
	if (runs.size === 0) activeOwnerRuns.delete(pending.ownerId);
}

function maybeForgetOwnerRun(pending: PendingRun): void {
	if (!pending.runFinished || pending.toolCalls.size > 0 || pending.deferDepth > 0) return;
	forgetOwnerRun(pending);
}

type DisposalState = { kind: "all" } | { kind: "owner"; ownerId: string } | { kind: "owners"; ownerIds: Set<string> };

const sessions = new Map<string, JsSession>();
const startingSessions = new Map<string, StartingJsSession>();
const resettingSessions = new Map<string, Promise<void>>();
/**
 * A session's worker is removed from `sessions` before its close/terminate
 * promise settles. Keep that promise by key so a replacement cannot start while
 * the old worker is still tearing down (especially when close is graceful but
 * slow or hung).
 */
const tearingDownSessions = new Map<string, Promise<void>>();
/**
 * Disposal is a lifecycle barrier, not merely a best-effort sweep. New
 * acquisitions wait behind the relevant barrier and retry after cleanup rather
 * than racing a worker that was just marked for teardown.
 */
let disposalPromise: Promise<void> | null = null;
let disposalState: DisposalState | null = null;
const disposalRequests: DisposalState[] = [];
// Global disposal invalidates operations that were already waiting for startup,
// reset, or a shared worker. This is separate from owner epochs because global
// cleanup may remove a session shared by several owners.
let allDisposalEpoch = 0;
// Monotonic owner generations fence calls that were already acquiring a worker
// when owner disposal began. Calls created after the increment may wait behind
// teardown and intentionally acquire a fresh/shared context afterward.
const ownerDisposalEpochs = new Map<string, number>();
// Keep a generation alive while an executeInVmContext call can still observe
// it. Completed owner generations are pruned after their disposal request and
// active calls settle, so this process-global map cannot grow with every owner
// that has ever used a JS context.
const activeOwnerDisposalRefs = new Map<string, number>();
// Sole-owner sessions/starts are claimed synchronously when owner disposal is
// requested. Other owners wait on the claim instead of attaching while the
// request is queued behind an earlier disposal.
type OwnerDisposalClaim = PromiseWithResolvers<void>;
const ownerDisposalClaims = new Map<string, OwnerDisposalClaim>();

function claimSoleOwnerKeys(ownerId: string): Map<string, OwnerDisposalClaim> {
	const keys = new Set<string>();
	for (const [sessionKey, session] of sessions) {
		if (session.ownerIds.size === 1 && session.ownerIds.has(ownerId)) keys.add(sessionKey);
	}
	for (const [sessionKey, starting] of startingSessions) {
		if (starting.ownerIds.size === 1 && starting.ownerIds.has(ownerId)) keys.add(sessionKey);
	}
	const claims = new Map<string, OwnerDisposalClaim>();
	for (const sessionKey of keys) {
		if (ownerDisposalClaims.has(sessionKey)) continue;
		const claim = Promise.withResolvers<void>();
		ownerDisposalClaims.set(sessionKey, claim);
		claims.set(sessionKey, claim);
	}
	return claims;
}

function releaseSoleOwnerClaims(claims: Map<string, OwnerDisposalClaim>): void {
	for (const [sessionKey, claim] of claims) {
		if (ownerDisposalClaims.get(sessionKey) !== claim) continue;
		ownerDisposalClaims.delete(sessionKey);
		claim.resolve();
	}
}

function hasPendingOwnerDisposal(ownerId: string): boolean {
	return disposalRequests.some(
		request =>
			(request.kind === "owner" && request.ownerId === ownerId) ||
			(request.kind === "owners" && request.ownerIds.has(ownerId)),
	);
}

function pruneOwnerDisposalEpoch(ownerId: string): void {
	if (activeOwnerDisposalRefs.has(ownerId) || hasPendingOwnerDisposal(ownerId)) return;
	ownerDisposalEpochs.delete(ownerId);
}

function pruneOwnerDisposalEpochs(): void {
	for (const ownerId of ownerDisposalEpochs.keys()) pruneOwnerDisposalEpoch(ownerId);
}

function retainOwnerDisposalRef(ownerId: string | undefined): void {
	if (ownerId === undefined) return;
	activeOwnerDisposalRefs.set(ownerId, (activeOwnerDisposalRefs.get(ownerId) ?? 0) + 1);
}

function releaseOwnerDisposalRef(ownerId: string | undefined): void {
	if (ownerId === undefined) return;
	const count = activeOwnerDisposalRefs.get(ownerId) ?? 0;
	if (count <= 1) {
		activeOwnerDisposalRefs.delete(ownerId);
		pruneOwnerDisposalEpoch(ownerId);
	} else {
		activeOwnerDisposalRefs.set(ownerId, count - 1);
	}
}

// Worker startup (module-graph import + WorkerCore construction) is infrastructure
// cost, not user compute. Floor it independently of Bun's 5s default per-test timeout
// so a slow cold-start under load isn't aborted mid-init — terminating a still-
// initializing eval runtime triggers the same kind of terminate-race that motivates
// avoiding `vm.runInContext` (see shared/indirect-eval.ts), here surfacing as a
// SIGILL/SIGSEGV. Callers that pass a larger per-cell budget still dominate.
const WORKER_INIT_TIMEOUT_MS = 15_000;
const WORKER_CLOSE_TIMEOUT_MS = 1_000;
// A deferred bridge phase protects workspace mutations, but a misbehaving host
// operation must not hold a worker key (or owner disposal) forever. Once this
// grace expires the worker is force-terminated and the key remains fenced until
// that termination settles.
const WORKER_DEFER_DRAIN_TIMEOUT_MS = 5_000;
const JS_EVAL_PROCESS_ARG = "__omp_worker_js_eval_process";
// Active graceful-close grace period before a worker that ack'd `close` but never
// emitted its `close` event is force-terminated. Defaults to the production floor;
// tests override it (and restore it) to exercise the close-timeout -> terminate
// path without a real wall-clock wait.
let workerCloseTimeoutMs: number = WORKER_CLOSE_TIMEOUT_MS;
let workerDeferDrainTimeoutMs: number = WORKER_DEFER_DRAIN_TIMEOUT_MS;
let useWorkerThreadForTests = false;

/**
 * Test-only seam: override the graceful-close grace period (ms). Returns the
 * previous value so callers can restore it. Production always uses
 * {@link WORKER_CLOSE_TIMEOUT_MS}; never call this outside tests.
 */
export function setWorkerCloseTimeoutMsForTests(ms: number): number {
	const previous = workerCloseTimeoutMs;
	workerCloseTimeoutMs = ms;
	return previous;
}

/** Test-only seam: override deferred bridge drain grace (ms). */
export function setWorkerDeferDrainTimeoutMsForTests(ms: number): number {
	const previous = workerDeferDrainTimeoutMs;
	workerDeferDrainTimeoutMs = ms;
	return previous;
}

/** Test-only seam for the legacy Worker lifecycle mocks. */
export function setJsEvalWorkerThreadForTests(enabled: boolean): boolean {
	const previous = useWorkerThreadForTests;
	useWorkerThreadForTests = enabled;
	return previous;
}

function refreshDisposalState(): void {
	if (disposalRequests.some(request => request.kind === "all")) {
		disposalState = { kind: "all" };
		return;
	}
	const ownerIds = new Set<string>();
	for (const request of disposalRequests) {
		if (request.kind === "owner") ownerIds.add(request.ownerId);
	}
	if (ownerIds.size === 0) disposalState = null;
	else if (ownerIds.size === 1) disposalState = { kind: "owner", ownerId: ownerIds.values().next().value as string };
	else disposalState = { kind: "owners", ownerIds };
}

function disposalBlocks(ownerId: string | undefined): boolean {
	if (!disposalState) return false;
	if (disposalState.kind === "all") return true;
	// An unscoped caller may attach to the session being disposed, so it must
	// wait as well. Other explicit owners may continue on shared sessions.
	return (
		ownerId === undefined ||
		(disposalState.kind === "owner" ? disposalState.ownerId === ownerId : disposalState.ownerIds.has(ownerId))
	);
}

async function waitForOwnerClaim(sessionKey: string, signal?: AbortSignal): Promise<void> {
	const claim = ownerDisposalClaims.get(sessionKey);
	if (!claim) return;
	await waitForAbort(claim.promise, signal, () => undefined);
}

function enqueueDisposal(request: DisposalState, operation: () => Promise<void>): Promise<void> {
	disposalRequests.push(request);
	refreshDisposalState();
	const prior = disposalPromise;
	const run = (async () => {
		await prior?.catch(() => undefined);
		await operation();
	})();
	let tracked!: Promise<void>;
	tracked = run.finally(() => {
		const index = disposalRequests.indexOf(request);
		if (index >= 0) disposalRequests.splice(index, 1);
		refreshDisposalState();
		if (disposalPromise === tracked) disposalPromise = null;
		// The request itself protects the generation while queued/running;
		// after removing it, only live executeInVmContext calls may require it.
		pruneOwnerDisposalEpochs();
	});
	disposalPromise = tracked;
	return tracked;
}

async function waitForDisposal(ownerId: string | undefined, signal?: AbortSignal): Promise<void> {
	while (disposalPromise && disposalBlocks(ownerId)) {
		const barrier = disposalPromise;
		await waitForAbort(
			barrier.catch(() => undefined),
			signal,
			() => undefined,
		);
	}
}

function findFallbackOwnerKey(session: SessionOwners): string | undefined {
	if (!session.hasFallbackOwner) return undefined;
	for (const key of session.ownerIds) {
		if (key.startsWith("fallback:")) return key;
	}
	return undefined;
}

/**
 * `attachSessionOwner` intentionally represents all unscoped callers with one
 * fallback owner. Keep that owner's actual key stable: later callers may have
 * different session ids, and deriving the key from their id would detach the
 * first caller's ownership when one waiter aborts.
 */
function attachJsSessionOwner(
	session: { ownerIds: Set<string>; hasFallbackOwner: boolean },
	sessionId: string,
	ownerId: string | undefined,
): void {
	if (ownerId === undefined && findFallbackOwnerKey(session)) {
		session.hasFallbackOwner = true;
		return;
	}
	attachSessionOwner(session, sessionId, ownerId);
}

function ownerWaiterKey(starting: StartingJsSession, sessionId: string, ownerId: string | undefined): string {
	if (ownerId !== undefined) return `owner:${ownerId}`;
	return findFallbackOwnerKey(starting) ?? `fallback:${sessionId}`;
}

function retainStartingWaiter(starting: StartingJsSession, sessionId: string, ownerId: string | undefined): void {
	attachJsSessionOwner(starting, sessionId, ownerId);
	const key = ownerWaiterKey(starting, sessionId, ownerId);
	starting.waiters.set(key, (starting.waiters.get(key) ?? 0) + 1);
}

function detachStartingOwner(starting: StartingJsSession, key: string, ownerId: string | undefined): void {
	if (ownerId !== undefined) {
		starting.ownerIds.delete(ownerId);
		return;
	}
	starting.ownerIds.delete(key);
	starting.hasFallbackOwner = findFallbackOwnerKey(starting) !== undefined;
}

/**
 * Release one startup waiter. A canceled waiter is detached before startup
 * publishes its owner snapshot; successful waiters are retained on the live
 * session even after the shared startup promise settles.
 */
function releaseStartingWaiter(
	starting: StartingJsSession,
	sessionId: string,
	ownerId: string | undefined,
	failed: boolean,
): boolean {
	const key = ownerWaiterKey(starting, sessionId, ownerId);
	const count = starting.waiters.get(key) ?? 0;
	if (count <= 1) starting.waiters.delete(key);
	else starting.waiters.set(key, count - 1);
	if (failed) {
		const completed = starting.completedWaiters.get(key) ?? 0;
		if (count <= 1 && completed === 0) detachStartingOwner(starting, key, ownerId);
	} else {
		starting.completedWaiters.set(key, (starting.completedWaiters.get(key) ?? 0) + 1);
	}
	if (!starting.settled && starting.waiters.size === 0) {
		starting.cancelled = true;
		starting.controller.abort(new ToolAbortError("JS context startup cancelled"));
	}
	return failed && count <= 1 && (starting.completedWaiters.get(key) ?? 0) === 0;
}

function sessionDeferredDrain(session: JsSession): Promise<void> | undefined {
	const drains = [...session.pending.values()]
		.filter(pending => pending.deferDepth > 0 && pending.deferDrained && !pending.deferTimedOut)
		.map(pending => pending.deferDrained!.promise);
	if (drains.length === 0) return undefined;
	return Promise.allSettled(drains).then(() => undefined);
}

/**
 * Wait for a shielded host phase without making worker/session teardown
 * unbounded. The worker is force-killed by the caller after the grace expires;
 * the host operation may still finish later, but no cell can reuse its worker.
 */
async function waitForDeferredDrain(drained: Promise<void> | undefined, label: string): Promise<boolean> {
	if (!drained) return true;
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeout = new Promise<false>(resolve => {
		timer = setTimeout(() => resolve(false), Math.max(0, workerDeferDrainTimeoutMs));
		timer.unref?.();
	});
	try {
		const completed = await Promise.race([
			drained.then(
				() => true,
				() => true,
			),
			timeout,
		]);
		if (!completed) {
			logger.warn("JS eval deferred bridge drain timed out; force-terminating worker", {
				label,
				timeoutMs: workerDeferDrainTimeoutMs,
			});
		}
		return completed;
	} finally {
		if (timer) clearTimeout(timer);
	}
}

function trackSessionTeardown(
	session: JsSession,
	error: Error,
	options: { force: boolean },
	deferredDrain = sessionDeferredDrain(session),
): Promise<void> {
	const existing = tearingDownSessions.get(session.sessionKey);
	if (existing) return existing;
	if (deferredDrain) {
		session.state = "draining";
		if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
	}
	const teardown = (async () => {
		const drainedInTime = await waitForDeferredDrain(deferredDrain, `session:${session.sessionKey}`);
		if (!drainedInTime) {
			// The worker is about to be force-killed. Drop owner-indexed references
			// now rather than retaining a pending run whose host promise ignores
			// abort forever; late host completion is already fenced by dead state.
			for (const pending of session.pending.values()) forgetOwnerRun(pending);
		}
		await killSession(session, error, { force: options.force || deferredDrain !== undefined });
	})();
	let tracked!: Promise<void>;
	tracked = teardown.finally(() => {
		if (tearingDownSessions.get(session.sessionKey) === tracked) tearingDownSessions.delete(session.sessionKey);
	});
	tearingDownSessions.set(session.sessionKey, tracked);
	return tracked;
}

function trackSessionTeardownAfterDrain(
	session: JsSession,
	error: Error,
	drained: Promise<void> | undefined,
): Promise<void> {
	const sessionDrain = sessionDeferredDrain(session);
	const allDrains =
		drained && sessionDrain
			? Promise.allSettled([drained, sessionDrain]).then(() => undefined)
			: (drained ?? sessionDrain);
	return trackSessionTeardown(session, error, { force: true }, allDrains);
}

async function detachPublishedStartupOwner(sessionKey: string, ownerId: string | undefined): Promise<void> {
	const session = sessions.get(sessionKey);
	if (!session) return;
	await detachStaleSessionOwner(sessionKey, session, ownerId, "JS context startup cancelled");
}

/**
 * Remove an owner only from the exact session an invalidated operation acquired.
 * Checking identity is important: global disposal can finish and a different
 * owner may publish a replacement under the same key before the stale waiter
 * resumes.
 */
async function detachStaleSessionOwner(
	sessionKey: string,
	session: JsSession,
	ownerId: string | undefined,
	reason = "JS context disposed",
): Promise<void> {
	if (sessions.get(sessionKey) !== session) return;
	const ownerKey = ownerId ?? findFallbackOwnerKey(session);
	if (!ownerKey || !session.ownerIds.delete(ownerKey)) return;
	if (ownerId === undefined) session.hasFallbackOwner = findFallbackOwnerKey(session) !== undefined;
	if (session.ownerIds.size !== 0) return;
	if (sessions.get(sessionKey) === session) sessions.delete(sessionKey);
	await trackSessionTeardown(session, new ToolAbortError(reason), { force: true });
}

export async function executeInVmContext(options: {
	sessionKey: string;
	sessionId: string;
	/** Logical owner identifier; scopes `reset` on shared contexts and retained-worker cleanup. */
	ownerId?: string;
	cwd: string;
	session: ToolSession;
	localRoots?: Record<string, string>;
	reset?: boolean;
	code: string;
	filename: string;
	timeoutMs?: number;
	runState: VmRunState;
}): Promise<{ value: unknown }> {
	const allDisposalEpochAtStart = allDisposalEpoch;
	const ownerDisposalEpoch = options.ownerId === undefined ? 0 : (ownerDisposalEpochs.get(options.ownerId) ?? 0);
	const disposalInvalidated = (): boolean =>
		allDisposalEpoch > allDisposalEpochAtStart ||
		(options.ownerId !== undefined && (ownerDisposalEpochs.get(options.ownerId) ?? 0) !== ownerDisposalEpoch);
	retainOwnerDisposalRef(options.ownerId);
	try {
		// Do not even resolve an owner-scoped key while its owner/global cleanup is
		// active: disposal may have removed the old key and is about to reap its
		// worker. A canceled caller detaches from this host-owned barrier.
		await waitForAbort(
			waitForDisposal(options.ownerId, options.runState.signal),
			options.runState.signal,
			() => undefined,
		);
		// An already-cancelled caller must not start a reset, worker, or bridge
		// discovery. The reset itself remains host-owned once started below.
		await waitForAbort(Promise.resolve(), options.runState.signal, () => undefined);
		const sessionKey = resolveOwnerScopedSessionKey({
			baseKey: options.sessionKey,
			ownerId: options.ownerId,
			reset: options.reset === true,
			hasSession: key => sessions.has(key) || startingSessions.has(key),
			getOwners: key => sessions.get(key) ?? startingSessions.get(key),
		});
		if (options.reset) {
			// Coalesce concurrent resets: an existing in-flight reset already
			// produces a fresh context, so a follow-up `reset: true` cell should
			// just wait for it rather than failing the user-visible call. Race the
			// caller's wait against the barrier, but never remove the barrier when a
			// waiter cancels; later cells must still wait for teardown to finish.
			const inFlight = resettingSessions.get(sessionKey);
			if (inFlight) {
				await waitForAbort(
					inFlight.catch(() => undefined),
					options.runState.signal,
					() => undefined,
				);
			} else {
				const resetPromise = resetVmContext(sessionKey);
				const trackedReset = resetPromise.then(() => undefined);
				resettingSessions.set(sessionKey, trackedReset);
				void trackedReset.then(
					() => {
						if (resettingSessions.get(sessionKey) === trackedReset) resettingSessions.delete(sessionKey);
					},
					error => {
						if (resettingSessions.get(sessionKey) === trackedReset) resettingSessions.delete(sessionKey);
						void error;
					},
				);
				await waitForAbort(trackedReset, options.runState.signal, () => undefined);
			}
		} else {
			// Internal coordination: wait for any in-flight reset to settle and then
			// run on the freshly-rebuilt context. Cancellation only detaches this
			// caller; it does not cancel the shared reset.
			const inFlight = resettingSessions.get(sessionKey);
			if (inFlight)
				await waitForAbort(
					inFlight.catch(() => undefined),
					options.runState.signal,
					() => undefined,
				);
		}
		await waitForDisposal(options.ownerId, options.runState.signal);
		const session = await acquireSession(
			sessionKey,
			{ cwd: options.cwd, sessionId: options.sessionId, localRoots: options.localRoots },
			options.timeoutMs,
			options.ownerId,
			options.runState.signal,
		);
		if (disposalInvalidated()) {
			await detachStaleSessionOwner(sessionKey, session, options.ownerId);
			throw new ToolAbortError(
				allDisposalEpoch > allDisposalEpochAtStart ? "JS context disposed" : "JS context owner disposed",
			);
		}
		return await runOnce(session, { ...options, ownerId: options.ownerId });
	} finally {
		releaseOwnerDisposalRef(options.ownerId);
	}
}

export async function resetVmContext(sessionKey: string): Promise<void> {
	const tearingDown = tearingDownSessions.get(sessionKey);
	if (tearingDown) {
		await tearingDown.catch(() => undefined);
		return;
	}
	const starting = startingSessions.get(sessionKey);
	const session = sessions.get(sessionKey) ?? (await starting?.promise.catch(() => undefined));
	if (!session) return;
	if (sessions.get(sessionKey) === session) sessions.delete(sessionKey);
	await trackSessionTeardown(session, new ToolError("JS context reset"), { force: false });
}

export async function disposeAllVmContexts(): Promise<void> {
	// Publish the barrier synchronously, before the first await in enqueueDisposal,
	// so callers already in startup/reset cannot later publish a stale worker.
	allDisposalEpoch += 1;
	return await enqueueDisposal({ kind: "all" }, async () => {
		// Cancel starts before waiting for resets: resetVmContext may itself be
		// waiting for one of these startup promises, and a never-ready worker must
		// not hold global disposal until the normal 15s init timeout.
		const pendingStarting = [...startingSessions.values()];
		for (const starting of pendingStarting) {
			starting.cancelled = true;
			starting.controller.abort(new ToolAbortError("JS context startup cancelled"));
		}
		const started = Promise.allSettled(pendingStarting.map(starting => starting.promise));
		await Promise.allSettled([...resettingSessions.values()]);
		await Promise.allSettled([...tearingDownSessions.values()]);
		const startedResults = await started;
		const all = [...sessions.values()];
		for (const result of startedResults) {
			if (result.status !== "fulfilled") continue;
			if (!all.includes(result.value)) all.push(result.value);
		}
		for (const [sessionKey, session] of sessions) {
			if (sessions.get(sessionKey) === session) sessions.delete(sessionKey);
		}
		await Promise.allSettled(
			all.map(session => trackSessionTeardown(session, new ToolError("JS context disposed"), { force: false })),
		);
	});
}

function abortOwnerRuns(ownerId: string, error: Error): Promise<void>[] {
	const drains: Promise<void>[] = [];
	// Do not derive this from `sessions`: owner disposal can run after an abort
	// moved the worker into `tearingDownSessions`, and the pending host phase is
	// still live during that interval.
	const pendingRuns = [...(activeOwnerRuns.get(ownerId) ?? [])];
	for (const pending of pendingRuns) {
		if (pending.ownerAbortRequested || pending.settled) continue;
		pending.ownerAbortRequested = true;
		pending.aborted = true;
		for (const ctrl of pending.toolCalls.values()) ctrl.abort(error);
		// Owner disposal must not kill a shared worker retained by a co-owner.
		// Tell the worker to stop this run and suppress all late emissions, then
		// reject this host waiter. The worker remains alive for co-owners.
		safeSend(pending.session, { type: "cancel-run", runId: pending.runId, error: toErrorPayload(error) });
		const finish = (): void => {
			if (pending.settled) return;
			pending.heldResult = undefined;
			pending.settled = true;
			pending.reject(error);
		};
		// Reject the owner promptly. A deferred host phase is drained separately
		// with a bounded grace before sole-owner teardown proceeds, so this does
		// not leave the caller waiting forever on an abort-insensitive operation.
		finish();
		const drained = pending.deferDepth > 0 ? pending.deferDrained?.promise : undefined;
		if (drained) {
			drains.push(
				waitForDeferredDrain(drained, `owner:${ownerId}:${pending.runId}`).then(drainedInTime => {
					if (!drainedInTime) pending.deferTimedOut = true;
					// A timeout is the explicit bound: do not retain an owner-indexed
					// reference forever if the host promise ignores its abort.
					forgetOwnerRun(pending);
				}),
			);
		} else {
			maybeForgetOwnerRun(pending);
		}
	}
	return drains;
}

/**
 * Shut down retained JS contexts owned solely by `ownerId` (e.g. a subagent's
 * private fork); shared contexts just drop the owner registration.
 */
export async function disposeVmContextsByOwner(ownerId: string): Promise<void> {
	ownerDisposalEpochs.set(ownerId, (ownerDisposalEpochs.get(ownerId) ?? 0) + 1);
	const claims = claimSoleOwnerKeys(ownerId);
	// Fence and reject active owner runs synchronously, before this request can
	// queue behind an unrelated disposal. The worker teardown itself remains
	// serialized below, and deferred phases still get their bounded grace.
	const ownerAbortDrains = abortOwnerRuns(ownerId, new ToolAbortError("JS context owner disposed"));
	return await enqueueDisposal({ kind: "owner", ownerId }, async () => {
		try {
			await Promise.allSettled(ownerAbortDrains);
			const ownerKeys = (session: SessionOwners): string[] => (session.ownerIds.has(ownerId) ? [ownerId] : []);
			const removeOwnerKeys = (session: SessionOwners, keys: string[]): void => {
				for (const key of keys) session.ownerIds.delete(key);
				if (!findFallbackOwnerKey(session)) session.hasFallbackOwner = false;
			};
			// Cancel starts owned solely by this owner before waiting for resets: a
			// reset may be waiting for the same startup promise, and disposal must not
			// inherit the normal init timeout in that cycle.
			const pendingStarting = [...startingSessions.values()];
			for (const starting of pendingStarting) {
				const keys = ownerKeys(starting);
				if (keys.length > 0 && starting.ownerIds.size === keys.length) {
					starting.cancelled = true;
					starting.controller.abort(new ToolAbortError("JS context startup cancelled"));
				}
			}
			await Promise.allSettled(
				pendingStarting.filter(starting => starting.cancelled).map(starting => starting.promise),
			);
			await Promise.allSettled([...resettingSessions.values()]);
			await Promise.allSettled([...tearingDownSessions.values()]);
			const toKill: JsSession[] = [];
			for (const session of [...sessions.values()]) {
				const keys = ownerKeys(session);
				if (keys.length === 0) continue;
				if (session.ownerIds.size === keys.length) {
					toKill.push(session);
					continue;
				}
				removeOwnerKeys(session, keys);
			}
			const startingToKill: StartingJsSession[] = [];
			for (const [sessionKey, starting] of [...startingSessions.entries()]) {
				if (sessions.has(sessionKey)) continue;
				const keys = ownerKeys(starting);
				if (keys.length === 0) continue;
				if (starting.ownerIds.size === keys.length) {
					starting.cancelled = true;
					starting.controller.abort(new ToolAbortError("JS context startup cancelled"));
					startingToKill.push(starting);
					continue;
				}
				removeOwnerKeys(starting, keys);
			}
			// Re-check shared ownership immediately before deleting/tearing down. An
			// explicit co-owner may have attached while an earlier startup drained.
			const finalToKill: JsSession[] = [];
			for (const session of toKill) {
				const keys = ownerKeys(session);
				if (session.ownerIds.size !== keys.length) {
					removeOwnerKeys(session, keys);
					continue;
				}
				if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
				finalToKill.push(session);
			}
			const started = await Promise.allSettled(startingToKill.map(starting => starting.promise));
			for (const result of started) {
				if (result.status !== "fulfilled") continue;
				const session = result.value;
				if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
				finalToKill.push(session);
			}
			await Promise.allSettled(
				finalToKill.map(session =>
					trackSessionTeardown(session, new ToolError("JS context disposed"), { force: false }),
				),
			);
		} finally {
			releaseSoleOwnerClaims(claims);
		}
	});
}

/**
 * Smoke probe: spawn the JS evaluator through the worker-host entry and prove
 * it answers the `init` handshake in a real isolated subprocess (not the inline
 * fallback). Catches silent process-load and init-message regressions
 * that otherwise strand every cell on the init timeout in a distribution build —
 * the failure mode that motivated `installWorkerInbox`. Wired into
 * `omp --smoke-test` so binary / source / tarball installs all exercise it.
 */
export async function smokeTestJsEvalWorker(): Promise<void> {
	const worker = spawnJsWorker();
	const session: JsSession = {
		sessionKey: "smoke",
		sessionId: "smoke",
		cwd: process.cwd(),
		worker,
		state: "alive",
		pending: new Map(),
		ownerIds: new Set(),
		hasFallbackOwner: false,
	};
	try {
		await initWorker(session, { cwd: process.cwd(), sessionId: "smoke" }, WORKER_INIT_TIMEOUT_MS);
		if (worker.mode !== "process") {
			throw new Error("JS eval worker smoke fell back from the isolated subprocess");
		}
	} finally {
		await worker.terminate().catch(() => undefined);
	}
}

async function runOnce(
	session: JsSession,
	options: {
		sessionId: string;
		cwd: string;
		session: ToolSession;
		localRoots?: Record<string, string>;
		code: string;
		filename: string;
		runState: VmRunState;
		ownerId?: string;
	},
): Promise<{ value: unknown }> {
	const runId = `r-${Snowflake.next()}`;
	const { promise, resolve, reject } = Promise.withResolvers<{ value: unknown }>();
	const pending: PendingRun = {
		runId,
		session,
		ownerId: options.ownerId,
		runState: options.runState,
		toolSession: options.session,
		resolve,
		reject,
		toolCalls: new Map(),
		deferDepth: 0,
		aborted: false,
		deferTimedOut: false,
		settled: false,
		runFinished: false,
		ownerAbortRequested: false,
	};
	session.pending.set(runId, pending);
	trackOwnerRun(pending);

	const onAbort = (): void => {
		const reason = options.runState.signal?.reason;
		const abortError = reasonToError(reason, "Execution aborted");
		// Stop delegated work at once — this is what kills spawned subagents —
		// and refuse further bridge calls so the drain below stays bounded to
		// phases that had already started.
		pending.aborted = true;
		for (const ctrl of pending.toolCalls.values()) ctrl.abort(abortError);
		// A critical host phase ignores its abort once started (isolation
		// worktree setup, merge/cherry-pick). Killing the worker now would
		// settle the cell on top of a git operation still in progress, so wait
		// for it. Hard-kill is still the only way to interrupt synchronous user
		// code, hence it stays the terminal step either way.
		const drained = pending.deferDepth > 0 ? pending.deferDrained?.promise : undefined;
		// Mark the key as draining immediately. A new cell must not reuse this
		// worker while the shielded host operation (typically isolation/merge) is
		// still mutating the workspace. The per-key teardown promise makes that
		// drain visible to acquireSession.
		void trackSessionTeardownAfterDrain(session, abortError, drained);
	};

	if (options.runState.signal?.aborted) {
		queueMicrotask(onAbort);
	} else {
		options.runState.signal?.addEventListener("abort", onAbort, { once: true });
	}

	try {
		try {
			session.worker.send({
				type: "run",
				runId,
				code: options.code,
				filename: options.filename,
				snapshot: { cwd: options.cwd, sessionId: options.sessionId, localRoots: options.localRoots },
			});
		} catch (error) {
			void killSessionFor(session, error instanceof Error ? error : new Error(String(error)), { force: true });
			throw error;
		}
		return await promise;
	} finally {
		options.runState.signal?.removeEventListener("abort", onAbort);
		session.pending.delete(runId);
		pending.runFinished = true;
		maybeForgetOwnerRun(pending);
	}
}

async function waitForAbort<T>(
	promise: Promise<T>,
	signal: AbortSignal | undefined,
	handleAbort: () => void,
): Promise<T> {
	if (!signal) return await promise;
	if (signal.aborted) {
		handleAbort();
		throw reasonToError(signal.reason, "Execution aborted");
	}
	const { promise: aborted, reject } = Promise.withResolvers<never>();
	const onAbort = () => {
		handleAbort();
		reject(reasonToError(signal.reason, "Execution aborted"));
	};
	signal.addEventListener("abort", onAbort, { once: true });
	try {
		return await Promise.race([promise, aborted]);
	} finally {
		signal.removeEventListener("abort", onAbort);
	}
}

async function acquireSession(
	sessionKey: string,
	snapshot: SessionSnapshot,
	timeoutMs?: number,
	ownerId?: string,
	signal?: AbortSignal,
): Promise<JsSession> {
	while (true) {
		await waitForDisposal(ownerId, signal);
		await waitForOwnerClaim(sessionKey, signal);
		// A claim may have been published while the disposal barrier await was
		// settling. Re-check synchronously before attaching; no await follows
		// this check until ownership has been attached.
		if (ownerDisposalClaims.has(sessionKey)) continue;
		const tearingDown = tearingDownSessions.get(sessionKey);
		if (tearingDown) {
			// The old worker has already been removed from `sessions`, but its
			// close/terminate is still authoritative for this key.
			await waitForAbort(
				tearingDown.catch(() => undefined),
				signal,
				() => undefined,
			);
			continue;
		}
		const existing = sessions.get(sessionKey);
		if (existing && existing.state === "alive") {
			existing.sessionId = snapshot.sessionId;
			existing.cwd = snapshot.cwd;
			attachJsSessionOwner(existing, snapshot.sessionId, ownerId);
			return existing;
		}
		if (existing && sessions.get(sessionKey) === existing) sessions.delete(sessionKey);
		const starting = startingSessions.get(sessionKey);
		if (starting) {
			if (starting.cancelled) {
				// A disposal or cancellation canceled this startup. Do not join its
				// doomed promise: wait for worker cleanup, then retry the key.
				await waitForAbort(
					starting.promise.catch(() => undefined),
					signal,
					() => undefined,
				);
				if (startingSessions.get(sessionKey) === starting) startingSessions.delete(sessionKey);
				continue;
			}
			retainStartingWaiter(starting, snapshot.sessionId, ownerId);
			try {
				const result = await waitForAbort(starting.promise, signal, () => undefined);
				releaseStartingWaiter(starting, snapshot.sessionId, ownerId, false);
				return result;
			} catch (error) {
				const detached = releaseStartingWaiter(starting, snapshot.sessionId, ownerId, true);
				if (detached) await detachPublishedStartupOwner(sessionKey, ownerId);
				throw error;
			}
		}
		break;
	}

	let startingSession!: StartingJsSession;
	const startupController = new AbortController();
	const startup = (async (): Promise<JsSession> => {
		// Attach the message listener before sending init. Both Bun Worker messages
		// and subprocess IPC can arrive immediately after the evaluator loads.
		const worker = spawnJsWorker();
		const session: JsSession = {
			sessionKey,
			sessionId: snapshot.sessionId,
			cwd: snapshot.cwd,
			worker,
			state: "alive",
			pending: new Map(),
			ownerIds: new Set(),
			hasFallbackOwner: false,
		};
		// Init headroom is the fixed infrastructure floor; the caller's per-cell timeout
		// dominates when larger so users can grant more by raising `timeout` on a cell.
		const readyTimeoutMs = Math.max(WORKER_INIT_TIMEOUT_MS, timeoutMs ?? 0);
		while (true) {
			try {
				await initWorker(session, snapshot, readyTimeoutMs, startupController.signal);
				break;
			} catch (error) {
				// Runtime crash/load failures surface asynchronously via the runtime's
				// error callback, after the synchronous spawn try/catch has returned.
				// Preserve the full process -> Worker -> inline ladder for those failures.
				const failed = session.worker;
				await failed.terminate().catch(() => undefined);
				if (startupController.signal.aborted || startingSession.cancelled) throw error;
				if (failed.mode === "inline") throw error;
				if (failed.mode === "process") {
					logger.warn("JS eval subprocess init failed; retrying with a Bun Worker", {
						error: error instanceof Error ? error.message : String(error),
					});
					session.worker = spawnBunWorker();
				} else {
					logger.warn("JS eval worker init failed; retrying with inline worker (no sync-loop guard)", {
						error: error instanceof Error ? error.message : String(error),
					});
					session.worker = spawnInlineWorker();
				}
				session.state = "alive";
			}
		}
		if (startupController.signal.aborted || startingSession.cancelled) {
			const error = reasonToError(startupController.signal.reason, "JS context startup cancelled");
			await killSession(session, error, { force: true });
			throw error;
		}
		session.ownerIds = new Set(startingSession.ownerIds);
		session.hasFallbackOwner = startingSession.hasFallbackOwner;
		// Publish only while this startup still owns the key. Disposal leaves the
		// starting record visible until this promise settles, so a replacement
		// cannot overlap the old worker's final teardown.
		if (
			startingSessions.get(sessionKey) === startingSession &&
			session.state === "alive" &&
			!tearingDownSessions.has(sessionKey)
		) {
			sessions.set(sessionKey, session);
		} else {
			const error = new ToolAbortError("JS context startup superseded");
			await killSession(session, error, { force: true });
			throw error;
		}
		return session;
	})();
	startingSession = {
		sessionId: snapshot.sessionId,
		ownerIds: new Set(),
		hasFallbackOwner: false,
		promise: startup,
		waiters: new Map(),
		completedWaiters: new Map(),
		controller: startupController,
	};
	void startup.then(
		() => {
			startingSession.settled = true;
			if (startingSessions.get(sessionKey) === startingSession) startingSessions.delete(sessionKey);
		},
		() => {
			startingSession.settled = true;
			if (startingSessions.get(sessionKey) === startingSession) startingSessions.delete(sessionKey);
		},
	);
	retainStartingWaiter(startingSession, snapshot.sessionId, ownerId);
	startingSessions.set(sessionKey, startingSession);
	try {
		const result = await waitForAbort(startup, signal, () => undefined);
		releaseStartingWaiter(startingSession, snapshot.sessionId, ownerId, false);
		return result;
	} catch (error) {
		const detached = releaseStartingWaiter(startingSession, snapshot.sessionId, ownerId, true);
		if (detached) await detachPublishedStartupOwner(sessionKey, ownerId);
		throw error;
	}
}

async function initWorker(
	session: JsSession,
	snapshot: SessionSnapshot,
	timeoutMs: number,
	signal?: AbortSignal,
): Promise<void> {
	const worker = session.worker;
	const { promise: readyPromise, resolve: resolveReady, reject: rejectReady } = Promise.withResolvers<void>();
	let resolved = false;
	const unsubscribeMessage = worker.onMessage(msg => {
		if (!resolved && msg.type === "ready") {
			resolved = true;
			resolveReady();
			return;
		}
		if (!resolved && msg.type === "init-failed") {
			resolved = true;
			rejectReady(errorFromPayload(msg.error));
			return;
		}
		handleSessionMessage(session, msg);
	});
	const unsubscribeError = worker.onError(error => {
		if (!resolved) {
			resolved = true;
			rejectReady(error);
			return;
		}
		// Worker died after a successful handshake: tear the session down so the
		// in-flight run (and the next acquire) fail fast instead of hanging on a
		// worker that will never reply.
		void killSessionFor(session, error, { force: true });
	});
	try {
		// Attach listeners and send init before awaiting ready. The worker now
		// emits ready only in response to init, so this ordering is race-free.
		worker.send({ type: "init", snapshot });
		await raceWithTimeout(readyPromise, timeoutMs, "Timed out initializing JS eval worker", signal);
	} catch (error) {
		// Handshake failed (timeout, init-failed, abort, or worker error): drop both
		// listeners so the abandoned worker can't keep routing messages into a
		// session the caller is about to discard or retry on the inline fallback.
		unsubscribeMessage();
		unsubscribeError();
		throw error;
	}
}

function handleSessionMessage(session: JsSession, msg: WorkerOutbound): void {
	switch (msg.type) {
		case "text": {
			const pending = session.pending.get(msg.runId);
			if (!pending || pending.aborted) return;
			pending.runState.onText?.(msg.chunk);
			return;
		}
		case "display": {
			const pending = session.pending.get(msg.runId);
			if (!pending || pending.aborted) return;
			pending.runState.onDisplay?.(msg.output);
			return;
		}
		case "tool-call":
			void handleToolCall(session, msg);
			return;
		case "result":
			settlePending(session, msg);
			return;
		case "log":
			logWorkerMessage(msg);
			return;
		case "ready":
		case "init-failed":
		case "closed":
			return;
	}
}

/**
 * Maintain {@link PendingRun.deferDepth} from the bridge's pause/resume status
 * events so an abort can wait out a critical `agent()` phase instead of
 * settling the cell over a half-applied merge.
 */
function trackDeferPhase(pending: PendingRun, event: JsStatusEvent): void {
	if (event.deferExternalAbort !== true) return;
	if (event.op === EVAL_TIMEOUT_PAUSE_OP) {
		pending.deferDepth++;
		pending.deferDrained ??= Promise.withResolvers<void>();
		return;
	}
	if (event.op !== EVAL_TIMEOUT_RESUME_OP || pending.deferDepth === 0) return;
	pending.deferDepth--;
	if (pending.deferDepth > 0) return;
	pending.deferDrained?.resolve();
	pending.deferDrained = undefined;
	maybeForgetOwnerRun(pending);
}

async function handleToolCall(session: JsSession, msg: Extract<WorkerOutbound, { type: "tool-call" }>): Promise<void> {
	const pending = session.pending.get(msg.runId);
	if (!pending) {
		safeSend(session, {
			type: "tool-reply",
			id: msg.id,
			reply: { ok: false, error: { message: "Run no longer active" } },
		});
		return;
	}
	if (pending.aborted) {
		safeSend(session, {
			type: "tool-reply",
			id: msg.id,
			reply: { ok: false, error: { message: "Run was interrupted" } },
		});
		return;
	}
	const ctrl = new AbortController();
	pending.toolCalls.set(msg.id, ctrl);
	try {
		const value = await callSessionTool(msg.name, msg.args, {
			session: pending.toolSession,
			signal: ctrl.signal,
			emitStatus: (event: JsStatusEvent) => {
				// A deferred bridge must still deliver its resume marker after abort;
				// teardown waits on that marker before killing/replacing the worker.
				if (pending.aborted && event.op !== EVAL_TIMEOUT_RESUME_OP) return;
				trackDeferPhase(pending, event);
				if (pending.aborted) return;
				pending.runState.onDisplay?.({ type: "status", event });
			},
		});
		safeSend(session, { type: "tool-reply", id: msg.id, reply: { ok: true, value } });
	} catch (error) {
		safeSend(session, { type: "tool-reply", id: msg.id, reply: { ok: false, error: toErrorPayload(error) } });
	} finally {
		pending.toolCalls.delete(msg.id);
		// Last call of a run whose worker result was withheld: settle it now.
		const held = pending.heldResult;
		if (held && !pending.settled && !pending.aborted && pending.toolCalls.size === 0) {
			finishPending(pending, held);
		}
		maybeForgetOwnerRun(pending);
	}
}

/** Deliver a worker `result` to the waiting {@link runOnce}. */
function finishPending(pending: PendingRun, msg: Extract<WorkerOutbound, { type: "result" }>): void {
	pending.settled = true;
	pending.heldResult = undefined;
	if (msg.ok) {
		pending.resolve({ value: undefined });
		return;
	}
	pending.reject(errorFromPayload(msg.error));
}

function settlePending(session: JsSession, msg: Extract<WorkerOutbound, { type: "result" }>): void {
	const pending = session.pending.get(msg.runId);
	if (!pending || pending.settled) return;
	// Once the turn is cancelled the scheduled kill is the sole settler, so a
	// late worker result can't cut the abort drain short.
	if (pending.aborted) return;
	// A cell owns every bridge call it starts. The worker finishes a run without
	// awaiting its outstanding tool calls, so `agent(...)` that is floated or
	// caught would settle the run here — `runOnce` then drops the abort listener
	// and the pending entry, leaving the subagent running with nothing able to
	// cancel it. Hold the result until the last call drains.
	if (pending.toolCalls.size > 0) {
		pending.heldResult = msg;
		return;
	}
	finishPending(pending, msg);
}

async function killSessionFor(session: JsSession, error: Error, options: { force: boolean }): Promise<void> {
	if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
	await trackSessionTeardown(session, error, options);
}

async function killSession(session: JsSession, error: Error, options: { force: boolean }): Promise<void> {
	if (session.state === "dead") return;
	session.state = "dead";
	for (const pending of session.pending.values()) {
		if (!pending.settled) {
			pending.settled = true;
			for (const ctrl of pending.toolCalls.values()) ctrl.abort(error);
			pending.reject(error);
		}
		// The worker is fenced/dead now. Do not retain an owner reference for a
		// host tool that ignores abort; its late completion cannot re-enter this
		// session because safeSend checks the dead state.
		forgetOwnerRun(pending);
	}
	session.pending.clear();
	if (options.force) {
		await session.worker.terminate().catch(() => undefined);
		return;
	}
	if (await session.worker.close().catch(() => false)) return;
	await session.worker.terminate().catch(() => undefined);
}

function safeSend(session: JsSession, msg: WorkerInbound): void {
	if (session.state !== "alive") return;
	try {
		session.worker.send(msg);
	} catch (err) {
		logger.debug("js worker send failed", { error: err instanceof Error ? err.message : String(err) });
	}
}

function reasonToError(reason: unknown, fallback: string): Error {
	if (reason instanceof Error) return reason;
	if (typeof reason === "string") return new ToolAbortError(reason);
	return new ToolAbortError(fallback);
}

function errorFromPayload(payload: RunErrorPayload): Error {
	if (payload.isAbort) {
		const err = new ToolAbortError(payload.message || "Execution aborted");
		if (payload.stack) err.stack = payload.stack;
		return err;
	}
	const ctor = payload.isToolError ? ToolError : Error;
	const error = new ctor(payload.message);
	if (payload.name) error.name = payload.name;
	if (payload.stack) error.stack = payload.stack;
	return error;
}

function toErrorPayload(error: unknown): RunErrorPayload {
	if (error instanceof Error) {
		return {
			name: error.name,
			message: error.message,
			stack: error.stack,
			isAbort: error.name === "AbortError" || error.name === "ToolAbortError",
			isToolError: error instanceof ToolError || error.name === "ToolError",
		};
	}
	return { message: String(error) };
}

function logWorkerMessage(msg: Extract<WorkerOutbound, { type: "log" }>): void {
	if (msg.level === "debug") logger.debug(msg.msg, msg.meta);
	else if (msg.level === "warn") logger.warn(msg.msg, msg.meta);
	else logger.error(msg.msg, msg.meta);
}

async function raceWithTimeout<T>(
	promise: Promise<T>,
	timeoutMs: number,
	reason: string,
	signal?: AbortSignal,
): Promise<T> {
	if (signal?.aborted) throw reasonToError(signal.reason, "Execution aborted");
	const timeoutSignal = AbortSignal.timeout(timeoutMs);
	const { promise: timeoutPromise, reject } = Promise.withResolvers<never>();
	const onTimeout = (): void => reject(new ToolError(reason));
	const onAbort = (): void => reject(reasonToError(signal?.reason, "Execution aborted"));
	timeoutSignal.addEventListener("abort", onTimeout, { once: true });
	signal?.addEventListener("abort", onAbort, { once: true });
	try {
		return await Promise.race([promise, timeoutPromise]);
	} finally {
		timeoutSignal.removeEventListener("abort", onTimeout);
		signal?.removeEventListener("abort", onAbort);
	}
}

function spawnJsWorker(): WorkerHandle {
	if (!useWorkerThreadForTests) {
		try {
			return spawnJsProcess();
		} catch (err) {
			// Fall through to the Bun Worker rung: a worker thread still interrupts
			// synchronous infinite loops via terminate(), which the inline fallback
			// cannot.
			logger.warn("JS eval subprocess spawn failed; falling back to a Bun Worker", {
				error: err instanceof Error ? err.message : String(err),
			});
		}
	}
	return spawnBunWorker();
}

function spawnBunWorker(): WorkerHandle {
	try {
		const hostEntry = workerHostEntry();
		const worker = hostEntry
			? new Worker(hostEntry, { type: "module", argv: ["__omp_worker_js_eval"] })
			: new Worker(new URL("./worker-entry.ts", import.meta.url).href, { type: "module" });
		return wrapBunWorker(worker);
	} catch (err) {
		logger.warn("Bun Worker spawn failed; using inline JS eval worker (no sync-loop guard)", {
			error: err instanceof Error ? err.message : String(err),
		});
		return spawnInlineWorker();
	}
}

function spawnJsProcess(): WorkerHandle {
	const spawned = createWorkerSubprocess<WorkerOutbound>({
		spawnCommand: resolveWorkerSpawnCmd(JS_EVAL_PROCESS_ARG),
		env: workerEnvFromParent(),
		exitLabel: "JS eval worker",
		detached: shouldDetachKernel(process.platform),
		reportCleanExit: true,
		unref: false,
	});
	const base = createWorkerHandle<WorkerInbound, WorkerOutbound>(spawned, message =>
		safeSendIpc(spawned.proc, message, "js-eval"),
	);
	return {
		mode: "process",
		send: message => base.send(message),
		onMessage: handler => base.onMessage(handler),
		onError: handler => base.onError(handler),
		async close() {
			const { promise, resolve } = Promise.withResolvers<boolean>();
			let settled = false;
			let timeout: NodeJS.Timeout | undefined;
			let unsubscribe = (): void => {};
			const finish = (value: boolean): void => {
				if (settled) return;
				settled = true;
				if (timeout) clearTimeout(timeout);
				unsubscribe();
				resolve(value);
			};
			unsubscribe = base.onMessage(message => {
				if (message.type !== "closed") return;
				void base.terminate().finally(() => finish(true));
			});
			timeout = setTimeout(() => finish(false), workerCloseTimeoutMs);
			base.send({ type: "close" });
			return await promise;
		},
		terminate: () => base.terminate(),
	};
}

function wrapBunWorker(worker: Worker): WorkerHandle {
	return {
		mode: "worker",
		send(msg) {
			worker.postMessage(msg);
		},
		onMessage(handler) {
			const wrap = (event: MessageEvent): void => handler(event.data as WorkerOutbound);
			worker.addEventListener("message", wrap);
			return () => worker.removeEventListener("message", wrap);
		},
		onError(handler) {
			const onError = (event: ErrorEvent): void => handler(errorFromWorkerEvent(event));
			const onMessageError = (event: MessageEvent): void =>
				handler(new ToolError(`JS eval worker message error: ${String(event.data)}`));
			const onClose = (): void => handler(new Error("JS eval worker exited"));
			worker.addEventListener("error", onError);
			worker.addEventListener("messageerror", onMessageError);
			worker.addEventListener("close", onClose);
			return () => {
				worker.removeEventListener("error", onError);
				worker.removeEventListener("messageerror", onMessageError);
				worker.removeEventListener("close", onClose);
			};
		},
		async close() {
			const { promise: closed, resolve } = Promise.withResolvers<boolean>();
			let settled = false;
			let sawClosedAck = false;
			let sawWorkerExit = false;
			let timeout: NodeJS.Timeout | undefined;
			let unsubscribe = (): void => {};
			const finish = (value: boolean): void => {
				if (settled) return;
				settled = true;
				if (timeout) clearTimeout(timeout);
				unsubscribe();
				worker.removeEventListener("close", onClose);
				resolve(value);
			};
			const finishIfClosed = (): void => {
				if (sawClosedAck && sawWorkerExit) finish(true);
			};
			const onClose = (): void => {
				sawWorkerExit = true;
				finishIfClosed();
			};
			unsubscribe = this.onMessage(msg => {
				if (msg.type !== "closed") return;
				sawClosedAck = true;
				finishIfClosed();
			});
			worker.addEventListener("close", onClose);
			timeout = setTimeout(() => finish(false), workerCloseTimeoutMs);
			worker.postMessage({ type: "close" } satisfies WorkerInbound);
			return await closed;
		},
		async terminate() {
			worker.terminate();
		},
	};
}

function errorFromWorkerEvent(event: ErrorEvent): Error {
	if (event.error instanceof Error) return event.error;
	if (event.message) return new Error(event.message);
	return new Error("Unknown JS eval worker error");
}

/**
 * Inline fallback for environments where Bun cannot spawn the worker entry
 * (e.g. some test runners). Preserves behavior but cannot interrupt synchronous
 * infinite loops because user code runs on the main thread.
 */
function spawnInlineWorker(): WorkerHandle {
	const hostListeners = new Set<(message: WorkerOutbound) => void>();
	const workerListeners = new Set<(message: WorkerInbound) => void>();
	const workerTransport: Transport = {
		send: msg =>
			queueMicrotask(() => {
				for (const listener of hostListeners) listener(msg);
			}),
		onMessage: handler => {
			workerListeners.add(handler);
			return () => workerListeners.delete(handler);
		},
		close: () => {},
	};
	const core = new WorkerCore(workerTransport, {
		mode: "inline",
		interceptUnhandledRejections: postmortem.interceptUnhandledRejections,
	});
	return {
		mode: "inline",
		send: msg =>
			queueMicrotask(() => {
				for (const listener of workerListeners) listener(msg);
			}),
		onMessage: handler => {
			hostListeners.add(handler);
			return () => hostListeners.delete(handler);
		},
		onError: () => () => {},
		async close() {
			const { promise: closed, resolve } = Promise.withResolvers<boolean>();
			let settled = false;
			let timeout: NodeJS.Timeout | undefined;
			let unsubscribe = (): void => {};
			const finish = (value: boolean): void => {
				if (settled) return;
				settled = true;
				if (timeout) clearTimeout(timeout);
				unsubscribe();
				hostListeners.clear();
				workerListeners.clear();
				resolve(value);
			};
			unsubscribe = this.onMessage(msg => {
				if (msg.type === "closed") finish(true);
			});
			this.send({ type: "close" });
			timeout = setTimeout(() => finish(false), workerCloseTimeoutMs);
			return await closed;
		},
		async terminate() {
			hostListeners.clear();
			workerListeners.clear();
			core.dispose();
		},
	};
}
