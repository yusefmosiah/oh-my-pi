import * as path from "node:path";

import { logger } from "@oh-my-pi/pi-utils";
import {
	attachSessionOwner,
	type CancelledErrorClass,
	getRemainingTimeoutMs,
	isCancellationError,
	isTimedOutCancellation,
	resolveOwnerScopedSessionKey,
	type SessionOwners,
	waitForPromiseWithCancellation,
} from "./executor-base";

interface KernelSessionRegistryOptions {
	sessionId?: string;
	kernelOwnerId?: string;
	interpreter?: string;
	reset?: boolean;
	signal?: AbortSignal;
	deadlineMs?: number;
	bridge?: unknown;
	bridgeSessionId?: string;
}

interface RegistryKernelShutdownResult {
	confirmed?: boolean;
}

interface RegistryKernel {
	isAlive(): boolean;
	shutdown(options?: { timeoutMs: number }): Promise<RegistryKernelShutdownResult>;
}

type DisposalState = { kind: "all" } | { kind: "owner"; ownerId: string } | { kind: "owners"; ownerIds: Set<string> };
type OperationEpoch = { all: number; owner: number; ownerKey?: string };

export interface KernelSession<TKernel extends RegistryKernel> extends SessionOwners {
	sessionKey: string;
	sessionId: string;
	cwd: string;
	kernel: TKernel;
}

interface StartingKernelSession<TSession> extends SessionOwners {
	promise: Promise<TSession>;
	cancelled?: boolean;
	settled?: boolean;
	waiters: Map<string, number>;
	completedWaiters: Map<string, number>;
	controller: AbortController;
	cleanupPromise?: Promise<void>;
	cleanupRemovalScheduled?: boolean;
}

interface ClaimedSession<TSession> {
	sessionKey: string;
	session: TSession;
	shutdownPromise: Promise<RegistryKernelShutdownResult>;
}

export interface KernelSessionRegistryContext<
	TKernel extends RegistryKernel,
	TOptions extends KernelSessionRegistryOptions,
	TSession extends KernelSession<TKernel>,
> {
	sessions: Map<string, TSession>;
	startKernel: (cwd: string, options: TOptions) => Promise<TKernel>;
	replaceSessionKernel: (session: TSession, cwd: string, options: TOptions) => Promise<TKernel>;
}

interface KernelSessionRegistryDescriptor<
	TKernel extends RegistryKernel,
	TOptions extends KernelSessionRegistryOptions,
	TResult,
	TSession extends KernelSession<TKernel>,
> {
	languageLabel: string;
	cancelledErrorClass: CancelledErrorClass;
	buildSessionKey: (sessionId: string, cwd: string, interpreter: string | undefined) => string;
	createSession: (session: KernelSession<TKernel>) => TSession;
	startKernel: (cwd: string, options: TOptions) => Promise<TKernel>;
	executeWithKernel: (kernel: TKernel, code: string, options: TOptions) => Promise<TResult>;
	waitForStartup?: (promise: Promise<TSession>, options: TOptions) => Promise<TSession>;
	replaceSessionKernel?: (
		session: TSession,
		cwd: string,
		options: TOptions,
		context: KernelSessionRegistryContext<TKernel, TOptions, TSession>,
	) => Promise<TKernel>;
	acquireLiveSessionKernel?: (
		session: TSession,
		cwd: string,
		options: TOptions,
		context: KernelSessionRegistryContext<TKernel, TOptions, TSession>,
	) => Promise<TKernel>;
	invalidateSession?: (session: TSession) => void;
	shutdownSession?: (session: TSession, resetting: boolean) => Promise<RegistryKernelShutdownResult>;
	clearResetsOnDisposeAll?: boolean;
	logBeforeReplacement?: boolean;
	isCancellation?: (error: unknown) => boolean;
	isTimedOutCancellation?: (error: unknown, signal?: AbortSignal) => boolean;
	validateKernel?: (session: TSession, kernel: TKernel) => boolean;
}

interface KernelSessionRegistry<TOptions extends KernelSessionRegistryOptions, TResult> {
	disposeAll(): Promise<void>;
	disposeByOwner(ownerId: string): Promise<void>;
	executeOnSession(code: string, cwd: string, options: TOptions): Promise<TResult>;
}

export function normalizeKernelSessionCwd(cwd: string): string {
	return path.resolve(cwd);
}

export function requireRemainingKernelTimeoutMs(
	deadlineMs: number | undefined,
	cancelledErrorClass: CancelledErrorClass,
): number | undefined {
	const remainingMs = getRemainingTimeoutMs(deadlineMs);
	if (remainingMs === undefined) return undefined;
	if (remainingMs <= 0) {
		throw new cancelledErrorClass(true);
	}
	return remainingMs;
}

export function formatSessionTimeoutAnnotation(timeoutMs?: number): string {
	if (timeoutMs === undefined) return "Command timed out";
	const secs = Math.max(1, Math.round(timeoutMs / 1000));
	return `Command timed out after ${secs} seconds`;
}

export function formatSessionKernelTimeoutAnnotation(timeoutMs: number | undefined, kernelKilled: boolean): string {
	const secs = timeoutMs === undefined ? undefined : Math.max(1, Math.round(timeoutMs / 1000));
	if (kernelKilled) {
		return "eval cell timed out and the kernel was unresponsive to interrupt; the kernel has been killed and will be recreated on the next call.";
	}
	const duration = secs === undefined ? "the configured timeout" : `${secs}s`;
	return `eval cell timed out after ${duration}; kernel interrupted but remains running. Reset the kernel via { reset: true } if state appears corrupted.`;
}

export function createKernelSessionRegistry<
	TKernel extends RegistryKernel,
	TOptions extends KernelSessionRegistryOptions,
	TResult,
	TSession extends KernelSession<TKernel>,
>(
	descriptor: KernelSessionRegistryDescriptor<TKernel, TOptions, TResult, TSession>,
): KernelSessionRegistry<TOptions, TResult> {
	const sessions = new Map<string, TSession>();
	const startingSessions = new Map<string, StartingKernelSession<TSession>>();
	const resettingSessions = new Map<string, Promise<void>>();
	const replacementSessions = new Map<string, { promise: Promise<TKernel>; deadlineMs?: number }>();
	const orphanedKernels = new Set<TKernel>();
	// Disposal is a registry-wide barrier. Retained kernels are process-scoped,
	// so allowing a new acquisition while an owner/global shutdown is awaiting
	// the old subprocess can resurrect a second kernel after cleanup completes.
	let disposalPromise: Promise<void> | null = null;
	let disposalState: DisposalState | null = null;
	const disposalRequests: DisposalState[] = [];
	let allDisposalEpoch = 0;
	const ownerDisposalEpoch = new Map<string, number>();
	// Keep an owner generation while an executeOnSession call can still observe
	// it. Once that call and its disposal request have settled, the generation
	// can be dropped: future callers start from generation zero and stale calls
	// no longer exist to be invalidated. This bounds retention by live work
	// rather than the number of owners that have ever disposed a session.
	const activeOwnerEpochRefs = new Map<string, Map<number, number>>();

	function hasPendingOwnerDisposal(ownerId: string): boolean {
		return disposalRequests.some(
			request =>
				(request.kind === "owner" && request.ownerId === ownerId) ||
				(request.kind === "owners" && request.ownerIds.has(ownerId)),
		);
	}

	function pruneOwnerDisposalEpoch(ownerId: string): void {
		if (activeOwnerEpochRefs.has(ownerId) || hasPendingOwnerDisposal(ownerId)) return;
		ownerDisposalEpoch.delete(ownerId);
	}

	function pruneOwnerDisposalEpochs(): void {
		for (const ownerId of ownerDisposalEpoch.keys()) pruneOwnerDisposalEpoch(ownerId);
	}

	function retainOperationEpoch(operationEpoch: OperationEpoch): void {
		const ownerKey = operationEpoch.ownerKey;
		if (ownerKey === undefined) return;
		let generations = activeOwnerEpochRefs.get(ownerKey);
		if (!generations) {
			generations = new Map();
			activeOwnerEpochRefs.set(ownerKey, generations);
		}
		generations.set(operationEpoch.owner, (generations.get(operationEpoch.owner) ?? 0) + 1);
	}

	function releaseOperationEpoch(operationEpoch: OperationEpoch): void {
		const ownerKey = operationEpoch.ownerKey;
		if (ownerKey === undefined) return;
		const generations = activeOwnerEpochRefs.get(ownerKey);
		if (!generations) return;
		const count = generations.get(operationEpoch.owner) ?? 0;
		if (count <= 1) generations.delete(operationEpoch.owner);
		else generations.set(operationEpoch.owner, count - 1);
		if (generations.size === 0) {
			activeOwnerEpochRefs.delete(ownerKey);
			pruneOwnerDisposalEpoch(ownerKey);
		}
	}

	async function shutdownAndTrackOrphan(kernel: TKernel): Promise<void> {
		try {
			const result = await kernel.shutdown();
			if (result.confirmed === false) orphanedKernels.add(kernel);
			else orphanedKernels.delete(kernel);
		} catch {
			orphanedKernels.add(kernel);
		}
	}

	async function drainOrphans(): Promise<void> {
		await Promise.allSettled([...orphanedKernels].map(kernel => shutdownAndTrackOrphan(kernel)));
	}

	function operationInvalidated(operationEpoch: OperationEpoch, options: TOptions): boolean {
		if (allDisposalEpoch > operationEpoch.all) return true;
		const ownerKey = operationEpoch.ownerKey ?? options.kernelOwnerId ?? options.sessionId;
		return ownerKey !== undefined && (ownerDisposalEpoch.get(ownerKey) ?? 0) > operationEpoch.owner;
	}

	function snapshotEpoch(options: TOptions, fallbackOwnerKey?: string): OperationEpoch {
		const ownerKey = options.kernelOwnerId ?? fallbackOwnerKey ?? options.sessionId;
		return {
			all: allDisposalEpoch,
			ownerKey,
			owner: ownerKey === undefined ? 0 : (ownerDisposalEpoch.get(ownerKey) ?? 0),
		};
	}

	function detachSessionOwner(session: SessionOwners, sessionId: string, ownerId: string | undefined): void {
		if (ownerId !== undefined) {
			session.ownerIds.delete(ownerId);
			return;
		}
		session.ownerIds.delete(`fallback:${sessionId}`);
		// A fallback owner is represented by a namespaced session-id key. Once
		// that caller detaches, no fallback ownership remains even if explicit
		// owners still keep the kernel alive.
		session.hasFallbackOwner = false;
	}

	function refreshDisposalState(): void {
		if (disposalRequests.some(request => request.kind === "all")) {
			disposalState = { kind: "all" };
			return;
		}
		const ownerIds = new Set(
			disposalRequests.flatMap(request =>
				request.kind === "owner" ? [request.ownerId] : request.kind === "owners" ? [...request.ownerIds] : [],
			),
		);
		if (ownerIds.size === 0) {
			disposalState = null;
		} else if (ownerIds.size === 1) {
			disposalState = { kind: "owner", ownerId: ownerIds.values().next().value as string };
		} else {
			disposalState = { kind: "owners", ownerIds };
		}
	}

	function disposalBlocks(options: TOptions): boolean {
		if (!disposalState) return false;
		if (disposalState.kind === "all") return true;
		if (options.kernelOwnerId === undefined) return true;
		return disposalState.kind === "owner"
			? disposalState.ownerId === options.kernelOwnerId
			: disposalState.ownerIds.has(options.kernelOwnerId);
	}

	function enqueueDisposal(
		request: DisposalState,
		operation: () => Promise<void>,
		prepare?: () => void,
	): Promise<void> {
		disposalRequests.push(request);
		refreshDisposalState();
		const prior = disposalPromise;
		// Run synchronous claims after publishing the barrier and before any
		// queued operation can acquire the old session. Claims are idempotent:
		// an earlier claim removes the session from the map, while a co-owner
		// leaves it present and this request only detaches its own owner below.
		prepare?.();
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
			// after removing it, only live operations may still require retention.
			pruneOwnerDisposalEpochs();
		});
		disposalPromise = tracked;
		return tracked;
	}

	function fallbackOwnerKey(sessionId: string): string {
		return `fallback:${sessionId}`;
	}

	function disposalOwnerKeys(session: SessionOwners, ownerId: string): string[] {
		const keys: string[] = [];
		if (session.ownerIds.has(ownerId)) keys.push(ownerId);
		const fallback = fallbackOwnerKey(ownerId);
		if (session.hasFallbackOwner && session.ownerIds.has(fallback)) keys.push(fallback);
		return keys;
	}

	function removeOwnerKeys(session: SessionOwners, keys: string[]): void {
		for (const key of keys) session.ownerIds.delete(key);
		if (keys.some(key => key.startsWith("fallback:"))) session.hasFallbackOwner = false;
	}

	function callerOwnsSession(session: SessionOwners, sessionId: string, ownerId: string | undefined): boolean {
		return ownerId !== undefined
			? session.ownerIds.has(ownerId)
			: session.hasFallbackOwner && session.ownerIds.has(fallbackOwnerKey(sessionId));
	}

	/**
	 * Claim sessions synchronously when owner disposal is requested. The
	 * disposal operation itself is queued behind any prior lifecycle work and
	 * therefore cannot scan the maps until a later microtask. Leaving a sole
	 * owner's session in `sessions` during that gap lets a different owner
	 * attach to a kernel which has already been claimed for shutdown. Co-owned
	 * sessions are only detached, so surviving owners can continue to reuse
	 * them while the disposing owner is removed.
	 */
	function restoreClaimedSessionIfAbsent(sessionKey: string, session: TSession): void {
		if (sessions.has(sessionKey) || startingSessions.has(sessionKey)) return;
		sessions.set(sessionKey, session);
	}

	function restoreClaimedSessionAfterReplacement(sessionKey: string, session: TSession): void {
		const starting = startingSessions.get(sessionKey);
		if (!starting) {
			restoreClaimedSessionIfAbsent(sessionKey, session);
			return;
		}
		// A replacement may still be starting when the claimed kernel's shutdown
		// fails. Do not put the old session back over that replacement; only
		// restore it after the replacement and any late-startup cleanup settle.
		void starting.promise
			.then(
				() => undefined,
				() => undefined,
			)
			.then(async () => {
				await starting.cleanupPromise?.catch(() => undefined);
				restoreClaimedSessionIfAbsent(sessionKey, session);
			});
	}

	function claimOwnerSessions(ownerId: string): ClaimedSession<TSession>[] {
		const claimed: ClaimedSession<TSession>[] = [];
		// A canceled startup is itself a lifecycle barrier. The owner disposal
		// operation re-checks ownership after that startup and its late-kernel
		// cleanup settle; allowing a co-owner to attach during that window is
		// intentional and prevents disposing a session that is no longer sole
		// owned. Claim immediately only when no such global re-check is pending.
		// A global disposal owns every live session while it waits for its
		// snapshot/startup barriers; its later pass must be the sole shutdown
		// owner, otherwise a queued owner request could double-shutdown a kernel.
		if (disposalRequests.some(request => request.kind === "all")) return claimed;
		if ([...startingSessions.values()].some(starting => disposalOwnerKeys(starting, ownerId).length > 0))
			return claimed;
		for (const [sessionKey, session] of [...sessions.entries()]) {
			const keys = disposalOwnerKeys(session, ownerId);
			if (keys.length === 0) continue;
			// Reset/replacement already owns this key's lifecycle. Let the queued
			// disposal operation join it instead of starting a concurrent shutdown.
			if (resettingSessions.has(sessionKey) || replacementSessions.has(sessionKey)) continue;
			if (session.ownerIds.size !== keys.length) {
				removeOwnerKeys(session, keys);
				continue;
			}
			descriptor.invalidateSession?.(session);
			if (sessions.get(sessionKey) === session) sessions.delete(sessionKey);
			const shutdownPromise = (async () => {
				// Generic replacement state lives in this registry. Python's
				// language-specific replacement is joined by its shutdown hook.
				await replacementSessions.get(sessionKey)?.promise.catch(() => undefined);
				return await shutdownSession(session, false);
			})();
			claimed.push({ sessionKey, session, shutdownPromise });
		}
		return claimed;
	}

	function startingWaiterKey(sessionId: string, ownerId: string | undefined): string {
		return ownerId === undefined ? `fallback:${sessionId}` : `owner:${ownerId}`;
	}

	function retainStartingWaiter(
		starting: StartingKernelSession<TSession>,
		sessionId: string,
		ownerId: string | undefined,
	): void {
		attachSessionOwner(starting, sessionId, ownerId);
		const key = startingWaiterKey(sessionId, ownerId);
		starting.waiters.set(key, (starting.waiters.get(key) ?? 0) + 1);
	}

	function releaseStartingWaiter(
		starting: StartingKernelSession<TSession>,
		sessionId: string,
		ownerId: string | undefined,
		failed: boolean,
	): boolean {
		const key = startingWaiterKey(sessionId, ownerId);
		const count = starting.waiters.get(key) ?? 0;
		if (count <= 1) starting.waiters.delete(key);
		else starting.waiters.set(key, count - 1);
		let detached = false;
		if (failed) {
			const completed = starting.completedWaiters.get(key) ?? 0;
			if (count <= 1 && completed === 0) {
				detachSessionOwner(starting, sessionId, ownerId);
				detached = true;
			}
		} else {
			starting.completedWaiters.set(key, (starting.completedWaiters.get(key) ?? 0) + 1);
		}
		if (!starting.settled && starting.waiters.size === 0) {
			starting.cancelled = true;
			starting.controller.abort(new descriptor.cancelledErrorClass(false));
		}
		return detached;
	}

	const context: KernelSessionRegistryContext<TKernel, TOptions, TSession> = {
		sessions,
		startKernel: descriptor.startKernel,
		replaceSessionKernel,
	};

	function waitForStartup(promise: Promise<TSession>, options: TOptions): Promise<TSession> {
		const startup = descriptor.waitForStartup?.(promise, options) ?? promise;
		return waitForPromiseWithCancellation(
			startup,
			options,
			descriptor.cancelledErrorClass,
			descriptor.isTimedOutCancellation,
		);
	}

	function isCurrent(session: TSession, kernel?: TKernel): boolean {
		return (
			sessions.get(session.sessionKey) === session &&
			(kernel === undefined ||
				(descriptor.validateKernel ? descriptor.validateKernel(session, kernel) : session.kernel === kernel))
		);
	}

	function throwIfCancelled(options: TOptions): void {
		if (!options.signal?.aborted) return;
		const timedOut =
			descriptor.isTimedOutCancellation?.(options.signal.reason, options.signal) ??
			isTimedOutCancellation(options.signal.reason, descriptor.cancelledErrorClass, options.signal);
		throw new descriptor.cancelledErrorClass(timedOut);
	}

	async function acquireSession(
		sessionKey: string,
		sessionId: string,
		cwd: string,
		options: TOptions,
		operationEpoch: OperationEpoch,
	): Promise<TSession> {
		if (disposalPromise && disposalBlocks(options)) throw new descriptor.cancelledErrorClass(false);
		const existing = sessions.get(sessionKey);
		if (existing) {
			attachSessionOwner(existing, sessionId, options.kernelOwnerId);
			return existing;
		}
		const starting = startingSessions.get(sessionKey);
		if (starting) {
			if (starting.cancelled) {
				// A disposal/canceled creator invalidated this startup. The entry is
				// retained until the late kernel (if any) has been shut down, so a
				// retry cannot overlap that helper. These waits still honor the new
				// caller's cancellation/deadline; the shared cleanup continues in the
				// background and remains the barrier for later callers.
				const settled = starting.promise.then(
					() => undefined,
					() => undefined,
				);
				await waitForPromiseWithCancellation(
					settled,
					options,
					descriptor.cancelledErrorClass,
					descriptor.isTimedOutCancellation,
				);
				if (starting.cleanupPromise) {
					await waitForPromiseWithCancellation(
						starting.cleanupPromise,
						options,
						descriptor.cancelledErrorClass,
						descriptor.isTimedOutCancellation,
					);
				}
				if (startingSessions.get(sessionKey) === starting) startingSessions.delete(sessionKey);
				throwIfCancelled(options);
				return await acquireSession(sessionKey, sessionId, cwd, options, operationEpoch);
			}
			retainStartingWaiter(starting, sessionId, options.kernelOwnerId);
			try {
				const result = await waitForStartup(starting.promise, options);
				releaseStartingWaiter(starting, sessionId, options.kernelOwnerId, false);
				return result;
			} catch (error) {
				const detached = releaseStartingWaiter(starting, sessionId, options.kernelOwnerId, true);
				const orphan = sessions.get(sessionKey);
				if (detached && orphan && callerOwnsSession(orphan, sessionId, options.kernelOwnerId)) {
					detachSessionOwner(orphan, sessionId, options.kernelOwnerId);
					if (orphan.ownerIds.size === 0) {
						descriptor.invalidateSession?.(orphan);
						if (sessions.get(sessionKey) === orphan) sessions.delete(sessionKey);
						await shutdownSession(orphan, false).catch(() => undefined);
					}
				}
				throw error;
			}
		}
		let startingSession!: StartingKernelSession<TSession>;
		const startupController = new AbortController();
		const startup = (async () => {
			if (allDisposalEpoch > operationEpoch.all) throw new descriptor.cancelledErrorClass(false);
			// The caller's abort must not tear down a startup shared by another
			// owner, but its absolute deadline remains the bounded startup budget.
			const startupOptions = { ...options, signal: startupController.signal } as TOptions;
			const kernelPromise = descriptor.startKernel(cwd, startupOptions);
			let kernelAccepted = false;
			try {
				const kernel = await waitForPromiseWithCancellation(
					kernelPromise,
					{ signal: startupController.signal, deadlineMs: options.deadlineMs },
					descriptor.cancelledErrorClass,
					descriptor.isTimedOutCancellation,
				);
				kernelAccepted = true;
				if (startingSession.cancelled || allDisposalEpoch > operationEpoch.all) {
					await shutdownAndTrackOrphan(kernel);
					throw new descriptor.cancelledErrorClass(false);
				}
				const session = descriptor.createSession({
					sessionKey,
					sessionId,
					cwd,
					kernel,
					ownerIds: new Set(startingSession.ownerIds),
					hasFallbackOwner: startingSession.hasFallbackOwner,
				});
				if (startingSessions.get(sessionKey) === startingSession) {
					sessions.set(sessionKey, session);
				}
				return session;
			} catch (error) {
				startupController.abort(error);
				if (!kernelAccepted) {
					// A cancellation/deadline can win before startKernel settles. Keep a
					// per-start cleanup barrier so disposal/retry cannot launch a new
					// process while the late helper is still being shut down.
					startingSession.cleanupPromise = kernelPromise
						.then(kernel => shutdownAndTrackOrphan(kernel))
						.catch(() => undefined);
				}
				throw error;
			}
		})();
		startingSession = {
			ownerIds: new Set(),
			hasFallbackOwner: false,
			promise: startup,
			waiters: new Map(),
			completedWaiters: new Map(),
			controller: startupController,
		};
		retainStartingWaiter(startingSession, sessionId, options.kernelOwnerId);
		startingSessions.set(sessionKey, startingSession);
		const removeStarting = () => {
			if (startingSessions.get(sessionKey) !== startingSession) return;
			const cleanup = startingSession.cleanupPromise;
			if (!cleanup) {
				startingSessions.delete(sessionKey);
				return;
			}
			// startup rejects as soon as its cancellation race wins, but the
			// underlying startKernel promise can settle much later. Keep the map
			// entry through that cleanup barrier to serialize retries.
			if (startingSession.cleanupRemovalScheduled) return;
			startingSession.cleanupRemovalScheduled = true;
			void cleanup.finally(() => {
				if (startingSessions.get(sessionKey) === startingSession) startingSessions.delete(sessionKey);
			});
		};
		void startup.then(
			() => {
				startingSession.settled = true;
				removeStarting();
			},
			error => {
				startingSession.settled = true;
				removeStarting();
				void error;
			},
		);
		try {
			const result = await waitForStartup(startup, options);
			releaseStartingWaiter(startingSession, sessionId, options.kernelOwnerId, false);
			return result;
		} catch (error) {
			const detached = releaseStartingWaiter(startingSession, sessionId, options.kernelOwnerId, true);
			const orphan = sessions.get(sessionKey);
			if (detached && orphan && callerOwnsSession(orphan, sessionId, options.kernelOwnerId)) {
				detachSessionOwner(orphan, sessionId, options.kernelOwnerId);
				if (orphan.ownerIds.size === 0) {
					descriptor.invalidateSession?.(orphan);
					if (sessions.get(sessionKey) === orphan) sessions.delete(sessionKey);
					await shutdownSession(orphan, false).catch(() => undefined);
				}
			}
			throw error;
		}
	}

	async function replaceSessionKernel(session: TSession, cwd: string, options: TOptions): Promise<TKernel> {
		const replacementEpoch = snapshotEpoch(options, session.sessionId);
		if (operationInvalidated(replacementEpoch, options)) throw new descriptor.cancelledErrorClass(false);
		// Language-specific replacement hooks own their own generation/joins (the
		// Python executor also extends a shared replacement deadline when a later
		// owner joins). Keep them outside the generic promise map so those hooks
		// continue to observe every join instead of hiding it behind one waiter.
		if (descriptor.replaceSessionKernel) {
			return await descriptor.replaceSessionKernel(session, cwd, options, context);
		}
		const existing = replacementSessions.get(session.sessionKey);
		if (existing) {
			if (
				options.deadlineMs !== undefined &&
				(existing.deadlineMs === undefined || options.deadlineMs > existing.deadlineMs)
			) {
				existing.deadlineMs = options.deadlineMs;
			}
			return await waitForPromiseWithCancellation(
				existing.promise,
				options,
				descriptor.cancelledErrorClass,
				descriptor.isTimedOutCancellation,
			);
		}
		// Replacement startup is shared by every owner of this session. Do not
		// let the first caller's abort cancel a replacement still needed by a
		// co-owner; retain the wall-clock deadline as the bounded lifecycle cap.
		const replacementOptions = { ...options, signal: undefined } as TOptions;
		const replacementState: { promise: Promise<TKernel>; deadlineMs?: number } = {
			promise: Promise.resolve(undefined as never),
			deadlineMs: options.deadlineMs,
		};
		const replacement = (async (): Promise<TKernel> => {
			if (descriptor.logBeforeReplacement) {
				logger.warn(`${descriptor.languageLabel} subprocess died or is unresponsive; spawning fresh process`, {
					sessionKey: session.sessionKey,
				});
			}
			const old = session.kernel;
			if (operationInvalidated(replacementEpoch, options)) throw new descriptor.cancelledErrorClass(false);
			const remaining = getRemainingTimeoutMs(replacementState.deadlineMs);
			let shutdownResult: RegistryKernelShutdownResult;
			try {
				shutdownResult = await old.shutdown(
					remaining !== undefined ? { timeoutMs: Math.max(0, remaining) } : undefined,
				);
			} catch {
				throw new descriptor.cancelledErrorClass(getRemainingTimeoutMs(replacementState.deadlineMs) === 0);
			}
			if (shutdownResult.confirmed === false) {
				throw new descriptor.cancelledErrorClass(getRemainingTimeoutMs(replacementState.deadlineMs) === 0);
			}
			if (sessions.get(session.sessionKey) !== session || operationInvalidated(replacementEpoch, options)) {
				throw new descriptor.cancelledErrorClass(false);
			}
			requireRemainingKernelTimeoutMs(replacementState.deadlineMs, descriptor.cancelledErrorClass);
			const next = await descriptor.startKernel(cwd, {
				...replacementOptions,
				deadlineMs: replacementState.deadlineMs,
			});
			if (sessions.get(session.sessionKey) !== session || operationInvalidated(replacementEpoch, options)) {
				await shutdownAndTrackOrphan(next);
				throw new descriptor.cancelledErrorClass(false);
			}
			session.kernel = next;
			return next;
		})();
		replacementState.promise = replacement;
		replacementSessions.set(session.sessionKey, replacementState);
		void replacement.then(
			() => {
				if (replacementSessions.get(session.sessionKey) === replacementState)
					replacementSessions.delete(session.sessionKey);
			},
			() => {
				if (replacementSessions.get(session.sessionKey) === replacementState)
					replacementSessions.delete(session.sessionKey);
			},
		);
		return await waitForPromiseWithCancellation(
			replacement,
			options,
			descriptor.cancelledErrorClass,
			descriptor.isTimedOutCancellation,
		);
	}

	async function acquireLiveSessionKernel(session: TSession, cwd: string, options: TOptions): Promise<TKernel> {
		if (descriptor.acquireLiveSessionKernel) {
			return await descriptor.acquireLiveSessionKernel(session, cwd, options, context);
		}
		if (!isCurrent(session)) throw new descriptor.cancelledErrorClass(false);
		if (!session.kernel.isAlive()) await replaceSessionKernel(session, cwd, options);
		if (!isCurrent(session)) throw new descriptor.cancelledErrorClass(false);
		return session.kernel;
	}

	async function shutdownSession(session: TSession, resetting: boolean): Promise<RegistryKernelShutdownResult> {
		return await (descriptor.shutdownSession?.(session, resetting) ?? session.kernel.shutdown());
	}

	async function executeWithOperationFence(
		session: TSession,
		kernel: TKernel,
		code: string,
		runOptions: TOptions,
		operationEpoch: OperationEpoch,
		options: TOptions,
	): Promise<TResult> {
		const result = await descriptor.executeWithKernel(kernel, code, runOptions);
		// Disposal can invalidate the session while the cell is running. Do not
		// publish a successful result from a kernel that has since been disposed,
		// replaced, or detached from this registry operation.
		if (operationInvalidated(operationEpoch, options) || !isCurrent(session, kernel)) {
			throw new descriptor.cancelledErrorClass(false);
		}
		return result;
	}

	async function resetSession(sessionKey: string): Promise<void> {
		await replacementSessions.get(sessionKey)?.promise.catch(() => undefined);
		const existing =
			sessions.get(sessionKey) ?? (await startingSessions.get(sessionKey)?.promise.catch(() => undefined));
		if (!existing) return;
		descriptor.invalidateSession?.(existing);
		if (sessions.get(sessionKey) === existing) sessions.delete(sessionKey);
		try {
			const result = await shutdownSession(existing, true);
			if (result.confirmed === false) {
				if (!sessions.has(sessionKey)) sessions.set(sessionKey, existing);
				throw new descriptor.cancelledErrorClass(false);
			}
		} catch (error) {
			if (!sessions.has(sessionKey)) sessions.set(sessionKey, existing);
			throw error;
		}
	}

	async function disposeAll(): Promise<void> {
		allDisposalEpoch += 1;
		return await enqueueDisposal({ kind: "all" }, async () => {
			const pendingResets = [...resettingSessions.values()];
			const pendingReplacements = [...replacementSessions.values()].map(replacement => replacement.promise);
			// Invalidate and abort every shared startup before waiting on resets.
			// A reset can itself be waiting for startup, and startKernel is allowed
			// to ignore its caller signal; waitForPromiseWithCancellation still
			// settles the registry promptly and late kernels are tracked by the
			// startup catch path.
			const pendingStarting = [...startingSessions.values()];
			for (const starting of pendingStarting) {
				starting.cancelled = true;
				starting.controller.abort(new descriptor.cancelledErrorClass(false));
			}
			await Promise.allSettled(pendingReplacements);
			const pending = pendingStarting.map(starting => starting.promise);
			const resetsDone = Promise.allSettled(pendingResets);
			await resetsDone;
			if (descriptor.clearResetsOnDisposeAll) resettingSessions.clear();
			const started = await Promise.allSettled(pending);
			await Promise.allSettled(
				pendingStarting.map(starting => starting.cleanupPromise).filter((p): p is Promise<void> => Boolean(p)),
			);
			const all = [...sessions.entries()];
			for (const result of started) {
				if (result.status !== "fulfilled") continue;
				if (!all.some(([, session]) => session === result.value)) all.push([result.value.sessionKey, result.value]);
			}
			for (const [id, session] of all) {
				descriptor.invalidateSession?.(session);
				if (sessions.get(id) === session) sessions.delete(id);
			}
			const results = await Promise.allSettled(all.map(([, session]) => shutdownSession(session, false)));
			for (let i = 0; i < all.length; i += 1) {
				const [id, session] = all[i];
				const result = results[i];
				if (result.status === "fulfilled" && result.value?.confirmed !== false) continue;
				const reason = result.status === "rejected" ? result.reason : "not confirmed";
				logger.warn(`${descriptor.languageLabel} kernel shutdown not confirmed`, {
					sessionId: session.sessionId,
					sessionKey: id,
					cwd: session.cwd,
					reason,
				});
				if (!sessions.has(id)) sessions.set(id, session);
			}
			await drainOrphans();
		});
	}

	async function disposeByOwner(ownerId: string): Promise<void> {
		ownerDisposalEpoch.set(ownerId, (ownerDisposalEpoch.get(ownerId) ?? 0) + 1);
		let claimed: ClaimedSession<TSession>[] = [];
		return await enqueueDisposal(
			{ kind: "owner", ownerId },
			async () => {
				const pendingResets = [...resettingSessions.values()];
				const pendingReplacements = [...replacementSessions.values()].map(replacement => replacement.promise);
				await Promise.allSettled(pendingReplacements);
				await Promise.allSettled(pendingResets);
				let toShutdown: TSession[] = claimed.map(entry => entry.session);
				const startingToShutdown: StartingKernelSession<TSession>[] = [];
				const claimedSessions = new Set(toShutdown);
				for (const session of [...sessions.values()]) {
					if (claimedSessions.has(session)) continue;
					const keys = disposalOwnerKeys(session, ownerId);
					if (keys.length === 0) continue;
					if (session.ownerIds.size === keys.length) {
						// Defer removal until canceled startups settle. A co-owner can
						// legitimately attach during that cleanup window; the final
						// ownership check below then preserves the live session.
						toShutdown.push(session);
						continue;
					}
					removeOwnerKeys(session, keys);
				}
				for (const [sessionKey, starting] of [...startingSessions.entries()]) {
					if (sessions.has(sessionKey)) continue;
					const keys = disposalOwnerKeys(starting, ownerId);
					if (keys.length === 0) continue;
					if (starting.ownerIds.size === keys.length) {
						starting.cancelled = true;
						starting.controller.abort(new descriptor.cancelledErrorClass(false));
						startingToShutdown.push(starting);
						continue;
					}
					removeOwnerKeys(starting, keys);
				}
				const started = await Promise.allSettled(startingToShutdown.map(starting => starting.promise));
				await Promise.allSettled(
					startingToShutdown
						.map(starting => starting.cleanupPromise)
						.filter((p): p is Promise<void> => Boolean(p)),
				);
				const retained: TSession[] = [];
				for (const session of toShutdown) {
					if (claimedSessions.has(session)) {
						retained.push(session);
						continue;
					}
					const keys = disposalOwnerKeys(session, ownerId);
					if (session.ownerIds.size > keys.length) {
						removeOwnerKeys(session, keys);
						continue;
					}
					descriptor.invalidateSession?.(session);
					if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
					retained.push(session);
				}
				toShutdown = retained;
				for (const result of started) {
					if (result.status !== "fulfilled") continue;
					const session = result.value;
					descriptor.invalidateSession?.(session);
					if (sessions.get(session.sessionKey) === session) sessions.delete(session.sessionKey);
					toShutdown.push(session);
				}
				const claimedResults = await Promise.allSettled(claimed.map(entry => entry.shutdownPromise));
				const remaining = toShutdown.filter(session => !claimedSessions.has(session));
				const results = await Promise.allSettled(remaining.map(session => shutdownSession(session, false)));
				for (let i = 0; i < toShutdown.length; i += 1) {
					const session = toShutdown[i];
					const result = claimedSessions.has(session)
						? claimedResults[claimed.findIndex(entry => entry.session === session)]
						: results[remaining.indexOf(session)];
					if (result.status === "fulfilled" && result.value?.confirmed !== false) {
						removeOwnerKeys(session, disposalOwnerKeys(session, ownerId));
						continue;
					}
					const reason = result.status === "rejected" ? result.reason : "not confirmed";
					logger.warn(`${descriptor.languageLabel} kernel shutdown not confirmed`, {
						sessionId: session.sessionId,
						sessionKey: session.sessionKey,
						cwd: session.cwd,
						reason,
					});
					if (claimedSessions.has(session)) {
						const replacementActive =
							sessions.has(session.sessionKey) || startingSessions.has(session.sessionKey);
						if (replacementActive) orphanedKernels.add(session.kernel);
						restoreClaimedSessionAfterReplacement(session.sessionKey, session);
					} else if (!sessions.has(session.sessionKey)) sessions.set(session.sessionKey, session);
				}
				await drainOrphans();
				claimed = [];
			},
			() => {
				claimed = claimOwnerSessions(ownerId);
			},
		);
	}

	async function executeOnSession(code: string, cwd: string, options: TOptions): Promise<TResult> {
		if (disposalPromise && disposalBlocks(options)) throw new descriptor.cancelledErrorClass(false);
		throwIfCancelled(options);
		const operationEpoch = snapshotEpoch(options);
		retainOperationEpoch(operationEpoch);
		try {
			const sessionId = options.sessionId ?? `session:${cwd}`;
			const sessionKey = resolveOwnerScopedSessionKey({
				baseKey: descriptor.buildSessionKey(sessionId, cwd, options.interpreter),
				ownerId: options.kernelOwnerId,
				reset: options.reset === true,
				hasSession: key => sessions.has(key) || startingSessions.has(key),
				getOwners: key => sessions.get(key) ?? startingSessions.get(key),
			});
			if (options.bridge && !options.bridgeSessionId) {
				options.bridgeSessionId = sessionId;
			}
			if (options.reset) {
				const inFlight = resettingSessions.get(sessionKey);
				if (inFlight) {
					await waitForPromiseWithCancellation(
						inFlight,
						options,
						descriptor.cancelledErrorClass,
						descriptor.isTimedOutCancellation,
					).catch(error => {
						throw error;
					});
				} else {
					const resetPromise = resetSession(sessionKey);
					const trackedReset = resetPromise.then(() => undefined);
					resettingSessions.set(sessionKey, trackedReset);
					try {
						await waitForPromiseWithCancellation(
							resetPromise,
							options,
							descriptor.cancelledErrorClass,
							descriptor.isTimedOutCancellation,
						);
					} finally {
						if (resettingSessions.get(sessionKey) === trackedReset) resettingSessions.delete(sessionKey);
					}
				}
			} else {
				const inFlight = resettingSessions.get(sessionKey);
				if (inFlight) {
					await waitForPromiseWithCancellation(
						inFlight,
						options,
						descriptor.cancelledErrorClass,
						descriptor.isTimedOutCancellation,
					);
				}
			}
			const priorSession = sessions.get(sessionKey) ?? startingSessions.get(sessionKey);
			const callerAlreadyOwned = priorSession
				? callerOwnsSession(priorSession, sessionId, options.kernelOwnerId)
				: false;
			const session = await acquireSession(sessionKey, sessionId, cwd, options, operationEpoch);
			if (operationInvalidated(operationEpoch, options) || options.signal?.aborted) {
				if (
					!callerAlreadyOwned &&
					isCurrent(session) &&
					callerOwnsSession(session, sessionId, options.kernelOwnerId)
				) {
					detachSessionOwner(session, sessionId, options.kernelOwnerId);
					if (session.ownerIds.size === 0) {
						descriptor.invalidateSession?.(session);
						sessions.delete(sessionKey);
						await shutdownSession(session, false).catch(() => undefined);
					}
				}
				if (options.signal?.aborted) throwIfCancelled(options);
				throw new descriptor.cancelledErrorClass(false);
			}
			const kernel = await acquireLiveSessionKernel(session, cwd, options);
			if (operationInvalidated(operationEpoch, options)) throw new descriptor.cancelledErrorClass(false);
			if (!isCurrent(session, kernel)) throw new descriptor.cancelledErrorClass(false);
			const runOptions = { ...options, cwd };
			try {
				return await executeWithOperationFence(session, kernel, code, runOptions, operationEpoch, options);
			} catch (err) {
				// A disposal/reset/replacement that races an execution invalidates
				// both its success and its failure; do not leak a stale kernel error
				// or retry work after the operation has crossed the lifecycle fence.
				if (operationInvalidated(operationEpoch, options) || !isCurrent(session, kernel)) {
					throw new descriptor.cancelledErrorClass(false);
				}
				if (
					descriptor.isCancellation?.(err) ||
					isCancellationError(err, descriptor.cancelledErrorClass) ||
					options.signal?.aborted
				)
					throw err;
				if (kernel.isAlive()) throw err;
				let retryKernel: TKernel;
				if (descriptor.acquireLiveSessionKernel) {
					retryKernel = await acquireLiveSessionKernel(session, cwd, options);
				} else {
					if (!isCurrent(session, kernel)) throw new descriptor.cancelledErrorClass(false);
					retryKernel = await replaceSessionKernel(session, cwd, options);
				}
				if (operationInvalidated(operationEpoch, options) || options.signal?.aborted) {
					if (options.signal?.aborted) throwIfCancelled(options);
					throw new descriptor.cancelledErrorClass(false);
				}
				if (!isCurrent(session, retryKernel)) throw new descriptor.cancelledErrorClass(false);
				return await executeWithOperationFence(session, retryKernel, code, runOptions, operationEpoch, options);
			}
		} finally {
			releaseOperationEpoch(operationEpoch);
		}
	}

	return { disposeAll, disposeByOwner, executeOnSession };
}
