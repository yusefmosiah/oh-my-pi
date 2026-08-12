import type { AgentTool, AgentToolResult } from "@oh-my-pi/pi-agent-core";
import { logger } from "@oh-my-pi/pi-utils";
import { INTENT_FIELD } from "@oh-my-pi/pi-wire";
import type { ToolSession } from "../tools";
import { ToolError } from "../tools/tool-errors";
import { EVAL_AGENT_BRIDGE_NAME, runEvalAgent } from "./agent-bridge";
import { isEvalTimeoutControlEvent } from "./bridge-timeout";
import { EVAL_BUDGET_BRIDGE_NAME, type EvalBudgetResult, runEvalBudget } from "./budget-bridge";
import { EVAL_COMPLETION_BRIDGE_NAME, runEvalCompletion } from "./completion-bridge";
import { EVAL_CONCURRENCY_BRIDGE_NAME, type EvalConcurrencyResult, runEvalConcurrency } from "./concurrency-bridge";
import type { EvalStatusEvent } from "./types";

export type { EvalStatusEvent } from "./types";

export interface ToolBridgeOptions {
	session: ToolSession;
	signal?: AbortSignal;
	/** False after the owning eval cell has been unregistered. */
	isActive?: () => boolean;
	/** Agent isolation may finish its critical phase after the caller aborts. */
	allowAbortedCompletion?: boolean;
	emitStatus?: (event: EvalStatusEvent) => void;
}

export type ToolValue =
	| string
	| EvalBudgetResult
	| EvalConcurrencyResult
	| {
			text: string;
			details?: unknown;
			images?: Array<{ mimeType: string; data: string }>;
			hasError?: boolean;
	  };
function toolResultHasError(result: AgentToolResult): boolean {
	if ((result as { isError?: unknown }).isError === true) {
		return true;
	}
	if (!(result.details && typeof result.details === "object")) {
		return false;
	}
	return (result.details as { isError?: unknown }).isError === true;
}

function getTool(session: ToolSession, name: string): AgentTool {
	const tool = session.getToolByName?.(name);
	if (!tool) {
		throw new ToolError(`Unknown tool from eval runtime: ${name}`);
	}
	return tool;
}

function normalizeArgs(args: unknown): unknown {
	if (!args || typeof args !== "object" || Array.isArray(args)) {
		return args;
	}
	const record = { ...(args as Record<string, unknown>) };
	if (record[INTENT_FIELD] === undefined) {
		record[INTENT_FIELD] = "js prelude";
	}
	return record;
}

function summarizeToolResult(
	name: string,
	args: unknown,
	result: AgentToolResult,
	text: string,
	hasError: boolean,
): EvalStatusEvent {
	const record = (args && typeof args === "object" ? (args as Record<string, unknown>) : {}) as Record<
		string,
		unknown
	>;
	const details = (
		result.details && typeof result.details === "object" ? (result.details as Record<string, unknown>) : {}
	) as Record<string, unknown>;
	const withError = (event: EvalStatusEvent): EvalStatusEvent =>
		hasError ? { ...event, hasError: true, error: text.slice(0, 500) } : event;

	switch (name) {
		case "read":
			return withError({ op: "read", path: record.path, chars: text.length, preview: text.slice(0, 500) });
		case "write":
			return withError({
				op: "write",
				path: record.path,
				chars: typeof record.content === "string" ? record.content.length : 0,
			});
		case "grep":
			return withError({
				op: "grep",
				pattern: record.pattern,
				path: record.path,
				count: details.matchCount ?? undefined,
			});
		case "glob":
			return withError({
				op: "glob",
				pattern: record.pattern,
				count: details.fileCount ?? undefined,
				matches: Array.isArray(details.files) ? details.files.slice(0, 20) : undefined,
			});
		case "bash":
			return withError({
				op: "run",
				cmd: record.command,
				code: typeof details.exitCode === "number" ? details.exitCode : undefined,
				output: text.slice(0, 500),
			});
		default:
			return withError({ op: name, chars: text.length });
	}
}

export async function callSessionTool(name: string, args: unknown, options: ToolBridgeOptions): Promise<ToolValue> {
	const allowAbortedCompletion = options.allowAbortedCompletion === true;
	if ((!allowAbortedCompletion && options.signal?.aborted) || options.isActive?.() === false) {
		throw new Error(`eval tool ${JSON.stringify(name)} aborted`);
	}
	const handlerOptions: ToolBridgeOptions = {
		...options,
		allowAbortedCompletion,
		emitStatus: event => {
			if (options.isActive?.() === false) return;
			if (options.signal?.aborted && !isEvalTimeoutControlEvent(event)) return;
			options.emitStatus?.(event);
		},
	};
	const ensureActive = (): void => {
		if ((!allowAbortedCompletion && options.signal?.aborted) || options.isActive?.() === false) {
			throw options.signal?.reason instanceof Error
				? options.signal.reason
				: new Error(`eval tool ${JSON.stringify(name)} aborted`);
		}
	};
	if (name === EVAL_COMPLETION_BRIDGE_NAME) {
		const value = await runEvalCompletion(args, handlerOptions);
		ensureActive();
		return value;
	}
	if (name === EVAL_AGENT_BRIDGE_NAME) {
		const value = await runEvalAgent(args, handlerOptions);
		ensureActive();
		return value;
	}
	if (name === EVAL_BUDGET_BRIDGE_NAME) {
		const value = await runEvalBudget(args, handlerOptions);
		ensureActive();
		return value;
	}
	if (name === EVAL_CONCURRENCY_BRIDGE_NAME) {
		const value = await runEvalConcurrency(args, handlerOptions);
		ensureActive();
		return value;
	}
	const tool = getTool(options.session, name);
	const normalizedArgs = normalizeArgs(args);
	const toolCallId = `js-${name}-${crypto.randomUUID()}`;
	try {
		const result = await tool.execute(toolCallId, normalizedArgs, options.signal);
		// A backend may intentionally shield the cell's abort while a host
		// operation finishes. Never publish a late status event or value from
		// that operation after the cell has ended.
		if ((!allowAbortedCompletion && options.signal?.aborted) || options.isActive?.() === false) {
			throw options.signal?.reason instanceof Error
				? options.signal.reason
				: new Error(`eval tool ${JSON.stringify(name)} aborted`);
		}
		const textBlocks = result.content.filter(
			(content): content is { type: "text"; text: string } =>
				content.type === "text" && typeof content.text === "string",
		);
		const imageBlocks = result.content.filter(
			(content): content is { type: "image"; mimeType: string; data: string } =>
				content.type === "image" && typeof content.mimeType === "string" && typeof content.data === "string",
		);
		const text = textBlocks.map(block => block.text).join("");
		const hasError = toolResultHasError(result);
		if (!options.signal?.aborted && options.isActive?.() !== false) {
			options.emitStatus?.(summarizeToolResult(name, normalizedArgs, result, text, hasError));
		}
		if (result.details === undefined && imageBlocks.length === 0 && !hasError) {
			return text;
		}
		const value: Exclude<ToolValue, string> = {
			text,
			details: result.details,
		};
		if (imageBlocks.length > 0) {
			value.images = imageBlocks.map(block => ({
				mimeType: block.mimeType,
				data: block.data,
			}));
		}
		if (hasError) {
			value.hasError = true;
		}
		return value;
	} catch (error) {
		if (!options.signal?.aborted && options.isActive?.() !== false) {
			options.emitStatus?.({
				op: name,
				error: error instanceof Error ? error.message : String(error),
			});
		}
		throw error;
	}
}

/** Host-side registration for any external eval runtime (Python, Ruby, Julia, Go, ...). */
export interface ToolBridgeEntry {
	toolSession: ToolSession;
	/** Raw turn cancellation delivered to tool implementations. */
	signal?: AbortSignal;
	/** Kernel-side abort shield used only to decide when the runtime may unwind. */
	shieldedSignal?: AbortSignal;
	emitStatus?: (event: EvalStatusEvent) => void;
	abortRequested?: () => boolean;
	/** Logical owner of this registration (usually the owning AgentSession). */
	ownerId?: string;
	/** Set false by unregister so late completions cannot publish into a new cell. */
	active?: boolean;
	/** Host-side teardown signal for requests that outlive the kernel process. */
	abortController?: AbortController;
	/** Keep an inactive entry addressable until existing HTTP calls settle. */
	pendingCalls?: number;
}

export interface ToolBridgeInfo {
	/** Authenticated endpoint retained for in-process/JS bridge callers. */
	url: string;
	/** Host-only bearer for the authenticated endpoint; never sent to retained runtimes. */
	token: string;
	/**
	 * Capability-broker endpoint for retained external runtimes. It is bound to
	 * loopback and still requires an active `(session, run)` registration, but
	 * deliberately has no bearer credential for user code to introspect.
	 */
	evalUrl?: string;
}

interface BridgeServer {
	info: ToolBridgeInfo;
	stop: () => Promise<void>;
}

/**
 * A registration is deliberately separate from the caller-owned entry. The
 * same entry object may be registered again for a reused `(session, run)`
 * tuple; storing generation state on the entry itself would let an old
 * unregister closure tear down the replacement.
 */
interface BridgeRegistration {
	key: string;
	entry: ToolBridgeEntry;
	ownerId?: string;
	controller: AbortController;
	active: boolean;
	pendingCalls: number;
}

const registrations = new Map<string, BridgeRegistration>();
// Replaced registrations are no longer addressable by new HTTP requests, but
// their handlers retain a record until the in-flight call has settled.
const retiredRegistrations = new Set<BridgeRegistration>();
const currentEntryRegistration = new WeakMap<ToolBridgeEntry, BridgeRegistration>();
// Underlying host tool promises can outlive the HTTP request when a shielded
// bridge call is aborted. Keep them visible to bounded bridge disposal so a
// session teardown does not mistake a raced response for completed host work.
const activeCalls = new Map<Promise<unknown>, string | undefined>();
let serverPromise: Promise<BridgeServer> | null = null;
let idleStopPromise: Promise<void> | null = null;
let idleStopTimer: ReturnType<typeof setTimeout> | undefined;
const MAX_BRIDGE_REQUEST_BYTES = 4 * 1024 * 1024;
const MAX_BRIDGE_RESPONSE_BYTES = 32 * 1024 * 1024;
const BRIDGE_DRAIN_TIMEOUT_MS = 5_000;

// Server startup/idle-stop/disposal are process-global. Serialize those
// transitions so dispose cannot stop a server while ensureToolBridge is still
// awaiting its startup promise (or return the just-stopped server to a caller).
let lifecycleTail: Promise<void> = Promise.resolve();
let bridgeDisposalInProgress = false;
let bridgeDisposalPromise: Promise<void> | null = null;
// Synchronous admission fence raised before an idle listener stop starts. A
// registration arriving after this point must not capture the listener being
// stopped; callers can retry after ensureToolBridge starts a fresh generation.
let bridgeStopInProgress = false;
// Owner-scoped disposal has the same synchronous admission barrier as global
// disposal, but only for registrations owned by that AgentSession. Co-owners
// remain allowed to register and keep the shared listener alive.
const ownerDisposals = new Map<string, Promise<void>>();
function withLifecycleLock<T>(operation: () => Promise<T>): Promise<T> {
	const prior = lifecycleTail;
	let release!: () => void;
	lifecycleTail = new Promise<void>(resolve => {
		release = resolve;
	});
	return prior
		.catch(() => undefined)
		.then(operation)
		.finally(release);
}

function jsonResponse(value: unknown, status = 200): Response {
	let serialized: string;
	try {
		serialized = JSON.stringify(value);
	} catch (error) {
		return Response.json(
			{
				ok: false,
				error: `Unable to encode bridge response: ${error instanceof Error ? error.message : String(error)}`,
			},
			{ status: 500 },
		);
	}
	if (new TextEncoder().encode(serialized).byteLength > MAX_BRIDGE_RESPONSE_BYTES) {
		return Response.json({ ok: false, error: "Bridge response exceeds the maximum size" }, { status: 413 });
	}
	return new Response(serialized, { status, headers: { "Content-Type": "application/json" } });
}

async function readJsonBody(req: Request): Promise<{ value?: unknown; error?: string }> {
	const contentLength = req.headers.get("content-length");
	const parsedLength = contentLength === null ? undefined : Number(contentLength);
	if (parsedLength !== undefined && Number.isFinite(parsedLength) && parsedLength > MAX_BRIDGE_REQUEST_BYTES) {
		return { error: "Bridge request exceeds the maximum size" };
	}
	if (!req.body) return { error: "Request body is required" };
	const reader = req.body.getReader();
	const chunks: Uint8Array[] = [];
	let total = 0;
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			if (value) {
				total += value.byteLength;
				if (total > MAX_BRIDGE_REQUEST_BYTES) {
					await reader.cancel();
					return { error: "Bridge request exceeds the maximum size" };
				}
				chunks.push(value);
			}
		}
	} finally {
		reader.releaseLock();
	}
	const body = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		body.set(chunk, offset);
		offset += chunk.byteLength;
	}
	try {
		return { value: JSON.parse(new TextDecoder().decode(body)) };
	} catch {
		return { error: "Invalid JSON body" };
	}
}

function hasBridgeWork(ownerId?: string): boolean {
	const matchesOwner = (candidate: string | undefined): boolean => ownerId === undefined || candidate === ownerId;
	for (const registration of registrations.values()) {
		if (registration.pendingCalls > 0 && matchesOwner(registration.ownerId)) return true;
	}
	for (const registration of retiredRegistrations) {
		if (registration.pendingCalls > 0 && matchesOwner(registration.ownerId)) return true;
	}
	for (const callOwner of activeCalls.values()) {
		if (matchesOwner(callOwner)) return true;
	}
	return ownerId === undefined ? registrations.size > 0 || retiredRegistrations.size > 0 : false;
}

function removeRegistration(registration: BridgeRegistration): void {
	if (registrations.get(registration.key) === registration) registrations.delete(registration.key);
	retiredRegistrations.delete(registration);
}

async function stopSharedToolBridgeLocked(): Promise<void> {
	// Owner disposal invokes this helper directly (rather than through the idle
	// timer), so the helper itself must fence synchronous registrations before
	// clearing `serverPromise` and awaiting the actual listener stop.
	const ownsStopFence = !bridgeStopInProgress;
	if (ownsStopFence) bridgeStopInProgress = true;
	const pending = serverPromise;
	serverPromise = null;
	try {
		if (!pending) return;
		await (await pending).stop();
	} catch (err) {
		logger.debug("Failed to stop eval tool bridge", { error: err instanceof Error ? err.message : String(err) });
	} finally {
		if (ownsStopFence) bridgeStopInProgress = false;
	}
}

function hasAddressableBridgeWork(): boolean {
	if (registrations.size > 0) return true;
	for (const registration of retiredRegistrations) {
		if (registration.pendingCalls > 0) return true;
	}
	return false;
}

function scheduleIdleBridgeStop(): void {
	if (bridgeStopInProgress) return;
	// Underlying host promises may intentionally outlive their HTTP request;
	// they no longer need a listener once their registration has drained.
	if (hasAddressableBridgeWork() || !serverPromise || idleStopPromise || idleStopTimer) return;
	idleStopTimer = setTimeout(() => {
		idleStopTimer = undefined;
		if (hasAddressableBridgeWork() || !serverPromise || idleStopPromise || bridgeStopInProgress) return;
		// Set this synchronously before entering the lifecycle queue. register()
		// is synchronous too, so it cannot attach between this check and lock
		// acquisition while the listener's stop is pending.
		bridgeStopInProgress = true;
		const stop = withLifecycleLock(async () => {
			// Active host promises no longer need the HTTP listener once their
			// registration/request has drained; they are tracked separately for
			// bounded disposal and cancellation diagnostics.
			if (hasAddressableBridgeWork() || !serverPromise) return;
			await stopSharedToolBridgeLocked();
		});
		idleStopPromise = stop;
		void stop.finally(() => {
			if (idleStopPromise === stop) idleStopPromise = null;
			bridgeStopInProgress = false;
		});
	}, 0);
	idleStopTimer.unref?.();
}

function retireRegistration(registration: BridgeRegistration, reason: string, schedule = true): void {
	if (registration.active) {
		registration.active = false;
		if (currentEntryRegistration.get(registration.entry) === registration) {
			registration.entry.active = false;
		}
		try {
			registration.controller.abort(new Error(reason));
		} catch {
			registration.controller.abort();
		}
	}
	if (registrations.get(registration.key) === registration) registrations.delete(registration.key);
	if (registration.pendingCalls > 0) retiredRegistrations.add(registration);
	else removeRegistration(registration);
	if (schedule) scheduleIdleBridgeStop();
}

function finishRegistrationCall(registration: BridgeRegistration): void {
	registration.pendingCalls = Math.max(0, registration.pendingCalls - 1);
	if (currentEntryRegistration.get(registration.entry) === registration) {
		registration.entry.pendingCalls = registration.pendingCalls;
	}
	if (!registration.active && registration.pendingCalls === 0) {
		removeRegistration(registration);
		scheduleIdleBridgeStop();
	}
}

async function callSessionToolPromptOnAbort(
	name: string,
	args: unknown,
	registration: BridgeRegistration,
): Promise<unknown> {
	const entry = registration.entry;
	if (!registration.active || entry.abortRequested?.()) {
		throw new Error(`bridge call ${JSON.stringify(name)} aborted: eval cell was interrupted`);
	}
	const teardownSignal = registration.controller.signal;
	const callSignal = entry.signal ? AbortSignal.any([entry.signal, teardownSignal]) : teardownSignal;
	const call = callSessionTool(name, args, {
		session: entry.toolSession,
		signal: callSignal,
		isActive: () => registration.active,
		allowAbortedCompletion:
			entry.shieldedSignal !== undefined && !entry.shieldedSignal.aborted && !teardownSignal.aborted,
		emitStatus: entry.emitStatus,
	});
	activeCalls.set(call, registration.ownerId);
	void call.then(
		() => {
			activeCalls.delete(call);
			scheduleIdleBridgeStop();
		},
		() => {
			activeCalls.delete(call);
			scheduleIdleBridgeStop();
		},
	);
	// The shield controls when the kernel may unwind, but registration teardown
	// must always wake the HTTP request. This prevents a replaced/owner-disposed
	// cell from retaining a request until a deferred isolation phase ends.
	const signal =
		entry.shieldedSignal && !entry.shieldedSignal.aborted
			? AbortSignal.any([entry.shieldedSignal, teardownSignal])
			: teardownSignal;
	if (signal.aborted) {
		void call.catch(() => {});
		throw new Error(`bridge call ${JSON.stringify(name)} aborted: eval cell was interrupted`);
	}
	const { promise: aborted, reject } = Promise.withResolvers<never>();
	const onAbort = () => reject(new Error(`bridge call ${JSON.stringify(name)} aborted: eval cell was interrupted`));
	signal.addEventListener("abort", onAbort, { once: true });
	try {
		const value = await Promise.race([call, aborted]);
		if (!registration.active) throw new Error(`bridge call ${JSON.stringify(name)} aborted: eval cell ended`);
		return value;
	} finally {
		signal.removeEventListener("abort", onAbort);
		void call.catch(() => {});
	}
}

async function startToolBridgeServer(): Promise<BridgeServer> {
	const token = crypto.randomUUID();
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		async fetch(req) {
			const url = new URL(req.url);
			const authenticated = url.pathname === "/v1/tool";
			const broker = url.pathname === "/v1/eval-tool";
			if (req.method !== "POST" || (!authenticated && !broker)) return new Response("Not Found", { status: 404 });
			// The retained Python/Ruby/Julia runners must not receive the bearer
			// used by the authenticated endpoint: arbitrary user code can inspect
			// every value in its own process. The broker route is loopback-only and
			// remains scoped by the active session/run registration below.
			if (authenticated && req.headers.get("authorization") !== `Bearer ${token}`)
				return new Response("Forbidden", { status: 403 });

			const parsed = await readJsonBody(req);
			if (parsed.error) {
				return jsonResponse({ ok: false, error: parsed.error }, parsed.error.includes("maximum size") ? 413 : 400);
			}
			const body = (parsed.value ?? {}) as { session?: unknown; run?: unknown; name?: unknown; args?: unknown };
			const sessionId = typeof body.session === "string" ? body.session : "";
			const runId = typeof body.run === "string" ? body.run : "";
			const name = typeof body.name === "string" ? body.name : "";
			if (!sessionId || !runId || !name) return jsonResponse({ ok: false, error: "Missing session/run/name" }, 400);
			const key = `${sessionId}:${runId}`;
			const registration = registrations.get(key);
			if (!registration?.active) {
				return jsonResponse({ ok: false, error: `No active eval tool bridge session: ${key}` });
			}

			registration.pendingCalls += 1;
			if (currentEntryRegistration.get(registration.entry) === registration) {
				registration.entry.pendingCalls = registration.pendingCalls;
			}
			try {
				const value = await callSessionToolPromptOnAbort(name, body.args, registration);
				return jsonResponse({ ok: true, value });
			} catch (err) {
				return jsonResponse({ ok: false, error: err instanceof Error ? err.message : String(err) });
			} finally {
				finishRegistrationCall(registration);
			}
		},
	});
	// The bridge is process-global infrastructure. Do not let an idle loopback
	// listener keep embedded/direct eval consumers alive after their kernels are
	// disposed; explicit disposeToolBridge() still stops it synchronously.
	(server as unknown as { unref?: () => void }).unref?.();
	const baseUrl = `http://${server.hostname}:${server.port}`;
	const info: ToolBridgeInfo = {
		// Preserve the historical root URL contract; callers append /v1/tool.
		url: baseUrl,
		token,
		evalUrl: `${baseUrl}/v1/eval-tool`,
	};
	logger.debug("Eval tool bridge listening", { url: info.url, evalUrl: info.evalUrl });
	return { info, stop: async () => await server.stop(true) };
}

/** Starts one process-local bridge server lazily and shares it across runtimes. */
export async function ensureToolBridge(): Promise<ToolBridgeInfo> {
	if (bridgeDisposalInProgress) {
		throw new Error("eval tool bridge disposal is in progress");
	}
	if (idleStopTimer) {
		clearTimeout(idleStopTimer);
		idleStopTimer = undefined;
	}
	// An idle stop is a normal listener transition, not a failed bridge
	// startup. Wait for it (if already begun), then retry the lifecycle check so
	// this caller receives a fresh listener generation instead of a transient
	// "stop in progress" error. `catch` is intentional: stopShared... already
	// logs stop failures, and a failed stop must not strand future callers.
	while (bridgeStopInProgress || idleStopPromise) {
		const stop = idleStopPromise;
		if (stop) await stop.catch(() => undefined);
		else await Bun.sleep(0);
		if (bridgeDisposalInProgress) {
			throw new Error("eval tool bridge disposal is in progress");
		}
	}
	while (true) {
		const info = await withLifecycleLock(async (): Promise<ToolBridgeInfo | undefined> => {
			if (bridgeDisposalInProgress) {
				throw new Error("eval tool bridge disposal is in progress");
			}
			// An idle stop can be queued ahead of this lock acquisition. Do not
			// attach to its server; release the lock and retry after the stop.
			if (bridgeStopInProgress) return undefined;
			if (!serverPromise) serverPromise = startToolBridgeServer();
			const pending = serverPromise;
			try {
				return (await pending).info;
			} catch (err) {
				if (serverPromise === pending) serverPromise = null;
				throw err;
			}
		});
		if (info) return info;
		while (bridgeStopInProgress || idleStopPromise) {
			const stop = idleStopPromise as Promise<void> | null;
			if (stop) await stop.catch(() => undefined);
			else await Bun.sleep(0);
			if (bridgeDisposalInProgress) {
				throw new Error("eval tool bridge disposal is in progress");
			}
		}
	}
}

export function registerToolBridge(sessionId: string, runId: string, entry: ToolBridgeEntry): () => void {
	if (bridgeDisposalInProgress) {
		throw new Error("eval tool bridge disposal is in progress");
	}
	if (entry.ownerId !== undefined && ownerDisposals.has(entry.ownerId)) {
		throw new Error(`eval tool bridge owner disposal is in progress: ${entry.ownerId}`);
	}
	if (bridgeStopInProgress) {
		throw new Error("eval tool bridge listener stop is in progress");
	}
	if (idleStopTimer) {
		clearTimeout(idleStopTimer);
		idleStopTimer = undefined;
	}
	const key = `${sessionId}:${runId}`;
	const previous = registrations.get(key);
	if (previous) {
		// A reused protocol identity must not leave the old cell authorized. Its
		// in-flight request remains fenced by this generation and its host work is
		// canceled through the registration-local controller.
		retireRegistration(previous, "eval tool bridge registration replaced", false);
	}
	const controller = new AbortController();
	const registration: BridgeRegistration = {
		key,
		entry,
		ownerId: entry.ownerId,
		controller,
		active: true,
		pendingCalls: 0,
	};
	currentEntryRegistration.set(entry, registration);
	entry.active = true;
	entry.abortController = controller;
	entry.pendingCalls = 0;
	registrations.set(key, registration);
	return () => {
		retireRegistration(registration, "eval cell ended");
	};
}

async function waitForBridgeDrain(ownerId?: string): Promise<void> {
	const deadline = Date.now() + BRIDGE_DRAIN_TIMEOUT_MS;
	while (hasBridgeWork(ownerId) && Date.now() < deadline) {
		await Bun.sleep(Math.min(50, Math.max(1, deadline - Date.now())));
	}
}

/** Dispose only registrations owned by one AgentSession, retaining co-owners. */
export async function disposeToolBridgeByOwner(ownerId: string): Promise<void> {
	const existing = ownerDisposals.get(ownerId);
	if (existing) return await existing;
	const disposal = (async (): Promise<void> => {
		// Let ownerDisposals publish the admission barrier before abort listeners
		// run while retiring registrations. This keeps same-owner re-entry from
		// racing the synchronous controller.abort() callbacks below.
		await Promise.resolve();
		const records = new Set([...registrations.values(), ...retiredRegistrations]);
		for (const registration of records) {
			if (registration.ownerId === ownerId) retireRegistration(registration, "eval owner disposed", false);
		}
		await waitForBridgeDrain(ownerId);
		await withLifecycleLock(async () => {
			if (!hasAddressableBridgeWork()) await stopSharedToolBridgeLocked();
		});
	})();
	ownerDisposals.set(ownerId, disposal);
	try {
		await disposal;
	} finally {
		if (ownerDisposals.get(ownerId) === disposal) ownerDisposals.delete(ownerId);
	}
}

export async function disposeToolBridge(): Promise<void> {
	if (bridgeDisposalPromise) return await bridgeDisposalPromise;
	bridgeDisposalInProgress = true;
	const disposal = (async (): Promise<void> => {
		await withLifecycleLock(async () => {
			const records = new Set([...registrations.values(), ...retiredRegistrations]);
			for (const registration of records) retireRegistration(registration, "eval tool bridge disposed", false);
			// Keep inactive records and underlying calls visible until they drain. The
			// bounded wait prevents a shielded/abort-insensitive host operation from
			// making process teardown unbounded.
			await waitForBridgeDrain();
			registrations.clear();
			retiredRegistrations.clear();
			await stopSharedToolBridgeLocked();
		});
	})();
	bridgeDisposalPromise = disposal;
	try {
		await disposal;
	} finally {
		if (bridgeDisposalPromise === disposal) bridgeDisposalPromise = null;
		bridgeDisposalInProgress = false;
	}
}
