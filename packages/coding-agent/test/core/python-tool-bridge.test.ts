import { afterAll, describe, expect, it } from "bun:test";
import type { AgentTool, AgentToolResult } from "@oh-my-pi/pi-agent-core";
import { buildManagedKernelEnvPatch } from "@oh-my-pi/pi-coding-agent/eval/executor-base";
import {
	disposePyToolBridge,
	disposePyToolBridgeByOwner,
	ensurePyToolBridge,
	registerPyToolBridge,
} from "@oh-my-pi/pi-coding-agent/eval/py/tool-bridge";
import type { ToolSession } from "@oh-my-pi/pi-coding-agent/tools";
import { INTENT_FIELD } from "@oh-my-pi/pi-wire";

interface FakeCall {
	id: string;
	args: unknown;
	signal?: AbortSignal;
}

function makeFakeTool(name: string, calls: FakeCall[], result: AgentToolResult): AgentTool {
	const tool = {
		name,
		label: name,
		description: name,
		parameters: { type: "object" },
		async execute(id: string, args: unknown, signal?: AbortSignal): Promise<AgentToolResult> {
			calls.push({ id, args, signal });
			return result;
		},
	} as unknown as AgentTool;
	return tool;
}

function makeSession(tools: Map<string, AgentTool>): ToolSession {
	return { getToolByName: (name: string) => tools.get(name) } as unknown as ToolSession;
}

async function call(
	info: { url: string; token: string },
	body: Record<string, unknown>,
	overrides?: { token?: string },
): Promise<Response> {
	return await fetch(`${info.url}/v1/tool`, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			Authorization: `Bearer ${overrides?.token ?? info.token}`,
		},
		body: JSON.stringify(body),
	});
}

async function callTokenlessEvalBroker(info: { evalUrl?: string }, body: Record<string, unknown>): Promise<Response> {
	expect(info.evalUrl).toMatch(/\/v1\/eval-tool$/);
	return await fetch(info.evalUrl!, {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify(body),
	});
}

describe("Python tool bridge HTTP server", () => {
	it("builds retained-runtime env patches without the authenticated bearer", () => {
		const patch = buildManagedKernelEnvPatch({
			bridge: {
				url: "http://127.0.0.1:1",
				token: "must-not-enter-runner",
				evalUrl: "http://127.0.0.1:1/v1/eval-tool",
			},
			bridgeSessionId: "session",
		});
		expect(patch).toEqual({
			PI_SESSION_FILE: null,
			PI_ARTIFACTS_DIR: null,
			PI_TOOL_BRIDGE_URL: "http://127.0.0.1:1/v1/eval-tool",
			PI_TOOL_BRIDGE_TOKEN: null,
			PI_TOOL_BRIDGE_SESSION: "session",
			PI_EVAL_LOCAL_ROOTS: null,
		});
	});

	afterAll(async () => {
		await disposePyToolBridge();
	});

	it("dispatches calls to the registered ToolSession and returns the tool value", async () => {
		const calls: FakeCall[] = [];
		const readTool = makeFakeTool("read", calls, {
			content: [{ type: "text", text: "file body" }],
		});
		const session = makeSession(new Map([["read", readTool]]));
		const info = await ensurePyToolBridge();
		const unregister = registerPyToolBridge("test-session-1", "run-1", { toolSession: session });
		try {
			const res = await call(info, {
				session: "test-session-1",
				run: "run-1",
				name: "read",
				args: { path: "foo.ts", [INTENT_FIELD]: "py prelude" },
			});
			const body = await res.json();
			expect(res.status).toBe(200);
			expect(body).toEqual({ ok: true, value: "file body" });
			expect(calls).toHaveLength(1);
			// `i` survives the bridge round trip so transcript renderers have a label.
			expect((calls[0]!.args as Record<string, unknown>)[INTENT_FIELD]).toBe("py prelude");
		} finally {
			unregister();
		}
	});

	it("dispatches the retained-runtime broker without exposing a bearer", async () => {
		const calls: FakeCall[] = [];
		const session = makeSession(
			new Map([["read", makeFakeTool("read", calls, { content: [{ type: "text", text: "broker body" }] })]]),
		);
		const info = await ensurePyToolBridge();
		const unregister = registerPyToolBridge("broker-session", "broker-run", { toolSession: session });
		try {
			const res = await callTokenlessEvalBroker(info, {
				session: "broker-session",
				run: "broker-run",
				name: "read",
				args: { path: "foo.ts", [INTENT_FIELD]: "py prelude" },
			});
			expect(res.status).toBe(200);
			expect(await res.json()).toEqual({ ok: true, value: "broker body" });
			expect(calls).toHaveLength(1);
		} finally {
			unregister();
		}
	});

	it("does not reuse a registration for a different run id", async () => {
		const calls: FakeCall[] = [];
		const session = makeSession(
			new Map([["read", makeFakeTool("read", calls, { content: [{ type: "text", text: "ok" }] })]]),
		);
		const info = await ensurePyToolBridge();
		const unregister = registerPyToolBridge("scoped-session", "run-a", { toolSession: session });
		try {
			const res = await call(info, { session: "scoped-session", run: "run-b", name: "read", args: {} });
			const body = (await res.json()) as { ok: boolean; error?: string };
			expect(body.ok).toBe(false);
			expect(body.error).toContain("No active eval tool bridge session");
			expect(calls).toHaveLength(0);
		} finally {
			unregister();
		}
	});

	it("returns ok=false when no session is registered for the given id", async () => {
		const info = await ensurePyToolBridge();
		const res = await call(info, { session: "missing", run: "run-missing", name: "read", args: {} });
		expect(res.status).toBe(200);
		const body = (await res.json()) as { ok: boolean; error?: string };
		expect(body.ok).toBe(false);
		expect(typeof body.error).toBe("string");
	});

	it("surfaces tool errors as ok=false with the error message", async () => {
		const session = {
			getToolByName: (_: string) =>
				({
					name: "boom",
					label: "boom",
					description: "boom",
					parameters: { type: "object" },
					async execute(): Promise<AgentToolResult> {
						throw new Error("kapow");
					},
				}) as unknown as AgentTool,
		} as unknown as ToolSession;
		const info = await ensurePyToolBridge();
		const unregister = registerPyToolBridge("err-session", "run-err", { toolSession: session });
		try {
			const res = await call(info, { session: "err-session", run: "run-err", name: "boom", args: {} });
			expect(res.status).toBe(200);
			const body = await res.json();
			expect(body).toEqual({ ok: false, error: "kapow" });
		} finally {
			unregister();
		}
	});

	it("rejects requests with a bad bearer token", async () => {
		const info = await ensurePyToolBridge();
		const res = await call(
			info,
			{ session: "anything", run: "run-anything", name: "read", args: {} },
			{ token: "wrong" },
		);
		expect(res.status).toBe(403);
	});

	it("returns 400 when body is missing required fields", async () => {
		const info = await ensurePyToolBridge();
		const res = await call(info, { name: "read" });
		expect(res.status).toBe(400);
	});

	it("drops a late tool completion after its registration is disposed", async () => {
		const deferred = Promise.withResolvers<AgentToolResult>();
		const statusEvents: Array<{ op: string }> = [];
		const tool = {
			name: "slow",
			label: "slow",
			description: "slow",
			parameters: { type: "object" },
			async execute(): Promise<AgentToolResult> {
				return await deferred.promise;
			},
		} as unknown as AgentTool;
		const info = await ensurePyToolBridge();
		const unregister = registerPyToolBridge("late-session", "late-run", {
			toolSession: makeSession(new Map([["slow", tool]])),
			emitStatus: event => statusEvents.push(event),
		});
		const response = call(info, { session: "late-session", run: "late-run", name: "slow", args: {} });
		await new Promise(resolve => setTimeout(resolve, 0));
		unregister();
		deferred.resolve({ content: [{ type: "text", text: "late" }] });
		const body = (await (await response).json()) as { ok: boolean; error?: string };
		expect(body.ok).toBe(false);
		expect(body.error).toContain("aborted");
		expect(statusEvents).toHaveLength(0);
	});

	it("aborts and fences an in-flight same-key registration replacement", async () => {
		const oldStarted = Promise.withResolvers<void>();
		const oldRelease = Promise.withResolvers<void>();
		let oldSignal: AbortSignal | undefined;
		const oldTool = {
			name: "slow",
			label: "slow",
			description: "slow",
			parameters: { type: "object" },
			async execute(_id: string, _args: unknown, signal?: AbortSignal): Promise<AgentToolResult> {
				oldSignal = signal;
				oldStarted.resolve();
				await oldRelease.promise;
				return { content: [{ type: "text", text: "old" }] };
			},
		} as unknown as AgentTool;
		const newTool = makeFakeTool("slow", [], { content: [{ type: "text", text: "new" }] });
		const info = await ensurePyToolBridge();
		const oldEntry = {
			toolSession: makeSession(new Map([["slow", oldTool]])),
			ownerId: "owner-old",
		};
		const oldUnregister = registerPyToolBridge("replacement", "same-run", oldEntry);
		try {
			const oldResponse = call(info, { session: "replacement", run: "same-run", name: "slow", args: {} });
			await oldStarted.promise;

			const newEntry = {
				toolSession: makeSession(new Map([["slow", newTool]])),
				ownerId: "owner-new",
			};
			const newUnregister = registerPyToolBridge("replacement", "same-run", newEntry);
			try {
				expect(oldSignal?.aborted).toBe(true);
				const newResponse = await call(info, {
					session: "replacement",
					run: "same-run",
					name: "slow",
					args: {},
				});
				expect(await newResponse.json()).toEqual({ ok: true, value: "new" });

				// The stale closure must not unregister the replacement generation.
				oldUnregister();
				const stillCurrent = await call(info, {
					session: "replacement",
					run: "same-run",
					name: "slow",
					args: {},
				});
				expect(await stillCurrent.json()).toEqual({ ok: true, value: "new" });

				const oldBody = (await (await oldResponse).json()) as { ok: boolean; error?: string };
				expect(oldBody.ok).toBe(false);
				expect(oldBody.error).toContain("aborted");
			} finally {
				newUnregister();
			}
		} finally {
			oldRelease.resolve();
			oldUnregister();
		}
	});

	it("keeps same-entry re-registration generations independent", async () => {
		const calls: FakeCall[] = [];
		const entry = {
			toolSession: makeSession(
				new Map([["read", makeFakeTool("read", calls, { content: [{ type: "text", text: "ok" }] })]]),
			),
		};
		const info = await ensurePyToolBridge();
		const firstUnregister = registerPyToolBridge("same-entry", "run", entry);
		firstUnregister();
		const secondUnregister = registerPyToolBridge("same-entry", "run", entry);
		try {
			// Calling the stale first closure must not remove the second generation.
			firstUnregister();
			const response = await call(info, { session: "same-entry", run: "run", name: "read", args: {} });
			expect(await response.json()).toEqual({ ok: true, value: "ok" });
		} finally {
			secondUnregister();
		}
	});

	it("disposes only registrations owned by the requested session", async () => {
		const callsA: FakeCall[] = [];
		const callsB: FakeCall[] = [];
		const info = await ensurePyToolBridge();
		const unregisterA = registerPyToolBridge("owners", "run-a", {
			toolSession: makeSession(
				new Map([["read", makeFakeTool("read", callsA, { content: [{ type: "text", text: "a" }] })]]),
			),
			ownerId: "owner-a",
		});
		const unregisterB = registerPyToolBridge("owners", "run-b", {
			toolSession: makeSession(
				new Map([["read", makeFakeTool("read", callsB, { content: [{ type: "text", text: "b" }] })]]),
			),
			ownerId: "owner-b",
		});
		try {
			await disposePyToolBridgeByOwner("owner-a");
			const disposed = await call(info, { session: "owners", run: "run-a", name: "read", args: {} });
			expect((await disposed.json()) as { ok: boolean }).toEqual(expect.objectContaining({ ok: false }));
			const survivor = await call(info, { session: "owners", run: "run-b", name: "read", args: {} });
			expect(await survivor.json()).toEqual({ ok: true, value: "b" });
			expect(callsA).toHaveLength(0);
			expect(callsB).toHaveLength(1);
		} finally {
			unregisterA();
			unregisterB();
		}
	});

	it("blocks new registrations during global bridge disposal", async () => {
		await ensurePyToolBridge();
		const unregister = registerPyToolBridge("dispose-guard", "run", {
			toolSession: makeSession(new Map()),
		});
		const disposal = disposePyToolBridge();
		try {
			expect(() =>
				registerPyToolBridge("dispose-guard", "replacement", { toolSession: makeSession(new Map()) }),
			).toThrow("disposal is in progress");
		} finally {
			unregister();
			await disposal;
		}
		// The stopped server can be restarted after the guard is cleared.
		const restarted = await ensurePyToolBridge();
		expect(restarted.url).toContain("127.0.0.1");
		expect(restarted.token).toEqual(expect.any(String));
	});

	it("blocks only the disposing owner while retaining co-owner registrations", async () => {
		const info = await ensurePyToolBridge();
		const unregisterA = registerPyToolBridge("owner-guard", "run-a", {
			ownerId: "owner-a",
			toolSession: makeSession(new Map()),
		});
		const unregisterB = registerPyToolBridge("owner-guard", "run-b", {
			ownerId: "owner-b",
			toolSession: makeSession(new Map()),
		});
		const disposal = disposePyToolBridgeByOwner("owner-a");
		try {
			expect(() =>
				registerPyToolBridge("owner-guard", "run-a2", {
					ownerId: "owner-a",
					toolSession: makeSession(new Map()),
				}),
			).toThrow("owner disposal is in progress");
			// A co-owner is still allowed to attach while owner-a drains.
			const unregisterB2 = registerPyToolBridge("owner-guard", "run-b2", {
				ownerId: "owner-b",
				toolSession: makeSession(new Map()),
			});
			unregisterB2();
			await disposal;
			const survivor = await call(info, { session: "owner-guard", run: "run-b", name: "missing", args: {} });
			expect((await survivor.json()) as { ok: boolean }).toEqual(expect.objectContaining({ ok: false }));
		} finally {
			unregisterA();
			unregisterB();
			await disposal;
		}
	});

	it("invokes emitStatus alongside the tool result", async () => {
		const calls: FakeCall[] = [];
		const readTool = makeFakeTool("read", calls, {
			content: [{ type: "text", text: "abc" }],
		});
		const session = makeSession(new Map([["read", readTool]]));
		const info = await ensurePyToolBridge();
		const statusEvents: Array<{ op: string }> = [];
		const unregister = registerPyToolBridge("status-session", "run-status", {
			toolSession: session,
			emitStatus: event => statusEvents.push(event),
		});
		try {
			const res = await call(info, {
				session: "status-session",
				run: "run-status",
				name: "read",
				args: { path: "foo.ts" },
			});
			expect(res.status).toBe(200);
			expect(statusEvents).toHaveLength(1);
			expect(statusEvents[0]!.op).toBe("read");
		} finally {
			unregister();
		}
	});
});
