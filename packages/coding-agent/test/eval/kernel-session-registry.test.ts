import { describe, expect, it } from "bun:test";
import { createKernelSessionRegistry, type KernelSession } from "../../src/eval/kernel-session-registry";

class TestCancelledError extends Error {
	readonly timedOut: boolean;

	constructor(timedOut: boolean) {
		super(timedOut ? "timed out" : "cancelled");
		this.name = "AbortError";
		this.timedOut = timedOut;
	}
}

type TestOptions = {
	sessionId?: string;
	kernelOwnerId?: string;
	interpreter?: string;
	reset?: boolean;
	signal?: AbortSignal;
	deadlineMs?: number;
};
type TestResult = { ok: true };
type TestKernel = {
	isAlive: () => boolean;
	shutdown: (options?: { timeoutMs: number }) => Promise<{ confirmed: boolean }>;
};
type MutableTestKernel = TestKernel & { alive: boolean; shutdownCalls: number };
type TestSession = KernelSession<TestKernel>;

function makeRegistry(
	startKernel: (cwd: string, options: TestOptions) => Promise<TestKernel>,
	validateKernel?: (session: TestSession, kernel: TestKernel) => boolean,
) {
	return createKernelSessionRegistry<TestKernel, TestOptions, TestResult, TestSession>({
		languageLabel: "test",
		cancelledErrorClass: TestCancelledError,
		buildSessionKey: (sessionId, cwd, interpreter) => `${sessionId}\0${cwd}\0${interpreter ?? ""}`,
		startKernel,
		createSession: session => session,
		executeWithKernel: async () => ({ ok: true }),
		validateKernel,
	});
}

function fakeKernel(onShutdown: () => void): TestKernel {
	return {
		isAlive: () => true,
		shutdown: async () => {
			onShutdown();
			return { confirmed: true };
		},
	};
}

function makeExecutionFenceRegistry(
	executeWithKernel: (kernel: TestKernel, code: string, options: TestOptions) => Promise<TestResult>,
) {
	return createKernelSessionRegistry<TestKernel, TestOptions, TestResult, TestSession>({
		languageLabel: "test",
		cancelledErrorClass: TestCancelledError,
		buildSessionKey: (sessionId, cwd, interpreter) => `${sessionId}\0${cwd}\0${interpreter ?? ""}`,
		startKernel: async () => fakeKernel(() => {}),
		createSession: session => session,
		executeWithKernel,
	});
}

describe("kernel session registry lifecycle barriers", () => {
	it("claims a sole-owner session before owner disposal waits for shutdown", async () => {
		const shutdownStarted = Promise.withResolvers<void>();
		const releaseShutdown = Promise.withResolvers<void>();
		let starts = 0;
		let firstShutdownCalls = 0;
		const registry = makeRegistry(async () => {
			starts += 1;
			if (starts === 1) {
				return {
					isAlive: () => true,
					shutdown: async () => {
						firstShutdownCalls += 1;
						shutdownStarted.resolve();
						await releaseShutdown.promise;
						return { confirmed: true };
					},
				};
			}
			return fakeKernel(() => {});
		});

		await registry.executeOnSession("first", "/tmp", {
			sessionId: "session",
			kernelOwnerId: "owner-a",
		});
		const disposal = registry.disposeByOwner("owner-a");
		await shutdownStarted.promise;

		// A surviving owner may start a replacement while the claimed kernel's
		// shutdown is still pending; it must never attach to that old kernel.
		await expect(
			registry.executeOnSession("second", "/tmp", {
				sessionId: "session",
				kernelOwnerId: "owner-b",
			}),
		).resolves.toEqual({ ok: true });
		expect(starts).toBe(2);
		expect(firstShutdownCalls).toBe(1);

		releaseShutdown.resolve();
		await disposal;
		await registry.disposeByOwner("owner-b");
	});

	it("claims queued owner disposal sessions before an earlier owner shutdown settles", async () => {
		const firstShutdownStarted = Promise.withResolvers<void>();
		const secondShutdownStarted = Promise.withResolvers<void>();
		const releaseFirstShutdown = Promise.withResolvers<void>();
		const releaseSecondShutdown = Promise.withResolvers<void>();
		let starts = 0;
		const registry = makeRegistry(async (_cwd, options) => {
			starts += 1;
			const first = options.sessionId === "first";
			return {
				isAlive: () => true,
				shutdown: async () => {
					if (first) {
						firstShutdownStarted.resolve();
						await releaseFirstShutdown.promise;
					} else {
						secondShutdownStarted.resolve();
						await releaseSecondShutdown.promise;
					}
					return { confirmed: true };
				},
			};
		});

		await registry.executeOnSession("first", "/tmp", { sessionId: "first", kernelOwnerId: "owner-a" });
		await registry.executeOnSession("second", "/tmp", { sessionId: "second", kernelOwnerId: "owner-b" });
		const firstDisposal = registry.disposeByOwner("owner-a");
		await firstShutdownStarted.promise;

		// owner-b's disposal is queued behind owner-a's pending shutdown, but its
		// sole-owner session must still be claimed synchronously.
		const secondDisposal = registry.disposeByOwner("owner-b");
		await secondShutdownStarted.promise;
		expect(starts).toBe(2);

		releaseSecondShutdown.resolve();
		releaseFirstShutdown.resolve();
		await Promise.all([firstDisposal, secondDisposal]);
	});

	it("claims an owner session despite an unrelated owner startup", async () => {
		const otherEntered = Promise.withResolvers<void>();
		const releaseOther = Promise.withResolvers<void>();
		const ownerShutdownStarted = Promise.withResolvers<void>();
		const releaseOwnerShutdown = Promise.withResolvers<void>();
		let starts = 0;
		const registry = makeRegistry(async (_cwd, options) => {
			starts += 1;
			if (options.sessionId === "other") {
				otherEntered.resolve();
				await releaseOther.promise;
			}
			return {
				isAlive: () => true,
				shutdown: async () => {
					if (options.sessionId === "main") {
						ownerShutdownStarted.resolve();
						await releaseOwnerShutdown.promise;
					}
					return { confirmed: true };
				},
			};
		});

		await registry.executeOnSession("main", "/tmp", { sessionId: "main", kernelOwnerId: "owner-a" });
		const otherStartup = registry.executeOnSession("other", "/tmp", {
			sessionId: "other",
			kernelOwnerId: "owner-b",
		});
		await otherEntered.promise;

		const disposal = registry.disposeByOwner("owner-a");
		await ownerShutdownStarted.promise;
		expect(starts).toBe(2);
		releaseOwnerShutdown.resolve();
		await disposal;

		releaseOther.resolve();
		await expect(otherStartup).resolves.toEqual({ ok: true });
		await registry.disposeAll();
	});

	it("does not resurrect a second kernel while global disposal awaits startup", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		let starts = 0;
		let shutdowns = 0;
		const registry = makeRegistry(async () => {
			starts += 1;
			entered.resolve();
			await release.promise;
			return fakeKernel(() => {
				shutdowns += 1;
			});
		});

		const first = registry.executeOnSession("first", "/tmp", { sessionId: "session" });
		await entered.promise;
		const disposal = registry.disposeAll();
		await expect(
			registry.executeOnSession("during-disposal", "/tmp", { sessionId: "session" }),
		).rejects.toBeInstanceOf(TestCancelledError);
		expect(starts).toBe(1);

		release.resolve();
		await expect(first).rejects.toBeInstanceOf(TestCancelledError);
		await disposal;
		expect(shutdowns).toBe(1);
	});

	it("shuts down a helper created after its caller aborts during startup", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const controller = new AbortController();
		let shutdowns = 0;
		const registry = makeRegistry(async () => {
			entered.resolve();
			await release.promise;
			return fakeKernel(() => {
				shutdowns += 1;
			});
		});

		const execution = registry.executeOnSession("cancelled", "/tmp", {
			sessionId: "session",
			signal: controller.signal,
		});
		await entered.promise;
		controller.abort();
		release.resolve();
		await expect(execution).rejects.toBeInstanceOf(TestCancelledError);
		expect(shutdowns).toBe(1);
	});

	it("keeps a shared startup alive when the creator aborts but another owner remains", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const creator = new AbortController();
		let shutdowns = 0;
		const registry = makeRegistry(async (_cwd, _options) => {
			entered.resolve();
			await release.promise;
			return fakeKernel(() => {
				shutdowns += 1;
			});
		});
		const first = registry.executeOnSession("creator", "/tmp", {
			sessionId: "session",
			kernelOwnerId: "owner-a",
			signal: creator.signal,
		});
		await entered.promise;
		const second = registry.executeOnSession("survivor", "/tmp", {
			sessionId: "session",
			kernelOwnerId: "owner-b",
		});
		creator.abort();
		release.resolve();
		await expect(first).rejects.toBeInstanceOf(TestCancelledError);
		await expect(second).resolves.toEqual({ ok: true });
		await registry.disposeAll();
		expect(shutdowns).toBe(1);
	});

	it("does not replace a newer kernel after a stale execution fails", async () => {
		type MutableKernel = TestKernel & { alive: boolean; shutdownCalls: number };
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const kernels: MutableKernel[] = [];
		let starts = 0;
		const registry = createKernelSessionRegistry<
			MutableKernel,
			TestOptions,
			TestResult,
			KernelSession<MutableKernel>
		>({
			languageLabel: "test",
			cancelledErrorClass: TestCancelledError,
			buildSessionKey: (sessionId, cwd, interpreter) => `${sessionId}\0${cwd}\0${interpreter ?? ""}`,
			startKernel: async () => {
				starts += 1;
				const kernel = {
					alive: true,
					shutdownCalls: 0,
					isAlive() {
						return this.alive;
					},
					async shutdown() {
						this.shutdownCalls += 1;
						this.alive = false;
						return { confirmed: true };
					},
				} satisfies MutableKernel;
				kernels.push(kernel);
				return kernel;
			},
			createSession: session => session,
			validateKernel: (session, kernel) => session.kernel === kernel,
			executeWithKernel: async (kernel, code) => {
				if (code === "first") {
					entered.resolve();
					await release.promise;
					if (!kernel.isAlive()) throw new Error("stale kernel");
				}
				return { ok: true };
			},
		});

		const first = registry.executeOnSession("first", "/tmp", { sessionId: "session" });
		await entered.promise;
		kernels[0].alive = false;
		await expect(registry.executeOnSession("second", "/tmp", { sessionId: "session" })).resolves.toEqual({
			ok: true,
		});
		release.resolve();
		await expect(first).rejects.toBeInstanceOf(TestCancelledError);
		expect(starts).toBe(2);
		expect(kernels[1].shutdownCalls).toBe(0);
		await registry.disposeAll();
	});

	it("rechecks ownership before shutting down a session after canceled startup cleanup", async () => {
		const otherEntered = Promise.withResolvers<void>();
		const releaseOther = Promise.withResolvers<void>();
		const otherShutdownEntered = Promise.withResolvers<void>();
		const releaseOtherShutdown = Promise.withResolvers<void>();
		let mainKernel!: MutableTestKernel;
		let ownerBExecution: Promise<TestResult> | undefined;
		let starts = 0;
		const registry = createKernelSessionRegistry<
			MutableTestKernel,
			TestOptions,
			TestResult,
			KernelSession<MutableTestKernel>
		>({
			languageLabel: "test",
			cancelledErrorClass: TestCancelledError,
			buildSessionKey: (sessionId, cwd, interpreter) => `${sessionId}\0${cwd}\0${interpreter ?? ""}`,
			createSession: session => session,
			startKernel: async (_cwd, options) => {
				starts += 1;
				const kernel = {
					alive: true,
					shutdownCalls: 0,
					isAlive() {
						return this.alive;
					},
					async shutdown() {
						this.shutdownCalls += 1;
						this.alive = false;
						if (options.sessionId === "other" && !ownerBExecution) {
							ownerBExecution = registry.executeOnSession("survivor", "/tmp", {
								sessionId: "main",
								kernelOwnerId: "owner-b",
							});
							otherShutdownEntered.resolve();
							await releaseOtherShutdown.promise;
						}
						return { confirmed: true };
					},
				} satisfies MutableTestKernel;
				if (options.sessionId === "main") mainKernel = kernel;
				if (options.sessionId === "other") {
					otherEntered.resolve();
					await releaseOther.promise;
				}
				return kernel;
			},
			executeWithKernel: async () => ({ ok: true }),
		});

		await registry.executeOnSession("initial", "/tmp", { sessionId: "main", kernelOwnerId: "owner-a" });
		const otherStartup = registry.executeOnSession("other", "/tmp", {
			sessionId: "other",
			kernelOwnerId: "owner-a",
		});
		await otherEntered.promise;
		const disposal = registry.disposeByOwner("owner-a");
		// Let disposal mark the startup cancelled before releasing its gate.
		await Promise.resolve();
		await Promise.resolve();
		releaseOther.resolve();
		await otherShutdownEntered.promise;
		releaseOtherShutdown.resolve();

		await expect(otherStartup).rejects.toBeInstanceOf(TestCancelledError);
		await disposal;
		await expect(ownerBExecution).resolves.toEqual({ ok: true });
		expect(starts).toBe(2);
		expect(mainKernel.shutdownCalls).toBe(0);
		await registry.disposeAll();
	});

	it("does not overlap a retry with late startup cleanup after caller cancellation", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const shutdownStarted = Promise.withResolvers<void>();
		const releaseShutdown = Promise.withResolvers<void>();
		const controller = new AbortController();
		let starts = 0;
		let secondBeforeShutdown = false;
		const registry = makeRegistry(async () => {
			starts += 1;
			if (starts === 1) {
				entered.resolve();
				await release.promise;
				return {
					isAlive: () => true,
					shutdown: async () => {
						shutdownStarted.resolve();
						await releaseShutdown.promise;
						return { confirmed: true };
					},
				};
			}
			secondBeforeShutdown = true;
			return fakeKernel(() => {});
		});
		const first = registry.executeOnSession("first", "/tmp", { sessionId: "session", signal: controller.signal });
		await entered.promise;
		controller.abort();
		release.resolve();
		await expect(first).rejects.toBeInstanceOf(TestCancelledError);
		const retry = registry.executeOnSession("retry", "/tmp", { sessionId: "session" });
		await shutdownStarted.promise;
		expect(starts).toBe(1);
		expect(secondBeforeShutdown).toBe(false);
		releaseShutdown.resolve();
		await expect(retry).resolves.toEqual({ ok: true });
		expect(starts).toBe(2);
		await registry.disposeAll();
	});

	it("keeps same-owner shared startup alive when one waiter aborts", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const firstController = new AbortController();
		let shutdowns = 0;
		const registry = makeRegistry(async () => {
			entered.resolve();
			await release.promise;
			return fakeKernel(() => {
				shutdowns += 1;
			});
		});
		const first = registry.executeOnSession("first", "/tmp", {
			sessionId: "same",
			kernelOwnerId: "owner",
			signal: firstController.signal,
		});
		await entered.promise;
		const second = registry.executeOnSession("second", "/tmp", {
			sessionId: "same",
			kernelOwnerId: "owner",
		});
		firstController.abort();
		release.resolve();
		await expect(first).rejects.toBeInstanceOf(TestCancelledError);
		await expect(second).resolves.toEqual({ ok: true });
		await registry.disposeAll();
		expect(shutdowns).toBe(1);
	});

	it("fences a result completed after owner disposal", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const registry = makeExecutionFenceRegistry(async () => {
			entered.resolve();
			await release.promise;
			return { ok: true };
		});

		const execution = registry.executeOnSession("in-flight", "/tmp", {
			sessionId: "session",
			kernelOwnerId: "owner-a",
		});
		await entered.promise;
		await registry.disposeByOwner("owner-a");

		// The cell itself may finish after disposal, but its result belongs to a
		// disposed epoch and must not be published to the caller.
		release.resolve();
		await expect(execution).rejects.toBeInstanceOf(TestCancelledError);
	});

	it("fences a result completed after global disposal", async () => {
		const entered = Promise.withResolvers<void>();
		const release = Promise.withResolvers<void>();
		const registry = makeExecutionFenceRegistry(async () => {
			entered.resolve();
			await release.promise;
			return { ok: true };
		});

		const execution = registry.executeOnSession("in-flight", "/tmp", { sessionId: "session" });
		await entered.promise;
		await registry.disposeAll();

		release.resolve();
		await expect(execution).rejects.toBeInstanceOf(TestCancelledError);
	});

	it("does not invoke startup when already cancelled", async () => {
		let starts = 0;
		const registry = makeRegistry(async () => {
			starts += 1;
			return fakeKernel(() => {});
		});
		const controller = new AbortController();
		controller.abort();

		await expect(
			registry.executeOnSession("cancelled", "/tmp", { sessionId: "session", signal: controller.signal }),
		).rejects.toBeInstanceOf(TestCancelledError);
		expect(starts).toBe(0);
	});
});
