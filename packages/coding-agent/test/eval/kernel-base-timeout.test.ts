import { describe, expect, it, vi } from "bun:test";
import { GoKernel } from "../../src/eval/go/kernel";
import { BaseKernel, type KernelExecuteOptions } from "../../src/eval/kernel-base";

type FakeFrame = Record<string, unknown>;

type FakeProcess = {
	stdin: {
		write(data: string): void;
		flush(): Promise<void>;
		end(): void;
	};
	stdout: ReadableStream<Uint8Array>;
	stderr: ReadableStream<Uint8Array>;
	exited: Promise<number>;
	exitCode: number | null;
	kill(signal?: string): void;
	emit(frame: FakeFrame): void;
	close(): void;
	writes: string[];
	kills: string[];
};

function fakeProcess(onWrite?: (line: string, process: FakeProcess) => void): FakeProcess {
	let stdoutController: ReadableStreamDefaultController<Uint8Array> | undefined;
	const stdout = new ReadableStream<Uint8Array>({
		start(controller) {
			stdoutController = controller;
		},
	});
	let resolveExited!: (code: number) => void;
	const exited = new Promise<number>(resolve => {
		resolveExited = resolve;
	});
	const stderr = new ReadableStream<Uint8Array>({
		start(controller) {
			controller.close();
		},
	});
	const process = {
		stdin: {
			write(data: string) {
				process.writes.push(data);
				onWrite?.(data.trim(), process);
			},
			async flush() {},
			end() {},
		},
		stdout,
		stderr,
		exited,
		exitCode: null,
		kills: [] as string[],
		writes: [] as string[],
		kill(signal = "SIGTERM") {
			process.kills.push(signal);
		},
		emit(frame: FakeFrame) {
			stdoutController?.enqueue(new TextEncoder().encode(`${JSON.stringify(frame)}\n`));
		},
		close() {
			stdoutController?.close();
			resolveExited(0);
		},
	};
	return process;
}

class TestKernel extends BaseKernel<KernelExecuteOptions> {
	constructor() {
		super("test", {
			languageName: "test",
			traceIpc: false,
			exitPayload: JSON.stringify({ type: "exit" }),
			interruptEscalationMs: 50,
			shutdownGraceMs: 20,
			buildPayload: (code, id) => JSON.stringify({ id, code }),
		});
	}

	attach(process: FakeProcess): void {
		this.setProcess(process as never);
	}
}

const makeGoKernel = (): GoKernel => {
	const Constructor = GoKernel as unknown as new (id: string) => GoKernel;
	return new Constructor("go-test");
};

describe("kernel cancellation write fences", () => {
	it("retries SIGINT after a cancellation from the write-start hook", async () => {
		const events: string[] = [];
		let emitDone!: () => void;
		const process = fakeProcess(line => {
			if (JSON.parse(line).id) events.push("write");
		});
		const originalKill = process.kill;
		process.kill = signal => {
			events.push(`kill:${signal}`);
			originalKill.call(process, signal);
		};
		const kernel = new TestKernel();
		kernel.attach(process);
		const controller = new AbortController();
		const resultPromise = kernel.execute("cell", {
			id: "write-race",
			signal: controller.signal,
			onRequestWriteStarted: () => {
				events.push("write-started");
				controller.abort();
			},
			onRequestWritten: () => {
				events.push("write-committed");
				emitDone();
			},
		});
		emitDone = () => {
			process.emit({ type: "done", id: "write-race", status: "error", cancelled: true });
		};
		const result = await resultPromise;
		expect(result.cancelled).toBe(true);
		expect(events).toEqual(["write-started", "write", "write-committed", "kill:SIGINT"]);
		expect(process.kills).toContain("SIGINT");
		process.close();
	});

	it("holds the Go serialization tail until poisoned-runner shutdown settles", async () => {
		let requestCount = 0;
		const process = fakeProcess((line, proc) => {
			const request = JSON.parse(line) as FakeFrame;
			if (request.type === "ready") {
				proc.emit({ type: "ready" });
				return;
			}
			if (request.id) {
				requestCount++;
				proc.emit({
					type: "done",
					id: request.id,
					status: requestCount === 1 ? "error" : "ok",
					cancelled: requestCount === 1,
				});
			}
		});
		const kernel = makeGoKernel();
		kernel.setProcess(process as never);
		await kernel.waitUntilReady();

		const releaseShutdown = Promise.withResolvers<{ confirmed: boolean }>();
		const cleanupStarted = Promise.withResolvers<void>();
		const kernelForTest = kernel as unknown as {
			waitForProcessExit: () => Promise<boolean>;
			shutdown: () => Promise<{ confirmed: boolean }>;
		};
		kernelForTest.waitForProcessExit = vi.fn(async () => true);
		kernelForTest.shutdown = vi.fn(async () => {
			cleanupStarted.resolve();
			return await releaseShutdown.promise;
		});

		const controller = new AbortController();
		const first = kernel.execute("poison", {
			signal: controller.signal,
			onRequestWriteStarted: () => controller.abort(),
		});
		const firstResult = await first;
		expect(firstResult.cancelled).toBe(true);
		await cleanupStarted.promise;

		const second = kernel.execute("after-poison");
		await Promise.resolve();
		expect(requestCount).toBe(1);
		releaseShutdown.resolve({ confirmed: true });
		await second;
		expect(requestCount).toBe(2);
		expect(kernelForTest.shutdown).toHaveBeenCalledTimes(1);
		process.close();
	});
});
