import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { $ } from "bun";

const runnerDir = path.resolve(import.meta.dir, "../../src/eval/go/runner");
const runnerPath = path.join(os.tmpdir(), `omp-eval-go-runner-test-${process.pid}`);

async function buildRunner(): Promise<void> {
	const result = await $`go build -trimpath -buildvcs=false -o ${runnerPath} .`.cwd(runnerDir).quiet().nothrow();
	if (result.exitCode !== 0) throw new Error(result.stderr.toString());
}

describe("Yaegi Go eval runner", () => {
	afterAll(async () => {
		await fs.rm(runnerPath, { force: true });
	});

	it(
		"reports its version and completes the startup-ready handshake",
		async () => {
			await buildRunner();
			const versionProc = Bun.spawn([runnerPath, "--version"], { stdout: "pipe", stderr: "pipe" });
			expect(await new Response(versionProc.stdout).text()).toContain("omp-eval-go-runner");
			expect(await versionProc.exited).toBe(0);

			const proc = Bun.spawn([runnerPath], {
				stdin: "pipe",
				stdout: "pipe",
				stderr: "pipe",
				env: {
					...process.env,
					PI_TOOL_BRIDGE_URL: "http://127.0.0.1:1",
					PI_TOOL_BRIDGE_TOKEN: "test-token",
					PI_TOOL_BRIDGE_SESSION: "test-session",
				},
			});
			proc.stdin.write('{"type":"ready"}\n');
			await proc.stdin.flush();
			const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
			const first = await reader.read();
			expect(new TextDecoder().decode(first.value)).toContain('"type":"ready"');
			proc.stdin.write('{"type":"exit"}\n');
			await proc.stdin.flush();
			expect(await proc.exited).toBe(0);
		},
		{ timeout: 30_000 },
	);

	it(
		"persists variables and calls the host bridge through omp",
		async () => {
			await buildRunner();
			const requests: Array<Record<string, unknown>> = [];
			const server = Bun.serve({
				hostname: "127.0.0.1",
				port: 0,
				async fetch(req) {
					if (new URL(req.url).pathname !== "/v1/eval-tool" || req.headers.has("authorization"))
						return Response.json({ ok: false, error: "forbidden" }, { status: 403 });
					const body = (await req.json()) as Record<string, unknown>;
					requests.push(body);
					const name = body.name;
					if (name === "__agent__")
						return Response.json({ ok: true, value: { text: "agent-value", details: { id: "a1" } } });
					if (name === "hub")
						return Response.json({ ok: true, value: { text: "hub-value", details: { op: "list" } } });
					return Response.json({ ok: true, value: { text: "host-value" } });
				},
			});
			const proc = Bun.spawn([runnerPath], {
				stdin: "pipe",
				stdout: "pipe",
				stderr: "pipe",
				env: {
					...process.env,
					PI_TOOL_BRIDGE_URL: `http://${server.hostname}:${server.port}`,
					PI_TOOL_BRIDGE_TOKEN: "test-token",
					PI_TOOL_BRIDGE_SESSION: "test-session",
				},
			});
			const writer = proc.stdin;
			const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
			const decoder = new TextDecoder();
			let buffer = "";
			const nextDone = async (): Promise<Record<string, unknown>[]> => {
				const frames: Record<string, unknown>[] = [];
				while (true) {
					while (!buffer.includes("\n")) {
						const chunk = await reader.read();
						if (chunk.done) throw new Error("runner exited before done frame");
						buffer += decoder.decode(chunk.value, { stream: true });
					}
					const newline = buffer.indexOf("\n");
					const line = buffer.slice(0, newline);
					buffer = buffer.slice(newline + 1);
					const frame = JSON.parse(line) as Record<string, unknown>;
					frames.push(frame);
					if (frame.type === "done") return frames;
				}
			};
			const send = async (id: string, code: string, identity = true) => {
				writer.write(
					`${JSON.stringify({
						id,
						code,
						cwd: process.cwd(),
						bridgeUrl: identity
							? `http://${server.hostname}:${server.port}/v1/eval-tool`
							: "http://127.0.0.1:1/path",
						bridgeSession: "test-session",
					})}\n`,
				);
				await writer.flush();
				return await nextDone();
			};
			await send("one", 'import "fmt"');
			const mixedFrames = await send("mixed", 'var mixed = 41\nfmt.Println("mixed", mixed)');
			expect(mixedFrames.some(frame => frame.type === "stdout" && frame.data === "mixed 41\n")).toBe(true);
			const mixedReuseFrames = await send("mixed-reuse", 'mixed++\nfmt.Println("mixed-reuse", mixed)');
			expect(mixedReuseFrames.some(frame => frame.type === "stdout" && frame.data === "mixed-reuse 42\n")).toBe(
				true,
			);
			for (const [id, code, text] of [
				["paren-import", 'import("fmt"); fmt.Println("paren")', "paren"],
				["comment-alias", 'import q /* comment */ "fmt"; q.Println("alias")', "alias"],
				["group-comment-alias", 'import ( q /* comment */ "fmt" ); q.Println("group")', "group"],
			] as const) {
				const importFrames = await send(id, code);
				expect(importFrames.some(frame => frame.type === "stdout" && frame.data === `${text}\n`)).toBe(true);
			}
			await send("two", "var value = 40");
			const frames = await send("three", 'r, e := omp.Tool("echo", nil); fmt.Println(value+2, r, e)');
			const stdout = frames.find(frame => frame.type === "stdout");
			expect(stdout?.data).toContain("42");
			expect(stdout?.data).toContain("host-value");
			expect(requests[0]?.name).toBe("echo");
			expect((requests[0]?.args as Record<string, unknown>)?.i).toBe("go prelude");
			writer.write(
				`${JSON.stringify({
					id: "bad-auth",
					code: 'r, e := omp.Tool("echo", nil); fmt.Println(r, e)',
					cwd: process.cwd(),
					bridgeUrl: `http://${server.hostname}:${server.port}/v1/eval-tool`,
					bridgeToken: "wrong-token",
					bridgeSession: "test-session",
				})}\n`,
			);
			await writer.flush();
			const injectedFrames = await nextDone();
			const injectedStdout = injectedFrames.find(frame => frame.type === "stdout");
			expect(injectedStdout?.data).toContain("host-value");

			await send("retain", 'func retained() { r, e := omp.Tool("echo", nil); fmt.Println(r, e) }');
			const staleFrames = await send("stale", "retained()");
			expect(staleFrames.some(frame => frame.type === "stdout" || frame.type === "stderr")).toBe(false);
			const invalidUrlFrames = await send("invalid-url", 'r, e := omp.Tool("echo", nil); fmt.Println(r, e)', false);
			const invalidUrlStdout = invalidUrlFrames.find(frame => frame.type === "stdout");
			expect(invalidUrlStdout?.data).toContain("bridge is unavailable");

			const facadeFrames = await send(
				"four",
				'a, ae := omp.Agent("do work"); h, he := omp.Hub("send", map[string]interface{}{"to": "a1", "message": "ping", "await": true}); s, se := omp.AgentWith("inline", map[string]interface{}{"async": false}); l, le := omp.Hub("list", nil); fmt.Println(a, ae, h, he, s, se, l, le)',
			);
			const facadeStdout = facadeFrames.find(frame => frame.type === "stdout");
			expect(facadeStdout?.data).toContain("agent-value");
			expect(facadeStdout?.data).toContain("hub-value");
			expect(requests.map(request => request.name)).toEqual([
				"echo",
				"echo",
				"__agent__",
				"hub",
				"__agent__",
				"hub",
			]);
			expect((requests[2]?.args as Record<string, unknown>)?.prompt).toBe("do work");
			expect((requests[2]?.args as Record<string, unknown>)?.i).toBe("go prelude");
			expect((requests[2]?.args as Record<string, unknown>)?.async).toBe(true);
			expect((requests[3]?.args as Record<string, unknown>)?.op).toBe("send");
			expect((requests[3]?.args as Record<string, unknown>)?.to).toBe("a1");
			expect((requests[3]?.args as Record<string, unknown>)?.message).toBe("ping");
			expect((requests[3]?.args as Record<string, unknown>)?.await).toBe(true);
			expect((requests[4]?.args as Record<string, unknown>)?.prompt).toBe("inline");
			expect((requests[4]?.args as Record<string, unknown>)?.async).toBe(false);
			expect((requests[5]?.args as Record<string, unknown>)?.op).toBe("list");
			expect((requests[5]?.args as Record<string, unknown>)?.i).toBe("go prelude");
			writer.write('{"type":"exit"}\n');
			await writer.flush();
			await proc.exited;
			await server.stop(true);
		},
		{ timeout: 30_000 },
	);

	it(
		"normalizes documented cross-targets and builds without cgo",
		async () => {
			const output = path.join(os.tmpdir(), `omp-eval-go-runner-targets-${process.pid}`);
			await fs.rm(output, { recursive: true, force: true });
			try {
				for (const target of ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64", "windows-x64"] as const) {
					const result = await $`bun scripts/build-go-runner.ts`
						.cwd(path.resolve(import.meta.dir, "../.."))
						.env({ ...process.env, GO_TARGET: target, GO_OUTPUT_DIR: output })
						.quiet()
						.nothrow();
					expect(result.exitCode).toBe(0);
					expect(result.stdout.toString()).toContain(`omp-eval-go-runner-${target.replace("x64", "amd64")}`);
				}
			} finally {
				await fs.rm(output, { recursive: true, force: true });
			}
		},
		{ timeout: 60_000 },
	);

	it(
		"blocks filesystem, network, and goroutine escape hatches",
		async () => {
			await buildRunner();
			const proc = Bun.spawn([runnerPath], {
				stdin: "pipe",
				stdout: "pipe",
				stderr: "pipe",
				env: {
					...process.env,
					PI_TOOL_BRIDGE_URL: "http://127.0.0.1:1",
					PI_TOOL_BRIDGE_TOKEN: "test-token",
					PI_TOOL_BRIDGE_SESSION: "test-session",
				},
			});
			const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
			const decoder = new TextDecoder();
			let buffer = "";
			const send = async (id: string, code: string): Promise<Record<string, unknown>[]> => {
				proc.stdin.write(`${JSON.stringify({ id, code })}\n`);
				await proc.stdin.flush();
				const frames: Record<string, unknown>[] = [];
				while (true) {
					while (!buffer.includes("\n")) {
						const chunk = await reader.read();
						if (chunk.done) throw new Error("runner exited before done frame");
						buffer += decoder.decode(chunk.value, { stream: true });
					}
					const newline = buffer.indexOf("\n");
					const frame = JSON.parse(buffer.slice(0, newline)) as Record<string, unknown>;
					buffer = buffer.slice(newline + 1);
					frames.push(frame);
					if (frame.type === "done") return frames;
				}
			};
			try {
				const osFrames = await send("os", 'import "os"');
				expect(osFrames.some(frame => String(frame.evalue).includes("unable to find source"))).toBe(true);
				const filepathFrames = await send("filepath", 'import "path/filepath"');
				expect(filepathFrames.some(frame => String(frame.evalue).includes("unable to find source"))).toBe(true);
				const relativeFrames = await send("relative", 'import "./relative"');
				expect(relativeFrames.some(frame => String(frame.evalue).includes("file does not exist"))).toBe(true);
				const goFrames = await send("go", "go func() {}()");
				expect(goFrames.some(frame => String(frame.evalue).includes("cannot start goroutines"))).toBe(true);
				await send("fmt-import", 'import "fmt"');
				const scanFrames = await send(
					"scan",
					"func init(){ var value string; n, err := fmt.Scan(&value); fmt.Println(n, err) }",
				);
				expect(scanFrames.some(frame => frame.type === "stdout" && frame.data === "0 EOF\n")).toBe(true);
				const fmtFrames = await send("fmt", 'fmt.Println("framed")');
				expect(fmtFrames.some(frame => frame.type === "stdout" && frame.data === "framed\n")).toBe(true);
				const rawFrames = await send("raw", 'fmt.Print("raw")');
				expect(rawFrames.some(frame => frame.type === "stdout" && frame.data === "raw")).toBe(true);
			} finally {
				proc.stdin.write('{"type":"exit"}\n');
				await proc.stdin.flush();
				await proc.exited;
			}
		},
		{ timeout: 30_000 },
	);

	it(
		"starts without inherited bridge credentials and requires per-request bridge identity",
		async () => {
			await buildRunner();
			const proc = Bun.spawn([runnerPath], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
			const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
			proc.stdin.write('{"type":"ready"}\n');
			await proc.stdin.flush();
			const first = await reader.read();
			expect(new TextDecoder().decode(first.value)).toContain('"type":"ready"');
			proc.stdin.write(
				`${JSON.stringify({ id: "missing-bridge", code: 'import "fmt"\nimport "omp"\nv, err := omp.Tool("read", map[string]interface{}{})\nfmt.Println(v, err)' })}\n`,
			);
			await proc.stdin.flush();
			const chunks: string[] = [];
			while (true) {
				const chunk = await reader.read();
				if (chunk.done) break;
				const text = new TextDecoder().decode(chunk.value);
				chunks.push(text);
				if (text.includes('"type":"done"')) break;
			}
			expect(chunks.join("")).toContain("OMP tool bridge is unavailable");
			proc.stdin.write('{"type":"exit"}\n');
			await proc.stdin.flush();
			expect(await proc.exited).toBe(0);
		},
		{ timeout: 30_000 },
	);
});
