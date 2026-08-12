#!/usr/bin/env bun

import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { $ } from "bun";

const codingAgentDir = path.resolve(import.meta.dir, "..");
const launchCwd = process.cwd();

const TARGETS: Record<string, string> = {
	"darwin-arm64": "darwin-arm64",
	"darwin-x64": "darwin-amd64",
	"linux-arm64": "linux-arm64",
	"linux-x64": "linux-amd64",
	"win32-x64": "windows-amd64",
};

function hostTarget(): string {
	const target = TARGETS[`${process.platform}-${process.arch}`];
	if (target) return target;
	throw new Error(
		`Unsupported host ${process.platform}-${process.arch}; Go eval supports darwin-arm64, darwin-amd64, linux-arm64, linux-amd64, and windows-amd64.`,
	);
}

function helperCacheRoot(): string {
	const configured = Bun.env.OMP_GO_CACHE_DIR?.trim();
	if (configured) return path.resolve(configured);
	if (process.platform === "win32") {
		return path.join(Bun.env.LOCALAPPDATA ?? path.join(os.homedir(), "AppData", "Local"), "omp", "eval-runners");
	}
	return path.join(Bun.env.XDG_CACHE_HOME ?? path.join(os.homedir(), ".cache"), "omp", "eval-runners");
}

function yamlString(value: string): string {
	// YAML single-quoted scalars preserve Windows backslashes and spaces.
	return `'${value.replaceAll("'", "''")}'`;
}

async function main(): Promise<number> {
	const target = hostTarget();
	const outputDir = path.resolve(helperCacheRoot(), target);
	await fs.mkdir(outputDir, { recursive: true });

	const helperPath = path.join(outputDir, `omp-eval-go-runner-${target}${process.platform === "win32" ? ".exe" : ""}`);
	const stampPath = path.join(outputDir, ".source-stamp");
	const sourceFiles = [
		path.join(codingAgentDir, "src", "eval", "go", "runner", "main.go"),
		path.join(codingAgentDir, "src", "eval", "go", "runner", "go.mod"),
		path.join(codingAgentDir, "src", "eval", "go", "runner", "go.sum"),
		path.join(codingAgentDir, "scripts", "build-go-runner.ts"),
	];
	const sourceBytes = (await Promise.all(sourceFiles.map(file => Bun.file(file).arrayBuffer()))).reduce(
		(total, bytes) => total + bytes.byteLength,
		0,
	);
	const sourceDigestInput = new Uint8Array(sourceBytes + target.length);
	let digestOffset = 0;
	for (const file of sourceFiles) {
		const bytes = new Uint8Array(await Bun.file(file).arrayBuffer());
		sourceDigestInput.set(bytes, digestOffset);
		digestOffset += bytes.byteLength;
	}
	sourceDigestInput.set(new TextEncoder().encode(target), digestOffset);
	const digestBuffer = await crypto.subtle.digest("SHA-256", sourceDigestInput);
	const sourceStamp = Array.from(new Uint8Array(digestBuffer), byte => byte.toString(16).padStart(2, "0")).join("");
	const cachedStamp = (
		await Bun.file(stampPath)
			.text()
			.catch(() => "")
	).trim();
	const helperReady = (await Bun.file(helperPath).exists()) && cachedStamp === sourceStamp;

	if (!helperReady) {
		const goVersion = await $`go version`.quiet().nothrow();
		if (goVersion.exitCode !== 0) {
			const detail = goVersion.stderr.toString().trim();
			console.error("Go eval needs a Go compiler to build the external Yaegi helper.");
			if (detail) console.error(detail);
			return goVersion.exitCode || 1;
		}
		const build = await $`${process.execPath} scripts/build-go-runner.ts`
			.cwd(codingAgentDir)
			.env({ ...Bun.env, GO_TARGET: target, GO_OUTPUT_DIR: outputDir })
			.quiet()
			.nothrow();
		if (build.exitCode !== 0) {
			const detail = build.stderr.toString().trim() || build.stdout.toString().trim();
			console.error("Failed to build the external Go/Yaegi helper.");
			if (detail) console.error(detail);
			return build.exitCode || 1;
		}
		if (!(await Bun.file(helperPath).exists())) {
			console.error(`Go helper build completed without producing ${helperPath}`);
			return 1;
		}
		await Bun.write(stampPath, `${sourceStamp}\n`);
	}

	const configDir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-go-eval-config-"));
	const configPath = path.join(configDir, "config.yml");
	await Bun.write(
		configPath,
		[`eval:`, `  go: true`, `go:`, `  interpreter: ${yamlString(helperPath)}`, ""].join("\n"),
	);

	try {
		const existingConfigFiles = Bun.env.PI_CONFIG_FILES;
		const configFiles = existingConfigFiles ? `${existingConfigFiles}${path.delimiter}${configPath}` : configPath;
		const child = Bun.spawn(
			[process.execPath, path.join(codingAgentDir, "src", "cli.ts"), ...process.argv.slice(2)],
			{
				cwd: launchCwd,
				env: { ...Bun.env, PI_GO: "1", PI_CONFIG_FILES: configFiles },
				stdin: "inherit",
				stdout: "inherit",
				stderr: "inherit",
			},
		);
		return await child.exited;
	} finally {
		await fs.rm(configDir, { recursive: true, force: true });
	}
}

try {
	process.exitCode = await main();
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exitCode = 1;
}
