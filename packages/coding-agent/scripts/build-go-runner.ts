#!/usr/bin/env bun

import * as fs from "node:fs/promises";
import * as path from "node:path";

const packageDir = path.join(import.meta.dir, "..");
const runnerDir = path.join(packageDir, "src", "eval", "go", "runner");
const outputDir = path.resolve(packageDir, Bun.env.GO_OUTPUT_DIR ?? path.join("dist", "eval-runners"));

const rawTarget = Bun.env.GO_TARGET ?? `${process.platform}-${process.arch}`;
const targetParts = rawTarget.toLowerCase().split("-");
if (targetParts.length !== 2 || !targetParts[0] || !targetParts[1]) {
	throw new Error(
		`Invalid GO_TARGET ${JSON.stringify(rawTarget)}; use <goos>-<goarch>, e.g. linux-amd64 or darwin-arm64`,
	);
}
const goosAliases: Record<string, string> = { win32: "windows", win: "windows", mac: "darwin", osx: "darwin" };
const goarchAliases: Record<string, string> = {
	x64: "amd64",
	amd64: "amd64",
	arm64: "arm64",
	aarch64: "arm64",
	ia32: "386",
	x86: "386",
	"386": "386",
};
const goos = goosAliases[targetParts[0]] ?? targetParts[0];
const goarch = goarchAliases[targetParts[1]];
const supported = new Set(["darwin/arm64", "darwin/amd64", "linux/arm64", "linux/amd64", "windows/amd64"]);
if (!goarch || !supported.has(`${goos}/${goarch}`)) {
	throw new Error(
		`Unsupported GO_TARGET ${JSON.stringify(rawTarget)}; supported targets: darwin-arm64, darwin-amd64, linux-arm64, linux-amd64, windows-amd64`,
	);
}
const normalizedTarget = `${goos}-${goarch}`;
const outfile = path.join(outputDir, `omp-eval-go-runner-${normalizedTarget}${goos === "windows" ? ".exe" : ""}`);

await fs.mkdir(outputDir, { recursive: true });
const proc = Bun.spawn(["go", "build", "-trimpath", "-buildvcs=false", "-o", outfile, "."], {
	cwd: runnerDir,
	env: { ...Bun.env, CGO_ENABLED: "0", GOOS: goos, GOARCH: goarch },
	stdout: "inherit",
	stderr: "inherit",
});
if ((await proc.exited) !== 0) throw new Error(`Go helper build failed for ${normalizedTarget}`);
console.log(outfile);
