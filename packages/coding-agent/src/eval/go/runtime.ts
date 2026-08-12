/** Runtime resolution for the optional external Yaegi helper. */
import * as path from "node:path";
import { $which } from "@oh-my-pi/pi-utils";
import { createEnvFilter, resolveExplicitPath, resolveRuntime } from "../runtime-env";

const DEFAULT_ENV_ALLOWLIST = [
	"PATH",
	"HOME",
	"USER",
	"LOGNAME",
	"SHELL",
	"LANG",
	"LC_ALL",
	"LC_CTYPE",
	"LC_MESSAGES",
	"TERM",
	"TERM_PROGRAM",
	"TERM_PROGRAM_VERSION",
	"TMPDIR",
	"TEMP",
	"TMP",
	"XDG_CACHE_HOME",
	"XDG_CONFIG_HOME",
	"XDG_DATA_HOME",
	"XDG_RUNTIME_DIR",
	"SSH_AUTH_SOCK",
	"SSH_AGENT_PID",
	"LD_LIBRARY_PATH",
	"DYLD_LIBRARY_PATH",
];

const WINDOWS_ENV_ALLOWLIST = [
	"APPDATA",
	"COMPUTERNAME",
	"COMSPEC",
	"HOMEDRIVE",
	"HOMEPATH",
	"LOCALAPPDATA",
	"NUMBER_OF_PROCESSORS",
	"OS",
	"PATH",
	"PATHEXT",
	"PROCESSOR_ARCHITECTURE",
	"PROCESSOR_IDENTIFIER",
	"PROGRAMDATA",
	"PROGRAMFILES",
	"PROGRAMFILES(X86)",
	"PROGRAMW6432",
	"SESSIONNAME",
	"SYSTEMDRIVE",
	"SYSTEMROOT",
	"TEMP",
	"TMP",
	"USERDOMAIN",
	"USERPROFILE",
	"USERNAME",
	"WINDIR",
];

const DEFAULT_ENV_DENYLIST = [
	"OPENAI_API_KEY",
	"ANTHROPIC_API_KEY",
	"GOOGLE_API_KEY",
	"GEMINI_API_KEY",
	"OPENROUTER_API_KEY",
	"PERPLEXITY_API_KEY",
	"PERPLEXITY_COOKIES",
	"EXA_API_KEY",
	"AZURE_OPENAI_API_KEY",
	"MISTRAL_API_KEY",
	"PI_TOOL_BRIDGE_URL",
	"PI_TOOL_BRIDGE_TOKEN",
	"PI_TOOL_BRIDGE_SESSION",
];

const DEFAULT_ENV_ALLOW_PREFIXES = ["LC_", "XDG_", "PI_", "GO_", "GOTOOLCHAIN_"];

export interface GoRuntime {
	runnerPath: string;
	env: Record<string, string | undefined>;
}

export const filterEnv = createEnvFilter({
	allowList: DEFAULT_ENV_ALLOWLIST,
	windowsAllowList: WINDOWS_ENV_ALLOWLIST,
	denyList: DEFAULT_ENV_DENYLIST,
	allowPrefixes: DEFAULT_ENV_ALLOW_PREFIXES,
});

export function resolveExplicitGoRuntime(
	interpreter: string,
	cwd: string,
	baseEnv: Record<string, string | undefined>,
): GoRuntime {
	return { runnerPath: resolveExplicitPath(interpreter, cwd), env: { ...baseEnv } };
}

export function enumerateGoRuntimes(
	cwd: string,
	baseEnv: Record<string, string | undefined>,
	interpreter?: string,
): GoRuntime[] {
	if (interpreter) return [resolveExplicitGoRuntime(interpreter, cwd, baseEnv)];
	const candidates = ["omp-eval-go-runner", "omp-yaegi"];
	const runtimes: GoRuntime[] = [];
	const seen = new Set<string>();
	for (const name of candidates) {
		const runnerPath = $which(name);
		if (runnerPath && !seen.has(runnerPath)) {
			seen.add(runnerPath);
			runtimes.push({ runnerPath, env: { ...baseEnv } });
		}
	}
	return runtimes;
}

export function resolveGoRuntime(
	cwd: string,
	baseEnv: Record<string, string | undefined>,
	interpreter?: string,
): GoRuntime {
	const explicit = interpreter ? [resolveExplicitGoRuntime(interpreter, cwd, baseEnv)] : undefined;
	if (explicit) return explicit[0];
	return resolveRuntime(cwd, baseEnv, "omp-eval-go-runner", (runnerPath, env) => ({ runnerPath, env }), interpreter);
}

export function normalizeGoRunnerPath(cwd: string, interpreter: string | undefined): string {
	return interpreter ? path.resolve(resolveExplicitPath(interpreter, cwd)) : "";
}
