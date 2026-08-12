import type { ToolSession } from "../../tools";
import {
	type ExecutorBackend,
	type ExecutorBackendExecOptions,
	type ExecutorBackendResult,
	resolveEvalUrlRoots,
} from "../backend";
import {
	namespaceSessionId as sharedNamespace,
	readInterpreterSetting as sharedReadInterpreterSetting,
	toExecutorBackendResult,
} from "../backend-helpers";
import { executeGo, type GoExecutorOptions } from "./executor";
import { checkGoKernelAvailability, type GoKernelAvailability } from "./kernel";

const GO_SESSION_PREFIX = "go:";

export function namespaceSessionId(sessionId: string): string {
	return sharedNamespace(sessionId, GO_SESSION_PREFIX);
}

function readInterpreterSetting(session: ToolSession): string | undefined {
	return sharedReadInterpreterSetting(session, "go.interpreter");
}

export function getGoKernelAvailability(session: ToolSession, signal?: AbortSignal): Promise<GoKernelAvailability> {
	return checkGoKernelAvailability(session.cwd, readInterpreterSetting(session), { signal });
}

export default {
	id: "go",
	label: "Go/Yaegi",
	highlightLang: "go",

	async isAvailable(session: ToolSession, signal?: AbortSignal): Promise<boolean> {
		return (await getGoKernelAvailability(session, signal)).ok;
	},

	async execute(code: string, opts: ExecutorBackendExecOptions): Promise<ExecutorBackendResult> {
		const executorOptions: GoExecutorOptions = {
			cwd: opts.cwd,
			idleTimeoutMs: opts.idleTimeoutMs,
			signal: opts.signal,
			sessionId: namespaceSessionId(opts.sessionId),
			interpreter: readInterpreterSetting(opts.session),
			sessionFile: opts.sessionFile,
			artifactsDir: opts.session.getArtifactsDir?.() ?? undefined,
			localRoots: resolveEvalUrlRoots(opts.session),
			kernelOwnerId: opts.kernelOwnerId,
			reset: opts.reset,
			onChunk: opts.onChunk,
			onStatus: opts.onStatus,
			toolSession: opts.session,
		};
		const result = await executeGo(code, executorOptions);
		return toExecutorBackendResult(result);
	},
} satisfies ExecutorBackend;
