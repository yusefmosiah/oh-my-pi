export * from "./backend";
export { default as goBackend, getGoKernelAvailability } from "./go";
export { default as juliaBackend } from "./jl";
export { default as jsBackend } from "./js";
export { default as pythonBackend } from "./py";
export { default as rubyBackend } from "./rb";
export * from "./types";

/** Dispose every retained eval runtime and the shared authenticated host bridge. */
export async function disposeAllEvalResources(): Promise<void> {
	const { disposeAllKernelSessions } = await import("./py/executor");
	const { disposeAllRubyKernelSessions } = await import("./rb/executor");
	const { disposeAllJuliaKernelSessions } = await import("./jl/executor");
	const { disposeAllGoKernelSessions } = await import("./go/executor");
	const { disposeAllVmContexts } = await import("./js/context-manager");
	const { disposeToolBridge } = await import("./tool-bridge");
	await Promise.allSettled([
		disposeAllKernelSessions(),
		disposeAllRubyKernelSessions(),
		disposeAllJuliaKernelSessions(),
		disposeAllGoKernelSessions(),
		disposeAllVmContexts(),
	]);
	await disposeToolBridge();
}
