/** Compatibility exports for the historical Python bridge path. */

export type { ToolBridgeEntry as PyToolBridgeEntry, ToolBridgeInfo as PyToolBridgeInfo } from "../tool-bridge";
export {
	disposeToolBridge as disposePyToolBridge,
	disposeToolBridgeByOwner as disposePyToolBridgeByOwner,
	ensureToolBridge as ensurePyToolBridge,
	registerToolBridge as registerPyToolBridge,
} from "../tool-bridge";
