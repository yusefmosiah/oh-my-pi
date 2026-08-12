/** Compatibility exports for the historical JS bridge path.
 * The dispatch and loopback server now live in the language-neutral eval bridge.
 */

export type { ToolBridgeOptions, ToolValue } from "../tool-bridge";
export { callSessionTool } from "../tool-bridge";
export type { JsStatusEvent } from "./shared/types";
