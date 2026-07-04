// Shared UI state the persistent shell (+layout) renders but routes/components write.
// Navigation lives in the URL (routes); playback lives in the player store; THIS holds the
// third axis — transient "inspection" chrome: which right panel is up, which track the
// right detail pane inspects, the help overlay, the search-box element (for "/"-focus), and
// the header status line the active route publishes. Module-level runes survive client
// navigation, so this is the seam between the layout chrome and the route views.
export const ui = $state<{
  right: "detail" | "queue" | "peers";
  inspectorTrackId: number | null;
  showHelp: boolean;
  searchEl: HTMLInputElement | undefined;
  /** Header status text, set by the active route (result count / loading / offline). */
  status: string;
}>({
  right: "detail",
  inspectorTrackId: null,
  showHelp: false,
  searchEl: undefined,
  status: "",
});
