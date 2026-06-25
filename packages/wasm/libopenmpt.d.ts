// Types for the custom Emscripten libopenmpt build (@trackerstream/wasm).
// Colocated with dist/libopenmpt.js (build.sh copies this file there) so TS uses
// it as the module's types instead of deep-checking the minified runtime. Only
// the surface trackerstream drives is declared; the build exports the full
// openmpt C API plus the listed runtime methods (see packages/wasm/build.sh).

export interface LibOpenMPT {
  HEAP8: Int8Array;
  HEAPU8: Uint8Array;
  HEAP16: Int16Array;
  HEAP32: Int32Array;
  HEAPF32: Float32Array;
  HEAPF64: Float64Array;
  _malloc(size: number): number;
  _free(ptr: number): void;
  UTF8ToString(ptr: number): string;
  stringToUTF8(s: string, ptr: number, max: number): void;
  lengthBytesUTF8(s: string): number;

  _openmpt_get_string(key: number): number;
  _openmpt_module_create_from_memory(
    data: number, size: number, logfunc: number, loguser: number, ctls: number,
  ): number;
  _openmpt_module_destroy(mod: number): void;
  _openmpt_module_read_float_stereo(
    mod: number, samplerate: number, count: number, left: number, right: number,
  ): number;
  _openmpt_module_get_current_order(mod: number): number;
  _openmpt_module_get_current_row(mod: number): number;
  _openmpt_module_get_current_pattern(mod: number): number;
  _openmpt_module_get_current_speed(mod: number): number;
  _openmpt_module_get_current_tempo2(mod: number): number;
  _openmpt_module_get_current_channel_vu_mono(mod: number, ch: number): number;
  _openmpt_module_set_position_seconds(mod: number, s: number): number;
  _openmpt_module_set_position_order_row(mod: number, o: number, r: number): number;
  _openmpt_module_get_position_seconds(mod: number): number;
  _openmpt_module_get_duration_seconds(mod: number): number;
  _openmpt_module_get_num_patterns(mod: number): number;
  _openmpt_module_get_num_orders(mod: number): number;
  _openmpt_module_get_num_samples(mod: number): number;
  // trackerstream streaming fork: patch decoded PCM into an immortal instance.
  _openmpt_module_provide_sample(
    mod: number, sampleIndex: number, data: number, frames: number,
  ): number;
  _openmpt_module_is_sample_pending(mod: number, sampleIndex: number): number;
  // Debug/harness only: decoded ModSample buffer pointer + its byte size.
  _openmpt_module_debug_sample_data(mod: number, sampleIndex: number): number;
  _openmpt_module_debug_sample_bytes(mod: number, sampleIndex: number): number;
  _openmpt_module_debug_sample_frames(mod: number, sampleIndex: number): number;
  _openmpt_module_get_num_channels(mod: number): number;
  _openmpt_module_get_num_instruments(mod: number): number;
  _openmpt_module_get_num_subsongs(mod: number): number;
  _openmpt_module_get_channel_name(mod: number, i: number): number;
  _openmpt_module_get_sample_name(mod: number, i: number): number;
  _openmpt_module_get_instrument_name(mod: number, i: number): number;
  _openmpt_module_get_subsong_name(mod: number, i: number): number;
  _openmpt_module_select_subsong(mod: number, i: number): number;
  _openmpt_module_get_selected_subsong(mod: number): number;
  _openmpt_module_set_repeat_count(mod: number, n: number): number;
  _openmpt_module_set_render_param(mod: number, param: number, value: number): number;
  _openmpt_module_get_metadata(mod: number, key: number): number;
  _openmpt_module_get_metadata_keys(mod: number): number;
  _openmpt_free_string(ptr: number): void;
}

declare const factory: (moduleArg?: Record<string, unknown>) => Promise<LibOpenMPT>;
export default factory;
