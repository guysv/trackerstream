#!/usr/bin/env bash
#
# trackerstream — custom Emscripten build of libopenmpt.
#
# Produces a single-file ES6 module (WASM inlined) targeting an
# AudioWorkletProcessor: off-thread playback, no async fetch inside the worklet.
# Codecs: libopenmpt's bundled minimp3 + stb_vorbis (public-domain, AGPL-clean)
# cover MP3/OGG-in-MO3 and compressed IT/XM samples without LGPL deps. (Set
# PORTS=1 to instead link real mpg123 + libvorbis via emscripten ports.)
#
#   bash build.sh            # download (if needed) + build -> dist/
#   PORTS=1 bash build.sh    # use mpg123 + libvorbis emscripten ports
#
set -euo pipefail
cd "$(dirname "$0")"

VERSION=0.8.7
SRC="libopenmpt-${VERSION}+release"
TARBALL="${SRC%+release}+release.makefile.tar.gz"
URL="https://lib.openmpt.org/files/libopenmpt/src/${TARBALL}"

# Activate emscripten if not already on PATH.
if ! command -v emcc >/dev/null 2>&1; then
  # shellcheck disable=SC1090
  source "${EMSDK_ENV:-$HOME/emsdk/emsdk_env.sh}" >/dev/null 2>&1 || {
    echo "ERROR: emcc not found and \$HOME/emsdk/emsdk_env.sh missing." >&2
    exit 1
  }
fi
echo "emcc: $(emcc -dumpversion 2>/dev/null || emcc -v 2>&1 | head -1)"

mkdir -p build && cd build
if [ ! -d "$SRC" ]; then
  [ -f "$TARBALL" ] || { echo "downloading $TARBALL"; curl -fSL -O "$URL"; }
  tar xzf "$TARBALL"
  # Apply trackerstream's libopenmpt patches (streaming sample-patch API) onto the
  # pristine upstream extraction. Applied once, at extraction time, in order.
  shopt -s nullglob
  for p in ../patches/*.patch; do
    echo "applying patch: $(basename "$p")"
    patch -d "$SRC" -p1 < "$p"
  done
  shopt -u nullglob
fi
cd "$SRC"
mkdir -p bin

# The exact C API surface trackerstream drives (superset of lab/lib.mjs) plus the
# render-quality + subsong controls Phase 0/5 need and the name/VU getters the
# now-playing display (Phase 0/4/5) needs. malloc/free for buffer glue.
EXPORTS='_malloc,_free,\
_openmpt_get_string,\
_openmpt_module_create_from_memory,_openmpt_module_destroy,\
_openmpt_module_read_float_stereo,\
_openmpt_module_get_current_order,_openmpt_module_get_current_row,\
_openmpt_module_get_current_pattern,_openmpt_module_get_current_speed,\
_openmpt_module_get_current_tempo2,\
_openmpt_module_get_current_channel_vu_mono,\
_openmpt_module_get_current_channel_vu_left,\
_openmpt_module_get_current_channel_vu_right,\
_openmpt_module_set_position_seconds,_openmpt_module_set_position_order_row,\
_openmpt_module_get_position_seconds,_openmpt_module_get_duration_seconds,\
_openmpt_module_get_num_patterns,_openmpt_module_get_num_orders,\
_openmpt_module_get_num_samples,_openmpt_module_get_num_channels,\
_openmpt_module_get_num_instruments,_openmpt_module_get_num_subsongs,\
_openmpt_module_get_order_pattern,_openmpt_module_get_pattern_num_rows,\
_openmpt_module_get_channel_name,_openmpt_module_get_sample_name,\
_openmpt_module_get_instrument_name,_openmpt_module_get_subsong_name,\
_openmpt_module_select_subsong,_openmpt_module_get_selected_subsong,\
_openmpt_module_set_repeat_count,\
_openmpt_module_set_render_param,_openmpt_module_get_render_param,\
_openmpt_module_ctl_set,\
_openmpt_module_get_metadata,_openmpt_module_get_metadata_keys,\
_openmpt_free_string,\
_openmpt_module_provide_sample,_openmpt_module_is_sample_pending,\
_openmpt_module_debug_sample_data,_openmpt_module_debug_sample_bytes,\
_openmpt_module_debug_sample_frames,\
_openmpt_module_error_get_last,_openmpt_module_error_clear'

RUNTIME='ccall,cwrap,UTF8ToString,stringToUTF8,lengthBytesUTF8,\
stackAlloc,stackSave,stackRestore,\
getValue,setValue,HEAP8,HEAPU8,HEAP16,HEAP32,HEAPF32,HEAPF64'

# Strip backslash-newlines used for readability above.
EXPORTS=${EXPORTS//\\$'\n'/}
RUNTIME=${RUNTIME//\\$'\n'/}

PORTS_FLAG=0
[ "${PORTS:-0}" = "1" ] && PORTS_FLAG=1

echo "=== building bin/libopenmpt.js (audioworkletprocessor, ports=$PORTS_FLAG) ==="
make -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)" \
  CONFIG=emscripten \
  EMSCRIPTEN_TARGET=audioworkletprocessor \
  EMSCRIPTEN_PORTS="$PORTS_FLAG" \
  $( [ "$PORTS_FLAG" = "1" ] && echo "ALLOW_LGPL=1" ) \
  VERBOSE=1 \
  SO_LDFLAGS="-sEXPORTED_FUNCTIONS=${EXPORTS} -sEXPORTED_RUNTIME_METHODS=${RUNTIME}" \
  bin/libopenmpt.js

cd ../..
mkdir -p dist
cp "build/$SRC/bin/libopenmpt.js" dist/libopenmpt.js
cp libopenmpt.d.ts dist/libopenmpt.d.ts   # colocated types (see libopenmpt.d.ts)
echo "=== built ==="
ls -la dist/
echo "size: $(wc -c < dist/libopenmpt.js) bytes"
