// A minimal FLAC decoder driven directly against libflac.js's raw C-API.
//
// WHY THIS EXISTS: libflacjs ships `lib/decoder.js` as TypeScript-style UMD whose factory takes
// `require` as a PARAMETER. Neither esbuild nor rollup can statically link its inner
// require("./utils/…") calls, so both emit a throwing __require shim and the module explodes at
// runtime in ANY browser bundle. The web client has to decode v4 FLAC sample leaves, so the
// `lib/` wrappers are unusable to us.
//
// The wasm core (`libflac.js`) is plain asm.js and loads in a browser fine. It exposes the whole
// stream-decoder API, and the wrapper we needed is ~40 lines — so we drive the C-API ourselves and
// never import `libflacjs/lib/*` on the client path at all. (The bake still uses lib/encoder.js;
// that only ever runs in Node.)

/** Per-channel planar output, at the stream's native bytes-per-sample. */
export interface DecodedFlac {
  /** One Uint8Array per channel, concatenated across all frames. */
  channels: Uint8Array[];
  channelCount: number;
  bitsPerSample: number;
  totalSamples: number;
}

interface FrameMeta {
  channels: number;
  bitsPerSample: number;
  total_samples: number;
}

/**
 * Decode a complete FLAC stream to planar per-channel bytes.
 *
 * `Flac` is the ready libflac.js module (the caller owns init/ready — see initFlacDecoder).
 * Mirrors what libflacjs's Decoder did: accumulate every write callback's per-channel blocks,
 * then concatenate per channel. Identical output, minus the unbundleable UMD.
 */
export function decodeFlacStream(Flac: any, flacBytes: Uint8Array): DecodedFlac {
  const id = Flac.create_libflac_decoder(false); // verify=false: the CID already authenticates the bytes
  if (!id) throw new Error("flac: create_libflac_decoder failed");

  // Per-frame, per-channel blocks, in decode order.
  const frames: Uint8Array[][] = [];
  let meta: FrameMeta | null = null;
  let readPos = 0;
  let failed = false;

  // libFLAC pulls input through this; hand it successive slices until exhausted.
  const onRead = (bufferSize: number): { buffer?: Uint8Array; readDataLength: number; error?: boolean } => {
    const n = Math.min(bufferSize, flacBytes.length - readPos);
    if (n <= 0) return { buffer: undefined, readDataLength: 0 }; // EOF (not an error)
    const buffer = flacBytes.subarray(readPos, readPos + n);
    readPos += n;
    return { buffer, readDataLength: n };
  };
  const onWrite = (blocks: Uint8Array[]): void => {
    // Copy: libflac reuses its heap views between callbacks.
    frames.push(blocks.map((b) => new Uint8Array(b)));
  };
  const onMeta = (m: FrameMeta): void => {
    if (m) meta = m;
  };
  const onError = (): void => {
    failed = true;
  };

  try {
    // init_decoder_stream returns an INIT STATUS, where 0 == FLAC__STREAM_DECODER_INIT_STATUS_OK.
    // It is not a boolean — truthiness here is inverted from what you'd expect.
    const status = Flac.init_decoder_stream(id, onRead, onWrite, onError, onMeta);
    if (status !== 0) {
      throw new Error(`flac: init_decoder_stream failed (status ${status})`);
    }
    if (!Flac.FLAC__stream_decoder_process_until_end_of_stream(id)) {
      throw new Error("flac: decode failed");
    }
    Flac.FLAC__stream_decoder_finish(id);
    if (failed) throw new Error("flac: decoder reported an error callback");
    if (!meta) throw new Error("flac: stream carried no metadata block");

    const { channels: channelCount, bitsPerSample, total_samples: totalSamples } = meta;
    const channels: Uint8Array[] = [];
    for (let c = 0; c < channelCount; c++) {
      let len = 0;
      for (const f of frames) len += f[c]?.length ?? 0;
      const out = new Uint8Array(len);
      let w = 0;
      for (const f of frames) {
        const b = f[c];
        if (b) {
          out.set(b, w);
          w += b.length;
        }
      }
      channels.push(out);
    }
    return { channels, channelCount, bitsPerSample, totalSamples };
  } finally {
    Flac.FLAC__stream_decoder_delete(id);
  }
}
