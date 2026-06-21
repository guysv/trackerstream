import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { homedir } from 'node:os';

const DIR = join(homedir(), 'tmp', 'somemods');
const u16 = (b, o) => b[o] | (b[o + 1] << 8);
const u32 = (b, o) => (b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)) >>> 0;

for (const name of ['a-windf.it', 'bz_pif.it', 'beyond_the_network.it']) {
  const b = readFileSync(join(DIR, name));
  const magic = String.fromCharCode(b[0], b[1], b[2], b[3]);
  const ordNum = u16(b, 0x20), insNum = u16(b, 0x22), smpNum = u16(b, 0x24), patNum = u16(b, 0x26);
  const flags = u16(b, 0x2c);
  const useInstruments = !!(flags & 0x04);
  const ordBase = 0xc0;
  const insOffBase = ordBase + ordNum;
  const smpOffBase = insOffBase + insNum * 4;
  const patOffBase = smpOffBase + smpNum * 4;
  console.log(`\n=== ${name} (${(b.length/1024|0)}K) magic=${magic} ===`);
  console.log(`orders=${ordNum} instruments=${insNum} samples=${smpNum} patterns=${patNum} useInstruments=${useInstruments}`);
  console.log(`order[0..7]=${[...b.subarray(ordBase, ordBase+8)].join(',')}`);
  let comp = 0, bits16 = 0, totalData = 0;
  const ptrs = [];
  for (let i = 0; i < smpNum; i++) {
    const so = u32(b, smpOffBase + i * 4);
    const imps = String.fromCharCode(b[so], b[so+1], b[so+2], b[so+3]);
    const flg = b[so + 0x12];
    const len = u32(b, so + 0x30);          // frames
    const dptr = u32(b, so + 0x48);
    const is16 = !!(flg & 0x02), stereo = !!(flg & 0x04), compressed = !!(flg & 0x08), has = !!(flg & 0x01);
    if (compressed) comp++; if (is16) bits16++;
    const rawBytes = len * (is16?2:1) * (stereo?2:1);
    if (has && len) totalData += rawBytes;
    ptrs.push({ i, so, imps, dptr, len, is16, stereo, compressed, has, rawBytes });
  }
  console.log(`samples: ${smpNum}  compressed=${comp}  16bit=${bits16}  uncompressed sample-data bytes=${(totalData/1024|0)}K (=${(totalData/b.length*100).toFixed(0)}% of file)`);
  console.log(`first 5 sample ptrs:`, ptrs.slice(0,5).map(p=>`#${p.i}@${p.dptr}(${p.rawBytes}b${p.compressed?' Z':''})`).join(' '));
}
