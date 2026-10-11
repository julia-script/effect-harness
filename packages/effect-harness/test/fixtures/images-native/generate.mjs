import sharp from 'sharp'
import { writeFileSync } from 'node:fs'
const path = new URL('./', import.meta.url)
const save = (name, bytes) => writeFileSync(new URL(name, path), bytes)
const pixels = Buffer.from([
  255, 0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 255, 255, 0, 255, 255, 0, 255, 255, 0, 255, 255,
  255,
])
const image = () => sharp(pixels, { raw: { width: 3, height: 2, channels: 4 } })
const png = await image().png().toBuffer()
save('opaque.png', png)
save('opaque.jpg', await image().jpeg({ quality: 95 }).toBuffer())
save('opaque.webp', await image().webp({ lossless: true }).toBuffer())
const crc = (bytes) => {
  let c = 0xffffffff
  for (const b of bytes) {
    c ^= b
    for (let i = 0; i < 8; i++) c = (c >>> 1) ^ (c & 1 ? 0xedb88320 : 0)
  }
  return (c ^ 0xffffffff) >>> 0
}
for (let n = 1; n <= 8; n++) {
  const tiff = Buffer.from([
    73,
    73,
    42,
    0,
    8,
    0,
    0,
    0,
    1,
    0,
    18,
    1,
    3,
    0,
    1,
    0,
    0,
    0,
    n,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
  ])
  const chunk = Buffer.alloc(tiff.length + 12)
  chunk.writeUInt32BE(tiff.length)
  chunk.write('eXIf', 4)
  tiff.copy(chunk, 8)
  chunk.writeUInt32BE(crc(chunk.subarray(4, -4)), chunk.length - 4)
  save(`orientation-${n}.png`, Buffer.concat([png.subarray(0, 33), chunk, png.subarray(33)]))
  save(
    `orientation-${n}.jpg`,
    await image().withMetadata({ orientation: n }).jpeg({ quality: 95 }).toBuffer(),
  )
}
const bmp = Buffer.alloc(54 + 24)
bmp.write('BM')
bmp.writeUInt32LE(bmp.length, 2)
bmp.writeUInt32LE(54, 10)
bmp.writeUInt32LE(40, 14)
bmp.writeInt32LE(3, 18)
bmp.writeInt32LE(2, 22)
bmp.writeUInt16LE(1, 26)
bmp.writeUInt16LE(24, 28)
bmp.writeUInt32LE(24, 34)
for (let y = 0; y < 2; y++)
  for (let x = 0; x < 3; x++) {
    const source = ((1 - y) * 3 + x) * 4,
      target = 54 + y * 12 + x * 3
    bmp[target] = pixels[source + 2]
    bmp[target + 1] = pixels[source + 1]
    bmp[target + 2] = pixels[source]
  }
save('opaque.bmp', bmp)
const frame = Buffer.from(pixels)
for (let i = 0; i < frame.length; i += 4) {
  frame[i] = 150
  frame[i + 1] = 50
  frame[i + 2] = 100
}
const animation = Buffer.concat([pixels, frame])
for (const format of ['gif', 'webp'])
  save(
    `animated.${format}`,
    await sharp(animation, { raw: { width: 3, height: 4, channels: 4, pageHeight: 2 } })
      .toFormat(format, { loop: 0, delay: [100, 100], lossless: true })
      .toBuffer(),
  )
pixels[3] = 0
for (const format of ['png', 'webp', 'gif'])
  save(`alpha.${format}`, await image().toFormat(format, { lossless: true }).toBuffer())
for (let i = 3; i < pixels.length; i += 4) pixels[i] = 0
save('transparent.png', await image().png().toBuffer())
