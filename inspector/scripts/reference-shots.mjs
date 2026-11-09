// Screenshots both plates of design/nebuladb-design-reference.html (dark + light, desktop + mobile).
import { chromium } from 'playwright'
import fs from 'node:fs'
const OUT = 'screenshots/reference'
fs.mkdirSync(OUT, { recursive: true })
const b = await chromium.launch({ executablePath: process.env.CHROME || undefined, args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] })
const file = 'file://' + process.cwd().replace(/inspector$/, '') + 'design/nebuladb-design-reference.html'
for (const [vw, vh, tag] of [[1440, 900, 'desktop'], [390, 844, 'mobile']]) {
  const ctx = await b.newContext({ viewport: { width: vw, height: vh } })
  const p = await ctx.newPage()
  await p.goto(file, { waitUntil: 'load' }).catch(() => {})
  await p.waitForTimeout(9000) // let the simulation produce flushes and compactions
  for (const theme of ['dark', 'light']) {
    await p.evaluate((t) => { document.documentElement.dataset.theme = t }, theme)
    await p.waitForTimeout(600)
    const frames = p.locator('.frame')
    await frames.nth(0).screenshot({ path: `${OUT}/plate1-inspector-${tag}-${theme}.png` })
    await frames.nth(1).screenshot({ path: `${OUT}/plate2-about-${tag}-${theme}.png` })
  }
  await ctx.close()
}
await b.close()
console.log(fs.readdirSync(OUT).join('\n'))
