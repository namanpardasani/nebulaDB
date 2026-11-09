// Phase 12 screenshots of the Strata design: light and dark, desktop / tablet /
// mobile, every panel, the armed kill switch, the crash overlay stages, and the
// "writers stalled" state (captured by flooding a server with small limits).
// Same environment as screenshots.mjs (see run-screenshots.sh). OUT defaults to
// screenshots/after.
import { chromium } from 'playwright'
import fs from 'node:fs'

const ADMIN = process.env.ADMIN ?? 'http://127.0.0.1:7391'
const UI = process.env.UI ?? 'http://127.0.0.1:4173'
const OUT = process.env.OUT ?? 'screenshots/after'
fs.mkdirSync(OUT, { recursive: true })
const browser = await chromium.launch({ executablePath: process.env.CHROME || undefined, args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] })
const url = (hash = '') => `${UI}/?admin=${encodeURIComponent(ADMIN)}${hash}`
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const post = (path, body) => fetch(ADMIN + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
const shots = []

async function mset(n = 500) {
  const args = ['MSET']
  for (let i = 0; i < n; i++) args.push(`user:${Math.floor(Math.random() * 400000).toString().padStart(6, '0')}`, 'v'.repeat(60 + Math.floor(Math.random() * 400)))
  await post('/api/cmd', { args }).catch(() => {})
}
async function load(ms, workers = 4, n = 25) {
  const stop = Date.now() + ms
  await Promise.all(Array.from({ length: workers }, async () => { while (Date.now() < stop) await mset(n) }))
}
async function shot(page, name, full = true) {
  const f = `${OUT}/${name}.png`
  await page.screenshot({ path: f, fullPage: full })
  shots.push(f); console.log('  ', f)
}
async function panel(page, id, name) {
  const f = `${OUT}/${name}.png`
  await page.locator('section.panel', { has: page.locator(`#${id}`) }).first().screenshot({ path: f })
  shots.push(f); console.log('  ', f)
}
const waitLive = (page) => page.getByText(/^connected/).first().waitFor({ timeout: 20000 })

for (const scheme of ['dark', 'light']) {
  console.log(`== ${scheme}`)
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: scheme })
  const page = await ctx.newPage()
  await page.goto(url()); await waitLive(page); await sleep(1200)
  if (scheme === 'dark') await shot(page, `01-inspector-empty-${scheme}`)

  // under load
  const bg = load(9000)
  await sleep(4000)
  await shot(page, `02-inspector-active-${scheme}`)
  await sleep(1200)
  await shot(page, `03-inspector-active-2-${scheme}`, false)
  await bg
  await sleep(1500)
  for (const [id, n] of [['h-writes', 'writes'], ['h-mem', 'memtable'], ['h-lsm', 'strata'], ['h-bp', 'backpressure'], ['h-act', 'activity'], ['h-bc', 'filters'], ['h-try', 'tryit'], ['h-ctl', 'controls']]) await panel(page, id, `component-${n}-${scheme}`)
  await page.locator('.stats').screenshot({ path: `${OUT}/component-stats-${scheme}.png` })
  await page.locator('header.top').screenshot({ path: `${OUT}/component-header-${scheme}.png` })

  // stalled: flood until the page says so
  console.log('  flooding for the stalled state…')
  const flood = load(40000, 8, 500)
  let stalled = false
  for (let i = 0; i < 80 && !stalled; i++) { stalled = await page.getByText('writers stalled').first().isVisible().catch(() => false); if (!stalled) await sleep(500) }
  console.log('  stalled seen:', stalled)
  if (stalled) { await shot(page, `04-inspector-stalled-${scheme}`, false); await panel(page, 'h-bp', `component-backpressure-stalled-${scheme}`) }
  else { await shot(page, `04-inspector-flood-not-stalled-${scheme}`, false); await panel(page, 'h-bp', `component-backpressure-flood-${scheme}`) }
  // let the flood finish and the engine drain before the next steps
  await flood; await sleep(2500)

  // try it
  const input = page.getByLabel('Command')
  for (const c of ['SET demo:1 "hello nebula"', 'GET demo:1', 'INCR demo:counter', 'SCAN 0 COUNT 5', 'NOPE']) { await input.fill(c); await input.press('Enter'); await sleep(300) }
  await sleep(400)
  await panel(page, 'h-try', `component-tryit-used-${scheme}`)
  await panel(page, 'h-writes', `component-writes-you-${scheme}`)
  await shot(page, `05-inspector-tryit-${scheme}`)

  // crash: arm, fire, overlay stages
  await page.evaluate(() => window.scrollTo(0, 0))
  await page.getByRole('button', { name: /Lift the safety cover/ }).scrollIntoViewIfNeeded()
  await page.getByRole('button', { name: /Lift the safety cover/ }).click()
  await sleep(700)
  await panel(page, 'h-ctl', `component-controls-armed-${scheme}`)
  await page.getByRole('button', { name: /CRASH IT/ }).click()
  await page.locator('.down-overlay.on').waitFor({ timeout: 10000 })
  await shot(page, `06-crash-signal-lost-${scheme}`, false)
  await page.locator('.down-box li').nth(2).waitFor({ timeout: 30000 })
  await shot(page, `07-crash-recovering-${scheme}`, false)
  await page.getByRole('heading', { name: 'back online' }).waitFor({ timeout: 30000 })
  await shot(page, `08-crash-back-online-${scheme}`, false)
  await page.locator('.down-overlay.on').waitFor({ state: 'detached', timeout: 15000 }).catch(() => {})
  await page.waitForFunction(() => !document.querySelector('.down-overlay.on'), null, { timeout: 15000 })
  await sleep(800)
  await shot(page, `09-after-recovery-${scheme}`, false)

  await page.goto(url('#/about')); await sleep(700)
  await shot(page, `10-about-${scheme}`)
  await ctx.close()

  for (const [w, h, tag] of [[1024, 768, 'tablet-landscape'], [768, 1024, 'tablet'], [390, 844, 'mobile']]) {
    const c = await browser.newContext({ viewport: { width: w, height: h }, colorScheme: scheme })
    const p = await c.newPage()
    await p.goto(url()); await waitLive(p); await sleep(1500)
    await shot(p, `11-inspector-${tag}-${scheme}`)
    await p.goto(url('#/about')); await sleep(600)
    await shot(p, `12-about-${tag}-${scheme}`)
    await c.close()
  }
}
// reduced motion sanity shot
const rc = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: 'dark', reducedMotion: 'reduce' })
const rp = await rc.newPage(); await rp.goto(url()); await waitLive(rp); await load(3000); await sleep(1200)
await shot(rp, '13-reduced-motion-dark', false)
await rc.close()
await browser.close()
fs.writeFileSync(`${OUT}/INDEX.txt`, shots.map((s) => s.replace(`${OUT}/`, '')).join('\n') + '\n')
console.log(`${shots.length} screenshots in ${OUT}`)
