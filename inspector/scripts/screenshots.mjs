// Captures every screen, state and component of the Inspector with Playwright.
//
// Expects: nebuladb running with the admin server on ADMIN (default
// http://127.0.0.1:7391) started with -supervise, and the built UI served at UI
// (default http://127.0.0.1:4173). Use scripts/run-screenshots.sh to set all
// that up. Output goes to OUT (default ./screenshots/baseline).
import { chromium } from 'playwright'
import fs from 'node:fs'

const ADMIN = process.env.ADMIN ?? 'http://127.0.0.1:7391'
const UI = process.env.UI ?? 'http://127.0.0.1:4173'
const OUT = process.env.OUT ?? 'screenshots/baseline'
const CHROME = process.env.CHROME
fs.mkdirSync(OUT, { recursive: true })

const browser = await chromium.launch({
  executablePath: CHROME || undefined,
  args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'],
})

const url = (hash = '') => `${UI}/?admin=${encodeURIComponent(ADMIN)}${hash}`
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const post = (path, body) => fetch(ADMIN + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })

// Generates write load through the same proxy the UI uses.
async function load(ms, workers = 4) {
  const stop = Date.now() + ms
  await Promise.all(Array.from({ length: workers }, async () => {
    while (Date.now() < stop) {
      const args = ['MSET']
      for (let i = 0; i < 25; i++) args.push(`user:${Math.floor(Math.random() * 200000).toString().padStart(6, '0')}`, 'v'.repeat(60 + Math.floor(Math.random() * 400)))
      await post('/api/cmd', { args }).catch(() => {})
    }
  }))
}

const shots = []
async function shot(page, name, opts = {}) {
  const file = `${OUT}/${name}.png`
  await page.screenshot({ path: file, fullPage: opts.fullPage ?? true })
  shots.push(file)
  console.log('  ', file)
}
async function card(page, heading) {
  return page.locator('section.card', { has: page.getByRole('heading', { name: heading, exact: true }) }).first()
}
async function cardShot(page, heading, name) {
  const el = await card(page, heading)
  const file = `${OUT}/${name}.png`
  await el.screenshot({ path: file })
  shots.push(file)
  console.log('  ', file)
}
async function waitLive(page) {
  await page.getByText('live', { exact: true }).first().waitFor({ timeout: 15000 })
}

for (const scheme of ['light', 'dark']) {
  console.log(`== ${scheme}`)
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, colorScheme: scheme })
  const page = await ctx.newPage()
  page.on('dialog', (d) => d.accept())

  // 1. empty database
  if (scheme === 'light') {
    await page.goto(url())
    await waitLive(page)
    await sleep(1200)
    await shot(page, `01-inspector-empty-${scheme}`)
  } else {
    await page.goto(url())
    await waitLive(page)
  }

  // 2. under load: tree forms, flushes and compactions happen
  const bg = load(9000)
  await sleep(3500)
  await shot(page, `02-inspector-active-${scheme}`)
  await sleep(1500)
  await shot(page, `03-inspector-active-2-${scheme}`, { fullPage: false })
  await bg
  await sleep(1500)

  // 3. components, individually
  const names = [
    ['Incoming writes', 'writes-feed'], ['Memtable and WAL', 'memtable'], ['LSM tree', 'levels'],
    ['Backpressure', 'backpressure'], ['Engine activity', 'activity'], ['Bloom filters', 'bloom'],
    ['Block cache', 'cache'], ['Try it', 'tryit-empty'], ['Controls', 'controls'],
  ]
  for (const [h, n] of names) await cardShot(page, h, `component-${n}-${scheme}`)
  await page.locator('.stat-row').screenshot({ path: `${OUT}/component-overview-${scheme}.png` })
  await page.locator('header.app-header').screenshot({ path: `${OUT}/component-header-${scheme}.png` })

  // 4. try it
  const input = page.getByLabel('command')
  for (const c of ['SET demo:1 "hello nebula"', 'GET demo:1', 'INCR demo:counter', 'SET demo:ttl x EX 30', 'TTL demo:ttl', 'SCAN 0 COUNT 5', 'NOPE']) {
    await input.fill(c)
    await input.press('Enter')
    await sleep(250)
  }
  await sleep(400)
  await cardShot(page, 'Try it', `component-tryit-used-${scheme}`)
  await shot(page, `04-inspector-tryit-${scheme}`)

  // 5. crash and recovery (once per theme)
  await page.evaluate(() => window.scrollTo(0, 0))
  await page.getByRole('button', { name: /Crash it/ }).scrollIntoViewIfNeeded()
  await page.getByRole('button', { name: /Crash it/ }).click()
  await page.evaluate(() => window.scrollTo(0, 0))
  await page.getByText(/Server unreachable/).waitFor({ timeout: 15000 })
  await shot(page, `05-crash-down-${scheme}`, { fullPage: false })
  await page.getByText(/Recovered after/).waitFor({ timeout: 30000 })
  await page.evaluate(() => window.scrollTo(0, 0))
  await sleep(1500)
  await shot(page, `06-crash-recovered-${scheme}`, { fullPage: false })
  await (await card(page, 'Engine activity')).screenshot({ path: `${OUT}/component-activity-recovery-${scheme}.png` })

  // 6. about page, desktop
  await page.goto(url('#/about'))
  await sleep(600)
  await shot(page, `07-about-${scheme}`)
  await ctx.close()

  // 7. mobile layout
  const mctx = await browser.newContext({ viewport: { width: 390, height: 844 }, colorScheme: scheme })
  const mp = await mctx.newPage()
  await mp.goto(url())
  await waitLive(mp)
  await sleep(1500)
  await shot(mp, `08-inspector-mobile-${scheme}`)
  await mp.goto(url('#/about'))
  await sleep(500)
  await shot(mp, `09-about-mobile-${scheme}`)
  await mctx.close()
}

await browser.close()
fs.writeFileSync(`${OUT}/INDEX.txt`, shots.map((s) => s.replace(`${OUT}/`, '')).join('\n') + '\n')
console.log(`${shots.length} screenshots in ${OUT}`)
