// scrape-cpa.js
const fs = require('fs');

let puppeteer;
let useStealth = false;
try {
  // Optional: stealth if installed
  const pextra = require('puppeteer-extra');
  const Stealth = require('puppeteer-extra-plugin-stealth');
  pextra.use(Stealth());
  puppeteer = pextra;
  useStealth = true;
} catch {
  puppeteer = require('puppeteer');
}

/** ---------- timing helpers ---------- */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = (min, max) => Math.floor(min + Math.random() * (max - min));
const rand = (a, b) => jitter(a, b);
const now = () => Date.now();

/** ---------- knobs you can tune ---------- */
const MAX_RETRIES_PER_LOCATION = 4;                 // per-location retries
const BASE_BACKOFF_MS = 60_000;                     // base exponential backoff
const BETWEEN_LOCATIONS_MS = [3_500, 6_000];        // spacing within batch (raised)
const SLOWMO_MS = 80;                               // puppeteer slow motion per action (raised)

let BATCH_SIZE = 5;                                  // adaptive
let BETWEEN_BATCHES_MS = [180_000, 300_000];         // 3–5 min
let GLOBAL_COOLDOWN_ON_RL_MS = [420_000, 660_000];   // 7–11 min

// Global rate limiter: ensure at least this delay between each *search submit*
const MIN_MS_BETWEEN_SUBMITS = [18_000, 28_000];

// Optional proxy support: export HTTPS_PROXY or HTTP_PROXY env var to route traffic
const PROXY = process.env.HTTPS_PROXY || process.env.HTTP_PROXY || null;

// Persisted user data dir to keep cookies/session and look less “fresh”
const USER_DATA_DIR = './.pupp_profile';

/** ---------- locations ---------- */
const LOCATIONS = [
  "EXETER", "SEMAPHORE", "SEMAPHORE PARK", "SEMAPHORE SOUTH", "WEST LAKES SHORE",
  "WEST LAKES", "GRANGE", "HENLEY BEACH", "HENLEY BEACH SOUTH", "GOODWOOD",
  "TENNYSON", "FINDON", "SEATON", "FULHAM", "KINGS PARK",
  "PLYMPTON", "FULHAM GARDENS", "WEST BEACH", "FLINDERS PARK", "WHITES VALLEY",
  "KIDMAN PARK", "MILE END", "MILE END SOUTH", "THEBARTON", "TORRENSVILLE",
  "BROOKLYN PARK", "NORTH PLYMPTON", "HAWKS NEST STATION", "LOCKLEYS", "UNDERDALE",
  "COWANDILLA", "HILTON", "MARLESTON", "RICHMOND", "WEST RICHMOND",
  "CLARENCE PARK", "MILLSWOOD", "WAYVILLE", "ASHFORD", "BLACK FOREST",
  "EVERARD PARK", "FORESTVILLE", "KESWICK", "KESWICK TERMINAL", "GLANDORE",
  "KURRALTA PARK", "NETLEY", "PLYMPTON PARK", "SOUTH PLYMPTON", "CLARENCE GARDENS",
  "EDWARDSTOWN", "MELROSE PARK", "NOVAR GARDENS", "COLONEL LIGHT GARDENS", "CUMBERLAND PARK",
  "DAW PARK", "PANORAMA", "WESTBOURNE PARK", "BEDFORD PARK", "CLOVELLY PARK",
  "PASADENA", "ST MARYS", "TONSLEY", "ASCOT PARK", "MARION",
  "MITCHELL PARK", "MORPHETTVILLE", "PARK HOLME", "GLENGOWRIE", "SOMERTON PARK",
  "GLENELG", "GLENELG EAST", "GLENELG NORTH", "GLENELG SOUTH", "OAKLANDS PARK",
  "WARRADALE", "DARLINGTON", "SEACOMBE GARDENS", "SEACOMBE HEIGHTS", "STURT",
  "BRIGHTON", "DOVER GARDENS", "HOVE", "NORTH BRIGHTON", "SOUTH BRIGHTON",
  "KINGSTON PARK", "BROWN HILL CREEK", "MARINO", "SEACLIFF", "SEACLIFF PARK",
  "SEAVIEW DOWNS", "BLACKWOOD", "CLAPHAM", "HAWTHORN", "BELLEVUE HEIGHTS",
  "EDEN HILLS", "COROMANDEL VALLEY", "CRAIGBURN FARM", "HAWTHORNDENE", "BELAIR",
  "GLENALTA", "HYDE PARK", "MALVERN", "UNLEY", "UNLEY PARK",
  "KINGSWOOD", "LOWER MITCHAM", "LYNTON", "MITCHAM", "NETHERBY",
  "SPRINGFIELD", "TORRENS PARK", "EASTWOOD", "FREWVILLE", "FULLARTON",
  "BRAHMA LODGE", "HIGHGATE", "PARKSIDE", "GLEN OSMOND", "GLENUNGA"
];

/** ---------- UA ---------- */
function makeUA() {
  const chromeMajor = 120 + Math.floor(Math.random() * 6); // 120–125
  return `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ` +
         `(KHTML, like Gecko) Chrome/${chromeMajor}.0.0.0 Safari/537.36`;
}

/** ---------- global limiter ---------- */
let lastSubmitAt = 0;
async function rateLimitSubmitGate() {
  const minGap = rand(MIN_MS_BETWEEN_SUBMITS[0], MIN_MS_BETWEEN_SUBMITS[1]);
  const dt = now() - lastSubmitAt;
  if (dt < minGap) {
    const wait = minGap - dt + rand(500, 1500);
    console.log(`🛑 Submit gate: sleeping ${Math.round(wait/1000)}s to respect global pacing...`);
    await sleep(wait);
  }
  lastSubmitAt = now();
}

/** ---------- browser ---------- */
async function launchBrowserOnce() {
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
  ];
  if (PROXY) args.push(`--proxy-server=${PROXY}`);

  const browser = await puppeteer.launch({
    headless: false,
    slowMo: SLOWMO_MS,
    args,
    defaultViewport: { width: 1280 + rand(-32, 32), height: 900 + rand(-32, 32) },
    userDataDir: USER_DATA_DIR, // persist cookies/session
  });

  const context = browser.defaultBrowserContext();
  const page = await browser.newPage();

  // Block heavy resources to cut request count (less chance to trip limits)
  try {
    await page.setRequestInterception(true);
    page.on('request', req => {
      const type = req.resourceType();
      if (['image','media','font','stylesheet'].includes(type)) {
        return req.abort();
      }
      req.continue();
    });
  } catch {}

  page.setDefaultTimeout(60_000);
  await page.setUserAgent(makeUA());

  return { browser, context, page };
}

/** ---------- rate limit detectors with Retry-After ---------- */
function attachRateLimitDetectors(page) {
  const signals = { rateLimited: false, cloudflare1015: false, retryAfterMs: null, last429At: null };

  page.on('response', async (res) => {
    try {
      const status = res.status();
      if (status === 429) {
        signals.rateLimited = true;
        signals.last429At = Date.now();
        const ra = res.headers()['retry-after'];
        if (ra) {
          // Retry-After may be seconds or HTTP-date; handle seconds only here
          const secs = Number(ra);
          if (!Number.isNaN(secs) && secs > 0) signals.retryAfterMs = secs * 1000 + rand(500, 2500);
        }
      } else if (status === 403) {
        // Some CF configs answer 403 for throttles
        signals.rateLimited = true;
      }
    } catch {}
  });

  const checkDom = async () => {
    try {
      const html = (await page.content()).toLowerCase();
      if (html.includes('error 1015') || html.includes('rate limited')) {
        signals.rateLimited = true;
        if (html.includes('1015')) signals.cloudflare1015 = true;
      }
    } catch {}
  };

  const timer = setInterval(checkDom, 1500);
  return { signals, stop: () => clearInterval(timer), checkDom };
}

async function waitOnBlock(signals, attempt) {
  if (signals?.retryAfterMs) {
    console.warn(`⏳ Honor Retry-After: waiting ${(signals.retryAfterMs/1000).toFixed(0)}s...`);
    await sleep(signals.retryAfterMs);
    return;
  }
  if (signals?.cloudflare1015) {
    const cool = rand(10 * 60_000, 16 * 60_000);
    console.warn(`🛑 Cloudflare 1015 detected. Cooling down for ${Math.round(cool/60000)} min...`);
    await sleep(cool);
    return;
  }
  const backoff = BASE_BACKOFF_MS * Math.pow(2, Math.max(0, attempt - 1));
  const withJitter = backoff + rand(10_000, 30_000);
  console.warn(`⏳ Backoff: waiting ${(withJitter/1000).toFixed(0)}s (attempt ${attempt})...`);
  await sleep(withJitter);
}

/** ---------- single location run (reuses same page) ---------- */
async function runOnce(page, location, attempt = 1) {
  console.log(`\n=== 🔍 Scraping: ${location} (attempt ${attempt}/${MAX_RETRIES_PER_LOCATION}) ===`);
  let detectors;
  try {
    detectors = attachRateLimitDetectors(page);

    // Soft reload to landing page each time (keeps cookies/session)
    await page.goto('https://apps.cpaaustralia.com.au/find-a-cpa/', { waitUntil: 'domcontentloaded', timeout: 60_000 });
    await detectors.checkDom();
    if (detectors.signals.rateLimited) throw new Error('Rate limited on landing');

    const inputSel = 'input[type="text"]:not([aria-hidden="true"])';
    await page.waitForSelector(inputSel, { visible: true });

    // Clear any previous value carefully
    await page.click(inputSel, { clickCount: 3, delay: rand(30, 70) });
    await sleep(rand(200, 450));
    await page.keyboard.press('Backspace');
    await sleep(rand(150, 350));

    // Type slowly
    for (const ch of location.split('')) {
      await page.type(inputSel, ch, { delay: rand(55, 110) });
      if (Math.random() < 0.18) await sleep(rand(60, 180)); // micro-pauses
    }
    await sleep(rand(450, 800));

    // Wait for suggestion and select
    try {
      await page.waitForFunction(() => !!(
        document.querySelector('.pac-item') ||
        document.querySelector('[role="listbox"] [role="option"]') ||
        document.querySelector('.MuiAutocomplete-option')
      ), { timeout: 8000 });
      await page.keyboard.press('ArrowDown', { delay: rand(40, 100) });
      await page.keyboard.press('Enter', { delay: rand(40, 100) });
    } catch {
      const opt =
        (await page.$('.pac-item')) ||
        (await page.$('[role="listbox"] [role="option"]')) ||
        (await page.$('.MuiAutocomplete-option'));
      if (opt) await opt.click({ delay: rand(15, 55) });
    }
    await sleep(rand(500, 900));

    // Global limiter before clicking Search
    await rateLimitSubmitGate();

    const searchBtn = '#initiateSearchBtn';
    await page.waitForSelector(searchBtn, { visible: true });
    await page.click(searchBtn, { delay: rand(20, 60) });

    // Wait for results or fail if rate-limited
    await Promise.race([
      page.waitForSelector('li.resultItem', { timeout: 40_000 }),
      (async () => {
        for (let i = 0; i < 35; i++) {
          await detectors.checkDom();
          if (detectors.signals.rateLimited) throw new Error('Rate limited while waiting for results');
          await sleep(900);
        }
      })()
    ]);

    // Lazy load more (gentler)
    for (let i = 0; i < 5; i++) {
      const before = await page.$$eval('li.resultItem', els => els.length);
      await page.evaluate(() => window.scrollBy(0, Math.round(window.innerHeight * 0.85)));
      await sleep(rand(900, 1400));
      const after = await page.$$eval('li.resultItem', els => els.length);
      if (after <= before) break;
    }

    // Extract
    const rows = await page.evaluate(() => {
      const clean = (v) => {
        if (v == null) return null;
        const s = String(v).trim();
        if (s === '' || s.toLowerCase() === 'null' || s.toLowerCase() === 'undefined') return null;
        return s;
      };
      return [...document.querySelectorAll('li.resultItem')].map(li => {
        const d = li.dataset || {};
        return {
          accountId: clean(d.accountid),
          type: clean(d.acctype),
          name: clean(d.name),
          address: clean(d.address1) || clean(d.address2),
          email: clean(d.emailaddress),
          phone: clean(d.telephone1) || clean(d.telephone2),
          website: clean(d.websiteurl),
          lat: d.lat ? Number(d.lat) : null,
          lng: d.lng ? Number(d.lng) : null
        };
      });
    });

    // Dedupe
    const uniq = new Map();
    for (const r of rows) {
      const key = r.accountId || `${(r.name || '').toLowerCase()}|${(r.address || '').toLowerCase()}`;
      if (!uniq.has(key)) uniq.set(key, r);
    }
    const results = [...uniq.values()];

    // Write CSV per location
    const cols = ['accountId','type','name','address','email','phone','website','lat','lng'];
    const esc = v => (v == null ? '' : `"${String(v).replace(/"/g, '""')}"`);
    const csv = [cols.join(','), ...results.map(r => cols.map(k => esc(r[k])).join(','))].join('\n');
    fs.writeFileSync(`${location.replace(/[^\w\-]+/g, '_')}.csv`, csv, 'utf8');

    console.log(`✅ Extracted ${results.length} records → ${location}.csv  ${useStealth ? '[stealth]' : ''}`);
    return { ok: true, rateLimited: false, results: results.length };
  } catch (err) {
    const rl = detectors?.signals?.rateLimited || false;
    console.error(`❌ Error on ${location}: ${err.message}${rl ? ' (rate limited)' : ''}`);
    if (rl) {
      // escalate global pacing immediately
      lastSubmitAt = now(); // reset gate reference
    }
    if (attempt < MAX_RETRIES_PER_LOCATION) {
      await waitOnBlock(detectors?.signals || {}, attempt);
      return runOnce(page, location, attempt + 1);
    }
    return { ok: false, rateLimited: rl, results: 0 };
  } finally {
    try { detectors?.stop?.(); } catch {}
    await sleep(rand(1200, 2000));
  }
}

/** ---------- batching controller (adaptive) ---------- */
function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) out.push(array.slice(i, i + size));
  return out;
}

async function processBatch(page, batch, batchIndex, totalBatches) {
  console.log(`\n📦 Batch ${batchIndex + 1}/${totalBatches} :: ${batch.length} location(s)`);
  let batchSawRateLimit = false;

  for (let i = 0; i < batch.length; i++) {
    const loc = batch[i];
    const info = `${loc} (${i + 1}/${batch.length} in batch; global ${batchIndex * batch.length + i + 1}/${LOCATIONS.length})`;
    console.log(`➡️  ${info}`);

    const res = await runOnce(page, loc);
    if (res.rateLimited) batchSawRateLimit = true;

    const pause = rand(BETWEEN_LOCATIONS_MS[0], BETWEEN_LOCATIONS_MS[1]);
    console.log(`🧯 Cooling ${Math.round(pause/1000)}s before next location...`);
    await sleep(pause);
  }

  const delay = rand(BETWEEN_BATCHES_MS[0], BETWEEN_BATCHES_MS[1]);
  console.log(`\n⏸️  Batch ${batchIndex + 1} done. Waiting ${Math.round(delay/1000)}s before next batch...`);
  await sleep(delay);

  if (batchSawRateLimit) {
    // Adaptive: shrink batch size and extend cool-downs
    if (BATCH_SIZE > 3) BATCH_SIZE = Math.max(3, BATCH_SIZE - 1);
    BETWEEN_BATCHES_MS = [BETWEEN_BATCHES_MS[0] + 60_000, BETWEEN_BATCHES_MS[1] + 120_000];
    GLOBAL_COOLDOWN_ON_RL_MS = [GLOBAL_COOLDOWN_ON_RL_MS[0] + 120_000, GLOBAL_COOLDOWN_ON_RL_MS[1] + 180_000];

    const extra = rand(GLOBAL_COOLDOWN_ON_RL_MS[0], GLOBAL_COOLDOWN_ON_RL_MS[1]);
    console.warn(`🧊 Batch had rate limiting. Extra global cool-down ${Math.round(extra/1000)}s... (BATCH_SIZE → ${BATCH_SIZE})`);
    await sleep(extra);
  }
}

(async () => {
  console.log(`\n🚀 Starting. Total locations: ${LOCATIONS.length}. Initial batch size: ${BATCH_SIZE}. Stealth: ${useStealth ? 'on' : 'off'}. Proxy: ${PROXY ? 'on' : 'off'}`);

  const { browser, page } = await launchBrowserOnce();
  try {
    const batches = chunk(LOCATIONS, BATCH_SIZE);

    for (let b = 0; b < batches.length; b++) {
      // Re-slice if batch size changed adaptively
      const remaining = LOCATIONS.slice(b * BATCH_SIZE);
      const dynamicBatches = chunk(remaining, BATCH_SIZE);
      const currentBatch = dynamicBatches[0];
      await processBatch(page, currentBatch, b, batches.length);
    }

    console.log('\n🏁 All locations processed.');
  } finally {
    await sleep(1500);
    try { await page.close(); } catch {}
    try { await (await browser).close(); } catch {}
  }
})();
