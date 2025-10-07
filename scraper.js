// scrape-cpa.js
// Updated: increased backoffs, batch cooldowns, slower human-like pacing,
// smaller batches, Cloudflare-specific cooldown, and extra idle after rate-limit.
const puppeteer = require('puppeteer');
const fs = require('fs');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = (min, max) => Math.floor(min + Math.random() * (max - min));
const rand = (a, b) => jitter(a, b);

// ======= Tunables (changed) =======
const MAX_RETRIES_PER_LOCATION = 4;
const BASE_BACKOFF_MS = 90_000;                    // base backoff 90s
const BETWEEN_LOCATIONS_MS = [3_500, 7_000];       // 3.5s - 7s between locations
const BETWEEN_BATCH_MS = [5 * 60_000, 7 * 60_000]; // 5 - 7 minutes between batches
const SLOWMO_MS = 150;                             // slowMo to look more human
const BATCH_SIZE = 3;                              // smaller batches
// ==================================

const LOCATIONS = [
  "Ashmore",
  "Ashwell", "Aspley", "Atherton", "Athol", "Atkinsons Dam",
  "Aubigny", "Auburn", "Auchenflower", "Augathella", "Augustine Heights",
  "Aurukun", "Austinville", "Avenell Heights", "Avoca", "Avoca Vale",
  "Avondale", "Ayr"
];

function makeUA() {
  const chromeMajor = 120 + Math.floor(Math.random() * 5);
  return `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 `
       + `(KHTML, like Gecko) Chrome/${chromeMajor}.0.0.0 Safari/537.36`;
}

async function launchBrowser(proxy) {
  const args = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
  ];
  // If you decide to use proxies, pass `proxy` string to this function and uncomment:
  // if (proxy) args.push(`--proxy-server=${proxy}`);

  return puppeteer.launch({
    headless: false, // set to true in CI if you prefer
    slowMo: SLOWMO_MS,
    args,
    defaultViewport: { width: 1280, height: 900 },
  });
}

function attachRateLimitDetectors(page) {
  const signals = { rateLimited: false, cloudflare1015: false };

  page.on('response', async (res) => {
    try {
      const status = res.status();
      if (status === 429) signals.rateLimited = true;
      // some Cloudflare responses might be 403 with a 1015 message in body
      if (status === 403 || status === 451) {
        // we still inspect DOM below for '1015'
      }
    } catch { /* ignore errors */ }
  });

  page.on('requestfailed', (req) => {
    // optional hook for debugging, not used now
    // console.warn(`Request failed: ${req.url()} ${req.failure()?.errorText}`);
  });

  const checkDom = async () => {
    try {
      const html = (await page.content()).toLowerCase();
      if (html.includes('error 1015') || html.includes('rate limited') || html.includes('you have been rate limited')) {
        signals.rateLimited = true;
        if (html.includes('1015')) signals.cloudflare1015 = true;
      }
    } catch { /* ignore */ }
  };

  const timer = setInterval(() => {
    // run checkDom but swallow errors
    checkDom().catch(()=>{});
  }, 1500);

  return {
    signals,
    stop: () => clearInterval(timer),
    checkDom,
  };
}

async function waitOnBlock(signals, attempt) {
  // Cloudflare 1015 gets a longer cooldown
  if (signals?.cloudflare1015) {
    const cool = rand(10 * 60_000, 18 * 60_000); // 10–18 minutes
    console.warn(`\n🛑 Cloudflare 1015 detected. Cooling down for ~${Math.round(cool/60000)} minutes...`);
    await sleep(cool);
    return;
  }

  // exponential backoff with bigger base and jitter
  const backoff = BASE_BACKOFF_MS * Math.pow(2, Math.max(0, attempt - 1));
  const withJitter = backoff + rand(10_000, 45_000);
  console.warn(`\n⏳ Rate limit detected. Backing off ${(withJitter/1000).toFixed(0)}s (attempt ${attempt})...`);
  await sleep(withJitter);

  // extra idle to avoid retry spikes
  const extraIdle = rand(30_000, 120_000); // 30s - 2min
  console.warn(`🛌 Extra idle for ${(extraIdle/1000).toFixed(0)}s to reduce retry spike...`);
  await sleep(extraIdle);
}

async function runOnce(location, attempt = 1) {
  console.log(`\n=== 🔍 Scraping: ${location} (attempt ${attempt}/${MAX_RETRIES_PER_LOCATION}) ===`);
  const browser = await launchBrowser();
  let detectors;
  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(45_000);
    await page.setUserAgent(makeUA());

    detectors = attachRateLimitDetectors(page);

    // Go to search page
    await page.goto('https://apps.cpaaustralia.com.au/find-a-cpa/', { waitUntil: 'domcontentloaded', timeout: 45_000 });
    await detectors.checkDom();
    if (detectors.signals.rateLimited) throw new Error('Rate limited on landing');

    // Type location
    const inputSel = 'input[type="text"]:not([aria-hidden="true"])';
    await page.waitForSelector(inputSel, { visible: true });
    await page.click(inputSel, { clickCount: 3, delay: rand(20, 60) });
    await page.type(inputSel, location, { delay: rand(30, 75) });
    await sleep(rand(250, 600));

    // Pick first suggestion
    try {
      await page.waitForFunction(() => !!(
        document.querySelector('.pac-item') ||
        document.querySelector('[role="listbox"] [role="option"]') ||
        document.querySelector('.MuiAutocomplete-option')
      ), { timeout: 6000 });
      await page.keyboard.press('ArrowDown', { delay: rand(20, 80) });
      await page.keyboard.press('Enter', { delay: rand(20, 80) });
    } catch {
      const opt =
        (await page.$('.pac-item')) ||
        (await page.$('[role="listbox"] [role="option"]')) ||
        (await page.$('.MuiAutocomplete-option'));
      if (opt) await opt.click({ delay: rand(10, 40) });
    }
    await sleep(rand(300, 700));

    // Search
    await page.waitForSelector('#initiateSearchBtn', { visible: true });
    await page.click('#initiateSearchBtn', { delay: rand(10, 40) });

    // Wait for results or check for rate-limit while waiting
    await Promise.race([
      page.waitForSelector('li.resultItem', { timeout: 30_000 }),
      (async () => {
        for (let i = 0; i < 20; i++) {
          await detectors.checkDom();
          if (detectors.signals.rateLimited) throw new Error('Rate limited while waiting for results');
          await sleep(1000);
        }
      })()
    ]);

    // Try to load more by scrolling
    for (let i = 0; i < 6; i++) {
      const before = await page.$$eval('li.resultItem', els => els.length);
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await sleep(rand(600, 1200));
      const after = await page.$$eval('li.resultItem', els => els.length);
      if (after <= before) break;
    }

    // Extract data
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

    const uniq = new Map();
    for (const r of rows) {
      const key = r.accountId || `${(r.name || '').toLowerCase()}|${(r.address || '').toLowerCase()}`;
      if (!uniq.has(key)) uniq.set(key, r);
    }
    const results = [...uniq.values()];

    const cols = ['accountId','type','name','address','email','phone','website','lat','lng'];
    const esc = v => (v == null ? '' : `"${String(v).replace(/"/g, '""')}"`);
    const csv = [cols.join(','), ...results.map(r => cols.map(k => esc(r[k])).join(','))].join('\n');

    // safe filename
    const safeName = location.replace(/[^\w\-]+/g, '_');
    fs.writeFileSync(`${safeName}.csv`, csv, 'utf8');

    console.log(`✅ Extracted ${results.length} records → ${safeName}.csv`);
  } catch (err) {
    const isRateLimited = detectors?.signals?.rateLimited;
    console.error(`❌ Error on ${location}: ${err.message}${isRateLimited ? ' (rate limited)' : ''}`);
    if (attempt < MAX_RETRIES_PER_LOCATION) {
      await waitOnBlock(detectors?.signals || { rateLimited: false, cloudflare1015: false }, attempt);
      try { await browser.close(); } catch {}
      return runOnce(location, attempt + 1);
    } else {
      console.error(`⚠️ Max retries reached for ${location}. Skipping.`);
    }
  } finally {
    try { detectors?.stop?.(); } catch {}
    await sleep(1500);
    try { await browser.close(); } catch {}
  }
}

(async () => {
  console.log(`Starting scrape: ${LOCATIONS.length} locations, batch size ${BATCH_SIZE}.`);
  for (let i = 0; i < LOCATIONS.length; i += BATCH_SIZE) {
    const batch = LOCATIONS.slice(i, i + BATCH_SIZE);
    console.log(`\n--- Starting batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} locations) ---`);
    for (const loc of batch) {
      await runOnce(loc);

      // small pause between locations to mimic human behavior
      await sleep(rand(BETWEEN_LOCATIONS_MS[0], BETWEEN_LOCATIONS_MS[1]));
    }

    if (i + BATCH_SIZE < LOCATIONS.length) {
      // cooldown between batches (5-7 minutes randomized)
      const cooldown = rand(BETWEEN_BATCH_MS[0], BETWEEN_BATCH_MS[1]);
      const minutes = (cooldown / 60000).toFixed(2);
      console.log(`\n🛌 Batch complete. Cooling down for ${minutes} minutes before next batch...`);
      await sleep(cooldown);
    } else {
      console.log('\n🏁 Final batch complete.');
    }
  }
  console.log('\n✅ All locations processed.');
})();
