// scrape-cpa.js
const puppeteer = require('puppeteer');
const fs = require('fs');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const jitter = (min, max) => Math.floor(min + Math.random() * (max - min));
const rand = (a, b) => jitter(a, b);

const MAX_RETRIES_PER_LOCATION = 4;          
const BASE_BACKOFF_MS = 60_000;              
const BETWEEN_LOCATIONS_MS = [2_000, 3_500]; 
const SLOWMO_MS = 60;                        

const LOCATIONS = [
  
];

function makeUA() {
  const chromeMajor = 120 + Math.floor(Math.random() * 5); // 120–124
  return `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 `
       + `(KHTML, like Gecko) Chrome/${chromeMajor}.0.0.0 Safari/537.36`;
}

async function launchBrowser() {
  return puppeteer.launch({
    headless: false, // set true in CI
    slowMo: SLOWMO_MS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
    ],
    defaultViewport: { width: 1280, height: 900 },
  });
}

// Detect rate limiting via network + content
function attachRateLimitDetectors(page) {
  const signals = { rateLimited: false, cloudflare1015: false };

  page.on('response', async (res) => {
    try {
      const status = res.status();
      if (status === 429) signals.rateLimited = true;
      // Optional: watch specific Cloudflare endpoints if needed
    } catch { /* ignore */ }
  });

  page.on('requestfailed', (req) => {
    // not strictly a rate limit signal, but can accompany 1015/429 storms
  });

  // Periodic DOM checks for Cloudflare 1015 page
  const checkDom = async () => {
    try {
      const html = (await page.content()).toLowerCase();
      if (html.includes('error 1015') || html.includes('rate limited')) {
        signals.rateLimited = true;
        if (html.includes('1015')) signals.cloudflare1015 = true;
      }
    } catch { /* ignore */ }
  };

  // Run a few times early; caller can also invoke on demand
  const timer = setInterval(checkDom, 1500);

  return {
    signals,
    stop: () => clearInterval(timer),
    checkDom,
  };
}

async function waitOnBlock(signals, attempt) {
  if (signals.cloudflare1015) {
    const cool = rand(8 * 60_000, 14 * 60_000); // 8–14 minutes
    console.warn(`🛑 Cloudflare 1015 detected. Cooling down for ${Math.round(cool/60000)} min...`);
    await sleep(cool);
    return;
  }
  // Generic 429/backoff: exponential with jitter
  const backoff = BASE_BACKOFF_MS * Math.pow(2, Math.max(0, attempt - 1));
  const withJitter = backoff + rand(5_000, 25_000);
  console.warn(`⏳ Rate limit/backoff: waiting ${(withJitter/1000).toFixed(0)}s (attempt ${attempt})...`);
  await sleep(withJitter);
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

    // Wait for results
    await Promise.race([
      page.waitForSelector('li.resultItem', { timeout: 30_000 }),
      (async () => {
        // If blocked mid-wait
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
    fs.writeFileSync(`${location.replace(/[^\w\-]+/g, '_')}.csv`, csv, 'utf8');

    console.log(`✅ Extracted ${results.length} records → ${location}.csv`);
  } catch (err) {
    const isRateLimited = detectors?.signals.rateLimited;
    console.error(`❌ Error on ${location}: ${err.message}${isRateLimited ? ' (rate limited)' : ''}`);
    if (attempt < MAX_RETRIES_PER_LOCATION) {
      await waitOnBlock(detectors?.signals || { rateLimited: false, cloudflare1015: false }, attempt);
      await browser.close().catch(()=>{});
      return runOnce(location, attempt + 1);
    }
  } finally {
    try { detectors?.stop?.(); } catch {}
    await sleep(1500);
    await browser.close().catch(()=>{});
  }
}

(async () => {
  for (const loc of LOCATIONS) {
    await runOnce(loc);
    await sleep(rand(BETWEEN_LOCATIONS_MS[0], BETWEEN_LOCATIONS_MS[1]));
  }
  console.log('\n🏁 All locations processed.');
})();
