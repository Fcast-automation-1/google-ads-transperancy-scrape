const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const INPUT_SHEET_NAME = 'Sheet1';
const OUTPUT_SHEET_NAME = 'Sheet6';
const CREDENTIALS_PATH = './credentials.json';
const CONCURRENT_PAGES = 3;
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
const RETRY_WAIT_MULTIPLIER = 1.5;

// ============================================
// GOOGLE SHEETS SETUP
// ============================================
async function getGoogleSheetsClient() {
    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

async function getInputsAndProcessed(sheets) {
    const inputResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${INPUT_SHEET_NAME}!A:A`,
    });
    const inputRows = inputResponse.data.values || [];

    const outputResponse = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${OUTPUT_SHEET_NAME}!A:A`,
    });
    const processedUrls = new Set((outputResponse.data.values || []).map(row => row[0]?.trim()).filter(Boolean));

    const toProcess = [];
    for (let i = 1; i < inputRows.length; i++) {
        const url = inputRows[i][0]?.trim();
        if (url && !processedUrls.has(url)) {
            toProcess.push({ url, rowIndex: i });
        }
    }
    return toProcess;
}

// ============================================
// AD DATA EXTRACTOR
// ============================================
async function extractAppData(transparencyUrl, browser, attempt = 1) {
    const page = await browser.newPage();
    const sleep = (ms) => new Promise(r => setTimeout(r, ms));
    let result = { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };

    try {
        console.log(`  🚀 Loading: ${transparencyUrl.substring(0, 60)}...`);

        // Using networkidle0 as per the user's provided code for thorough loading
        await page.goto(transparencyUrl, { waitUntil: 'networkidle0', timeout: MAX_WAIT_TIME });

        const baseWait = 5000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait);

        // Scroll to trigger lazy loading
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await sleep(2000);
        await page.evaluate(() => window.scrollTo(0, 0));
        await sleep(2000);

        // 1. Check ALL frames for both App Name and Ad Link
        const frames = page.frames();
        for (let i = 0; i < frames.length; i++) {
            const frame = frames[i];
            try {
                const frameData = await frame.evaluate(() => {
                    const data = { appName: null, storeLink: null };

                    // --- STORE LINK DETECTION ---
                    // Try XPath from user code
                    const xpath = '//*[@id="portrait-landscape-phone"]/div[1]/div[5]/a[2]';
                    const xpathResult = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                    if (xpathResult && xpathResult.href) {
                        data.storeLink = xpathResult.href;
                    }

                    // Try specified selectors
                    const linkSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a.ns-sbqu4-e-75[href*="googleadservices"]',
                        'a.install-button-anchor[href*="googleadservices"]',
                        'a[data-asoch-targets][href*="googleadservices"]',
                        '#portrait-landscape-phone a[href*="googleadservices"]',
                        'a[href*="googleadservices.com/pagead/aclk"]'
                    ];

                    if (!data.storeLink) {
                        for (const sel of linkSelectors) {
                            const el = document.querySelector(sel);
                            if (el && el.href) {
                                data.storeLink = el.href;
                                break;
                            }
                        }
                    }

                    // fallback to any googleadservices link
                    if (!data.storeLink) {
                        const allLinks = document.querySelectorAll('a[href*="googleadservices"]');
                        if (allLinks.length > 0) data.storeLink = allLinks[0].href;
                    }

                    // --- APP NAME DETECTION ---
                    const nameSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        '.short-app-name a',
                        'a[class*="app-name"]',
                        'span[class*="app-name"]'
                    ];
                    for (const sel of nameSelectors) {
                        const el = document.querySelector(sel);
                        if (el && el.innerText.trim()) {
                            data.appName = el.innerText.trim();
                            break;
                        }
                    }

                    return data;
                });

                if (frameData.storeLink && result.storeLink === 'NOT_FOUND') result.storeLink = frameData.storeLink;
                if (frameData.appName && result.appName === 'NOT_FOUND') result.appName = frameData.appName;

                if (result.storeLink !== 'NOT_FOUND' && result.appName !== 'NOT_FOUND') break;
            } catch (e) { }
        }

        // 2. Search main page and Regex Fallback if still not found
        if (result.storeLink === 'NOT_FOUND') {
            const html = await page.content();
            const matches = html.match(/https:\/\/www\.googleadservices\.com\/pagead\/aclk[^"'’\s]*/g);
            if (matches && matches.length > 0) {
                result.storeLink = matches[0];
            }
        }

        // --- FINAL CLEANUP OF LINK ---
        if (result.storeLink !== 'NOT_FOUND' && result.storeLink.includes('googleadservices.com/')) {
            try {
                const urlObj = new URL(result.storeLink);
                const adUrl = urlObj.searchParams.get('adurl');
                if (adUrl) result.storeLink = adUrl;
            } catch (e) { }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { appName: 'ERROR', storeLink: 'ERROR' };
    }
}

async function extractWithRetry(url, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const data = await extractAppData(url, browser, attempt);
        // If we found at least one pieces of info, we count it as a success for that level
        if (data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND') {
            return data;
        }
        if (attempt < MAX_RETRIES) {
            console.log(`  🔄 [Attempt ${attempt}] Info not found, retrying...`);
            await new Promise(r => setTimeout(r, 3000));
        }
    }
    return { appName: 'NOT_FOUND', storeLink: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting App Info Agent (Sheet1 -> ${OUTPUT_SHEET_NAME})...\n`);

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getInputsAndProcessed(sheets);

    if (toProcess.length === 0) {
        console.log('✨ No new URLs to process.');
        process.exit(0);
    }

    console.log(`📋 Found ${toProcess.length} new URLs to process\n`);

    const browser = await puppeteer.launch({
        headless: true,
        defaultViewport: { width: 1920, height: 1080 },
        args: [
            '--autoplay-policy=no-user-gesture-required',
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ]
    });

    for (let i = 0; i < toProcess.length; i += CONCURRENT_PAGES) {
        const batch = toProcess.slice(i, i + CONCURRENT_PAGES);
        console.log(`\n📦 Batch ${Math.floor(i / CONCURRENT_PAGES) + 1}/${Math.ceil(toProcess.length / CONCURRENT_PAGES)}`);

        const batchResults = await Promise.all(batch.map(async (item) => {
            console.log(`  🔗 Processing: ...${item.url.substring(item.url.length - 30)}`);
            const data = await extractWithRetry(item.url, browser);
            console.log(`  ✅ Result: [${data.appName}] [${data.storeLink.substring(0, 30)}...]`);
            return { url: item.url, appName: data.appName, storeLink: data.storeLink };
        }));

        const values = batchResults.map(r => [r.url, r.appName, r.storeLink, new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' })]);

        try {
            await sheets.spreadsheets.values.append({
                spreadsheetId: SPREADSHEET_ID,
                range: `${OUTPUT_SHEET_NAME}!A:D`,
                valueInputOption: 'RAW',
                resource: { values }
            });
            console.log(`  💾 Saved ${batchResults.length} rows to ${OUTPUT_SHEET_NAME}`);
        } catch (err) {
            console.error(`  ❌ Sheet write error: ${err.message}`);
        }
    }

    await browser.close();
    console.log('\n🏁 Workflow complete.');
})();
