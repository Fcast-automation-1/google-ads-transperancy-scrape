const puppeteer = require('puppeteer');
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1beJ263B3m4L8pgD9RWsls-orKLUvLMfT2kExaiyNl7g';
const SHEET_NAME = 'Sheet1';
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

async function getUrlData(sheets) {
    // Read columns A through H to check for existing data in G and H
    // A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7
    const response = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!A:H`,
    });

    const rows = response.data.values || [];
    const toProcess = [];

    // Skip header row
    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const url = row[0]?.trim();
        const existingAppLink = row[6]?.trim(); // Column G
        const existingAppName = row[7]?.trim(); // Column H

        // Process if URL exists and Column G (APP Links) is empty
        if (url && !existingAppLink) {
            toProcess.push({
                url: url,
                rowIndex: i // 0-indexed row number (row 2 in sheet is index 1)
            });
        }
    }
    return toProcess;
}

async function batchWriteToSheet(sheets, updates) {
    if (updates.length === 0) return;

    const data = [];
    updates.forEach(({ rowIndex, appName, storeLink }) => {
        // Column G is index 6, Column H is index 7
        // Spreadsheet rows are 1-indexed, so rowIndex + 1
        data.push({
            range: `${SHEET_NAME}!G${rowIndex + 1}`,
            values: [[storeLink]]
        });
        data.push({
            range: `${SHEET_NAME}!H${rowIndex + 1}`,
            values: [[appName]]
        });
    });

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: {
                valueInputOption: 'RAW',
                data: data
            }
        });
        console.log(`  ✅ Batch updated ${updates.length} rows (Columns G & H)`);
    } catch (error) {
        console.error(`  ❌ Batch update error:`, error.message);
    }
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
        await page.goto(transparencyUrl, { waitUntil: 'networkidle0', timeout: MAX_WAIT_TIME });

        const baseWait = 5000 * Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait);

        // Scroll to trigger lazy loading
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await sleep(2000);
        await page.evaluate(() => window.scrollTo(0, 0));
        await sleep(2000);

        const frames = page.frames();
        for (let i = 0; i < frames.length; i++) {
            const frame = frames[i];
            try {
                const frameData = await frame.evaluate(() => {
                    const data = { appName: null, storeLink: null };

                    // Store Link detection (using XPath and selectors from user-provided logic)
                    const xpath = '//*[@id="portrait-landscape-phone"]/div[1]/div[5]/a[2]';
                    const xpathResult = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
                    if (xpathResult && xpathResult.href) data.storeLink = xpathResult.href;

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

                    if (!data.storeLink) {
                        const allLinks = document.querySelectorAll('a[href*="googleadservices"]');
                        if (allLinks.length > 0) data.storeLink = allLinks[0].href;
                    }

                    // App Name detection
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

        if (result.storeLink === 'NOT_FOUND') {
            const html = await page.content();
            const matches = html.match(/https:\/\/www\.googleadservices\.com\/pagead\/aclk[^"'’\s]*/g);
            if (matches && matches.length > 0) result.storeLink = matches[0];
        }

        // Cleanup Google Ad Services redirects to direct store links if possible
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
        if (data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND') return data;
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
    console.log(`🤖 Starting App Info Agent (Sheet1 Column G & H)...\n`);

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

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
            console.log(`  🔗 [Row ${item.rowIndex + 1}] Processing...`);
            const data = await extractWithRetry(item.url, browser);
            console.log(`  ✅ [Row ${item.rowIndex + 1}] Result: [${data.appName}]`);
            return { rowIndex: item.rowIndex, appName: data.appName, storeLink: data.storeLink };
        }));

        await batchWriteToSheet(sheets, batchResults);
    }

    await browser.close();
    console.log('\n🏁 Workflow complete.');
})();
