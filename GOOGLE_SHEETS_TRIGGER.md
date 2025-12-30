# Google Sheets Auto-Start Setup

To make your scraping workflow start automatically when new data is added to your Google Sheet, follow these steps:

## 1. Open Script Editor
1. Open your Google Sheet.
2. Go to **Extensions** > **Apps Script**.

## 2. Add the Script
Copy and paste the following code into the script editor (replace any existing code):

```javascript
// CONFIGURATION
const GITHUB_OWNER = 'YOUR_GITHUB_USERNAME'; // e.g., 'johndoe'
const GITHUB_REPO = 'google-ads-transperancy-scrape'; // Your repository name
const GITHUB_TOKEN = 'YOUR_GITHUB_PAT_TOKEN'; // Start with ghp_...

function triggerGitHubWorkflow() {
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/dispatches`;
  
  const payload = {
    event_type: 'sheet_update',
    client_payload: {
      timestamp: new Date().toISOString()
    }
  };

  const options = {
    method: 'post',
    headers: {
      'Authorization': 'token ' + GITHUB_TOKEN,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload)
  };

  try {
    UrlFetchApp.fetch(url, options);
    Logger.log('✅ Workflow triggered successfully');
  } catch (error) {
    Logger.log('❌ Error triggering workflow: ' + error.toString());
  }
}

// Check specifically for new URLs that don't have video IDs yet
function checkForChanges(e) {
  const sheet = e.source.getActiveSheet();
  const range = e.range;
  const startRow = range.getRow();
  const numRows = range.getNumRows();

  let hasNewWork = false;

  // Check the rows that were just changed
  for (let i = 0; i < numRows; i++) {
    const currentRow = startRow + i;
    const url = sheet.getRange(currentRow, 1).getValue().toString().trim(); // Column A
    const videoId = sheet.getRange(currentRow, 6).getValue().toString().trim(); // Column F

    // If A has a URL and F is empty, we found something new to work on!
    if (url !== "" && videoId === "") {
      hasNewWork = true;
      break; // One row is enough to justify starting the scraper
    }
  }

  if (hasNewWork) {
    Logger.log("🚀 New unprocessed URL(s) detected. Starting GitHub scraper...");
    triggerGitHubWorkflow();
  } else {
    Logger.log("ℹ️ Change ignored: No new unprocessed URLs found in this edit.");
  }
}
```

## 3. Set up the Trigger
Since the script needs to connect to GitHub, it cannot run automatically without setup.

1. In the Apps Script sidebar, click on the **Triggers** icon (alarm clock).
2. Click **+ Add Trigger** (bottom right).
3. configure it as follows:
   - **Choose which function to run**: `checkForChanges`
   - **Select event source**: `From spreadsheet`
   - **Select event type**: `On change` (This covers rows added, copy-paste, etc.)
4. Click **Save**.
5. You will see a "Sign in with Google" popup.
6. Click **Advanced** > **Go to (Script Name) (unsafe)** > **Allow**.

## 4. Get a GitHub Token
1. Go to GitHub > Settings > Developer settings > Personal access tokens > Tokens (classic).
2. Generate a new token.
   - For **Public** Repos: Check the **`public_repo`** scope.
   - For **Private** Repos: Check the **`repo`** scope.
3. Paste this token into the `GITHUB_TOKEN` variable in the script above.
