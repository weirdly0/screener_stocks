# High Delivery Quantity Scanner (Free, NSE)

## Overview

This tool scans NSE stocks for high delivery quantity spikes, helping traders and analysts identify unusual delivery activity. It uses official NSE datasets, applies robust filtering, and outputs results in Excel format, with optional TradingView links and Telegram alerts.

- **Universe:** Uses cached `sec_list.csv` (preferred) or falls back to `EQUITY_L.csv`.
- **Bhav Prefilter:** Uses legacy daily bhavcopy to skip illiquid/no-move stocks.
- **Delivery Data:** Fetches per-symbol delivery data from NSE's `priceVolumeDeliverable` API.
- **Async Mode:** Optional turbo mode using `httpx` for concurrent requests.
- **Filters:**
  - % Change > 0
  - Deliverable Qty ≥ 10,000 (configurable)
  - Delivery spike ≥ 4x (vs last N days avg, configurable)
  - Market Cap ≥ ₹100 cr (configurable)
- **Output:** Excel file (Candidates + Raw), TradingView links, optional Telegram alert.

## Features

- Fast, robust, and free (no paid APIs)
- Caches universe files for speed
- Bhavcopy prefilter for liquidity and price movement
- Delivery spike detection
- Market cap filtering via Yahoo Finance
- Excel output with full details
- TradingView chart links for each candidate
- Optional Telegram alerts (with summary and Excel attachment)
- Optional async mode for faster scans

## Installation

1. **Clone or Download** the script to your machine.
2. **Install dependencies:**
   ```zsh
   pip install pandas requests yfinance openpyxl tqdm httpx
   ```
   - `tqdm` and `httpx` are optional (for progress bars and async mode).
3. **(Optional) Set up Telegram alerts:**
   - Set environment variables:
     ```zsh
     export TELEGRAM_BOT_TOKEN="<your_bot_token>"
     export TELEGRAM_CHAT_ID="<your_chat_id>"
     ```

## Usage

Run the script from the command line:

```zsh
python fast.py [options]
```

### Main Options

| Option                   | Description                                                                                 | Default                |
|-------------------------|---------------------------------------------------------------------------------------------|------------------------|
| `--spike-multiple`      | Delivery spike threshold (e.g., 4 = 4x spike vs avg)                                        | 3.0                    |
| `--avg-days`            | Number of days for average delivery calculation                                             | 5                      |
| `--min-deliv-qty`       | Minimum deliverable quantity to consider                                                    | 10,000                 |
| `--min-mcap-inr`        | Minimum market cap in INR (e.g., 1000000000 = ₹100 cr)                                     | 1,000,000,000          |
| `--symbols`             | Comma-separated list of NSE symbols to scan (overrides universe)                            |                        |
| `--universe`            | Universe source: `nse` (default) or `file`                                                 | nse                    |
| `--universe-file`       | Path to CSV file with SYMBOL column (if using `--universe file`)                            |                        |
| `--universe-cache-hours`| Max age (hours) for cached universe files                                                   | 24                     |
| `--no-bhav-prefilter`   | Disable bhav prefilter step (scan all symbols)                                              |                        |
| `--use-async`           | Use async mode (httpx) for faster data fetching                                             |                        |
| `--concurrency`         | Number of concurrent requests in async mode                                                 | 8                      |

### Example Commands

- **Default scan (all NSE EQ stocks):**
  ```zsh
  python fast.py
  ```
- **Scan with async mode and higher concurrency:**
  ```zsh
  python fast.py --use-async --concurrency 12
  ```
- **Scan specific symbols:**
  ```zsh
  python fast.py --symbols RELIANCE,SBIN,TATAMOTORS
  ```
- **Change thresholds:**
  ```zsh
  python fast.py --min-deliv-qty 20000 --spike-multiple 5
  ```
- **Use a custom universe file:**
  ```zsh
  python fast.py --universe file --universe-file my_watchlist.csv
  ```
- **Disable bhav prefilter (scan all):**
  ```zsh
  python fast.py --no-bhav-prefilter
  ```

- **Ema Feature**
  ```zsh
  --ema-days (default 200) and --no-ema-filter
  ```

## Output

- **Excel file:** Saved in `output/HighDelivery_<YYYY-MM-DD>.xlsx` with:
  - `Candidates` sheet: Filtered stocks meeting all criteria
  - `Raw_All` sheet: All scanned stocks with metrics
  - `Run_Info` sheet: Scan configuration
- **Console:** Prints summary and top candidates
- **TradingView links:** For each candidate
- **Telegram alert:** If configured, sends summary and Excel file

## Advanced Details

- **Universe caching:** Universe files are cached in `cache/` for speed. Use `--universe-cache-hours` to control refresh.
- **Bhav prefilter:** Uses latest available bhavcopy (up to 3 days back) to skip illiquid/no-move stocks.
- **Delivery data:** Fetches per-symbol CSV from NSE API, parses and computes metrics.
- **Market cap:** Fetched from Yahoo Finance for shortlisted stocks only.
- **Async mode:** Uses `httpx` and asyncio for concurrent requests (if installed and enabled).
- **Rate limiting:** Steady drip to avoid server throttling.
- **Robust retries:** Handles network errors and retries failed requests.
- **Telegram:** Sends summary and Excel file if `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set.

## Troubleshooting

- **No matches:** Try lowering thresholds (`--spike-multiple`, `--min-deliv-qty`, `--min-mcap-inr`).
- **API errors:** Check your internet connection and try again. NSE endpoints may be rate-limited.
- **Excel not saved:** Ensure `output/` directory exists and is writable.
- **Telegram not working:** Check your bot token and chat ID environment variables.

## File Structure

```
fast.py                # Main script
cache/                 # Cached universe files
output/                # Excel output files
```

## License

This script is provided free for personal and research use. No warranty. Use at your own risk.

## Author

- weirdly0
- Contributions welcome!
