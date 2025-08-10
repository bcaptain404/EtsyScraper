These scripts aid in dumping Etsy ad metrics [of your own Etsy shop] into a csv file.

NOTE: AI was used to churn-out these scripts for a single-use purpose; it has hardly been touched by human hands, I'll probably come back to this project some day and do something proper to it. But for now, this software is brand-new, untested, and probably full of bugs - use at your own risk.

Steps:
- run make-shim.sh - this will create a symlink to your Chrome profile (a necessary step [for now] to get automation working in Chrome). Only run this once.
- close all chrome processes.
- run scrape.sh - this will launch chrome.
- etsy should be open now. click through each month to get DAILY ad metric data. If you get monthly or yearly data without getting daily data for that particular timeframe (or if there is no data for that particular day), it will populate that day with the whole month's data in the CSV.
- close chrome.
- CTRL+C out of the script if you have to
- run harvest.sh
- open the generated CSV file, inspect for errors, etc. voila.

I'll pretty this project up later.
