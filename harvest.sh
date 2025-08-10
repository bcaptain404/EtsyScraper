python etsy_ads_metrics_harvest.py \
  --in ./out \
  --csv ./out/etsy_ads_daily_allfields.csv \
  --derived \
  --tz-offset-hours -4 \
  --verbose \
  --keep-raw \
  --aggregate-policy min-nonzero \

