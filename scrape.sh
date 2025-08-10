ROOT="$HOME/.config/google-chrome"
SHIM="$HOME/.config/google-chrome-playwright"
PROFILE="Profile 2"

#pkill -f 'chrome' || true

python etsy_ads_metrics_capture.py \
  --profile-dir "$SHIM" \
  --chrome-profile-name "$PROFILE" \
  --out-dir ./out \
  --headful --autorun --keep-open --save-all \
  --executable /opt/google/chrome/chrome

