ROOT="$HOME/.config/google-chrome"
SHIM="$HOME/.config/google-chrome-playwright"
PROFILE="Profile 2"

mkdir -p "$SHIM"
ln -sfn "$ROOT/Local State" "$SHIM/Local State"
ln -sfn "$ROOT/$PROFILE"    "$SHIM/$PROFILE"

