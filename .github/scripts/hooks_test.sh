#!/bin/sh
# Verify GUI PATH recovery, explicit binaries, argument forwarding, and failures.
set -eu

hooks=$(cd "$(dirname "$0")/../../.githooks" && pwd)
sandbox=$(mktemp -d)
trap 'test -z "$sandbox" || rm -r -- "$sandbox"' EXIT HUP INT TERM
mkdir -p "$sandbox/home/go/bin" "$sandbox/bin"
cat > "$sandbox/home/go/bin/lefthook" <<'SH'
#!/bin/sh
printf '%s\n' "$@" > "$HOOK_TEST_LOG"
exit "${HOOK_TEST_STATUS:-0}"
SH
chmod +x "$sandbox/home/go/bin/lefthook"
cp "$sandbox/home/go/bin/lefthook" "$sandbox/bin/lefthook"
cp "$sandbox/home/go/bin/lefthook" "$sandbox/explicit tool"

for hook in pre-commit prepare-commit-msg; do
  for path in /usr/bin:/bin "$sandbox/bin:/usr/bin:/bin"; do
    env -i PATH="$path" HOME="$sandbox/home" HOOK_TEST_LOG="$sandbox/log" \
      /bin/sh "$hooks/$hook" 'argument with spaces'
    printf 'run\n--no-auto-install\n%s\nargument with spaces\n' "$hook" > "$sandbox/expected"
    cmp "$sandbox/expected" "$sandbox/log"
  done
  env -i PATH=/usr/bin:/bin LEFTHOOK_BIN="$sandbox/explicit tool" HOOK_TEST_LOG="$sandbox/log" \
    /bin/sh "$hooks/$hook"
  printf 'run\n--no-auto-install\n%s\n' "$hook" > "$sandbox/expected"
  cmp "$sandbox/expected" "$sandbox/log"

  status=0
  env -i PATH=/usr/bin:/bin LEFTHOOK_BIN="$sandbox/explicit tool" HOOK_TEST_LOG="$sandbox/log" \
    HOOK_TEST_STATUS=7 /bin/sh "$hooks/$hook" || status=$?
  test "$status" = 7

  status=0
  env -i PATH=/usr/bin:/bin LEFTHOOK_BIN="$sandbox/missing" \
    /bin/sh "$hooks/$hook" > "$sandbox/error" 2>&1 || status=$?
  test "$status" = 127
  grep -q 'Install it and run task hooks' "$sandbox/error"

  env -i PATH=/usr/bin:/bin LEFTHOOK=0 LEFTHOOK_BIN="$sandbox/missing" /bin/sh "$hooks/$hook"
done
printf '%s\n' 'Git hook tests passed.'
