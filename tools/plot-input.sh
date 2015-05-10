#!/bin/bash

set -e

function die() {
    echo "$@" >&2
    exit 1
}

INPUT_FILE="$1"
PLOT_FILE='plot.png'

[[ "$INPUT_FILE" ]] || INPUT_FILE='input.txt'
[[ -f "$INPUT_FILE" ]] || die "$INPUT_FILE does not exist"

echo "splitting input data"

TMPFILE="$(mktemp)"
CLUSTERS="$(awk '{print $1}' "$INPUT_FILE" | uniq)"

for CLUSTER in $CLUSTERS; do
    awk "\$1 == $CLUSTER" "$INPUT_FILE" > "$TMPFILE.$CLUSTER"
    echo "  '$TMPFILE.$CLUSTER' using 2:3 with points, \\" >>"$TMPFILE"
done

echo "generating gnuplot script"

cat >"$TMPFILE" <<EOF
set term png size 1920, 1080
set output '$PLOT_FILE'
plot \\
EOF

for CLUSTER in $CLUSTERS; do
    echo "  '$TMPFILE.$CLUSTER' using 2:3 with points, \\" >>"$TMPFILE"
done
echo "" >>"$TMPFILE"

echo "plotting $TMPFILE"
gnuplot "$TMPFILE"
echo "plot saved to $PLOT_FILE"
