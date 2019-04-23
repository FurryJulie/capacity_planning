#!/bin/sh

TMP_DIR="/tmp/capacity_planning"

HNAME="$(hostname).$(hostname -d)"
STATS_FILE="$TMP_DIR/$HNAME-host"
DATE="$(date '+%s')"

test -d "$TMP_DIR/stats" || mkdir -p "$TMP_DIR/stats"
test -d "$TMP_DIR/$HNAME-vms" || mkdir -p "$TMP_DIR/$HNAME-vms"

STATS_DIR="$TMP_DIR/$HNAME-vms"
touch "$STATS_DIR"/tmp

echo "time: $DATE" > "$STATS_FILE"

virsh -r nodeinfo >> "$STATS_FILE"

free -m >> "$STATS_FILE"

CMD=$(virsh -r list |awk '{print $1}'|grep -o '[0-9]\+')

rm -f  "$STATS_DIR"/*
for vm in $CMD
do
	virsh -r dominfo "$vm" > "$STATS_DIR/$vm"
done

