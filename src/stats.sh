#!/bin/sh

#MIT License
#
#Copyright (c) 2019 Julie Daligaud
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

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

