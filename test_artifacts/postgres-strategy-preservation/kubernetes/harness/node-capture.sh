#!/bin/sh
# Capture service bytes only for UIDs admitted in this campaign namespace.
set -eu
destination=/tmp/dpone-pg-preserve-ec30
mkdir -p "$destination/allowed"
while [ ! -e "$destination/stop" ]; do
    for allowed in "$destination"/allowed/*; do
        [ -d "$allowed" ] || continue
        uid=${allowed##*/}
        base=/var/lib/kubelet/pods/$uid/volumes/kubernetes.io~empty-dir
        mkdir -p "$destination/$uid"
        for name in runtime-evidence.json runtime-stderr.log runtime-startup-error.json; do
            source=$base/dpone-run-output/$name
            [ -f "$source" ] || continue
            if cp -p "$source" "$destination/$uid/$name.tmp" 2>/dev/null; then
                mv "$destination/$uid/$name.tmp" "$destination/$uid/$name"
            fi
        done
        source=$base/xcom/return.json
        if [ -f "$source" ] && cp -p "$source" "$destination/$uid/return.json.tmp" 2>/dev/null; then
            mv "$destination/$uid/return.json.tmp" "$destination/$uid/return.json"
        fi
    done
    sleep 0.03
done
