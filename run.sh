#!/bin/bash
# shellcheck disable=SC2120,SC2001,SC2004,SC2086

set -e

REPL="${REPL:-}"
ENABLE_HA="${ENABLE_HA:-}"

make cgo-go-install

for i in example*; do
    rm -rf "./$i"
done

mkdir -p example0
if [ -n "$ENABLE_HA" ]; then
    mkdir -p example1 example2
fi

function print {
    sed 's/^/    [+] /' $1
}

function header {
    sleep 1
    echo ""
    echo "# $1"
    echo ""
}

#
# Setup ha block
#
header "Setup cluster..."

nu-juju-data --api 127.0.0.1:8666 --db 127.0.0.1:9666 --dir example0 &
PID0=$!
if [ -n "$ENABLE_HA" ]; then
    nu-juju-data --api 127.0.0.1:8667 --db 127.0.0.1:9667 --join 127.0.0.1:9666 --dir example1 &
    PID1=$!
    nu-juju-data --api 127.0.0.1:8668 --db 127.0.0.1:9668 --join 127.0.0.1:9666 --dir example2 &
    PID2=$!
fi

sleep 6

function cleanup {
    kill $PID0
    if [ -n "$ENABLE_HA" ]; then
        kill $PID1 $PID2
    fi
}

function port {
    if [ -n "$ENABLE_HA" ]; then
        shuf -i 8666-8668 -n 1
        return
    fi
    echo "8666"
}

trap cleanup EXIT

# Show the repl if requested.
if [ -n "$REPL" ]; then 
    rlwrap -H ~/.dqlite_repl.history socat - ./example0/juju.sock
fi
