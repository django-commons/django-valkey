#!/bin/sh
# Six-node Valkey cluster (three primaries, three replicas) in a single
# container, on the official valkey/valkey image.
#
# Every node announces 127.0.0.1. The test client on the host reaches the nodes
# through the published ports 7000-7005, and the cluster redirects it (MOVED,
# CLUSTER SLOTS) to 127.0.0.1:700x, which the host can also reach. Inside the
# container the six processes share one loopback, so the same addresses work
# for the cluster bus (ports 17000-17005), which is never published.
#
# Usage: cluster.sh [BASE_PORT]   (default 7000; the nodes use BASE_PORT..+5)
set -eu

BASE_PORT="${1:-7000}"
PORTS=""
for i in 0 1 2 3 4 5; do
    PORTS="$PORTS $((BASE_PORT + i))"
done

for port in $PORTS; do
    mkdir -p "/data/$port"
    valkey-server \
        --port "$port" \
        --dir "/data/$port" \
        --cluster-enabled yes \
        --cluster-config-file nodes.conf \
        --cluster-node-timeout 5000 \
        --cluster-announce-ip 127.0.0.1 \
        --protected-mode no \
        --save "" \
        --appendonly no &
done

for port in $PORTS; do
    until valkey-cli -p "$port" ping >/dev/null 2>&1; do
        sleep 0.2
    done
done

nodes=""
for port in $PORTS; do
    nodes="$nodes 127.0.0.1:$port"
done
# shellcheck disable=SC2086 # word splitting is the point
valkey-cli --cluster create $nodes --cluster-replicas 1 --cluster-yes

# Keep the container alive for as long as the nodes are. The healthcheck in
# compose.yaml watches cluster_state, so a dead node still surfaces.
wait
