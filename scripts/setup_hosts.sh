#!/usr/bin/env bash
# Usage: sudo bash scripts/setup_hosts.sh <node-number 1..4>
# Sets the hostname and maps node1..node4 to their ZeroTier IPs in /etc/hosts.
set -euo pipefail
N="${1:?give node number 1-4}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"; source "$DIR/cluster.env"

hostnamectl set-hostname "node$N"

# THE classic Hadoop bug: Ubuntu maps the hostname to 127.0.1.1, so the NameNode
# binds to loopback and DataNodes can't reach it. Remove that line.
sed -i '/^127\.0\.1\.1/d' /etc/hosts
sed -i '/ node[1-4]$/d' /etc/hosts      # drop any old node entries (re-runnable)

sed -e "s/__NODE1_IP__/$NODE1_IP/" -e "s/__NODE2_IP__/$NODE2_IP/" \
    -e "s/__NODE3_IP__/$NODE3_IP/" -e "s/__NODE4_IP__/$NODE4_IP/" \
    "$DIR/conf/hosts.snippet" | grep -v '^#' >> /etc/hosts

echo "== /etc/hosts now =="; cat /etc/hosts
for h in node1 node2 node3 node4; do
  ping -c1 -W2 "$h" >/dev/null && echo "  $h reachable" || echo "  $h NOT reachable (is it joined + authorized in ZeroTier?)"
done
