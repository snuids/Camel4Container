#!/bin/bash

echo 'Loading stats...'

# Get disk usage information
disks=$(df -h | awk 'NR>1 {print "{\"filesystem\":\""$1"\", \"size\":\""$2"\", \"used\":\""$3"\", \"avail\":\""$4"\", \"use%\":\""$5"\", \"mounted\":\""$6"\"}"}' | jq -s .)

# Get load averages
load=$(uptime | awk -F'[a-z]:' '{ print $2 }' | awk '{print "{\"1min\":\""$1"\", \"5min\":\""$2"\", \"15min\":\""$3"\"}"}' | jq .)

# Get Docker statistics
docker_stats=$(docker stats --no-stream --format '{"container":"{{.Container}}", "name":"{{.Name}}", "cpu":"{{.CPUPerc}}", "memory":"{{.MemUsage}}", "net_io":"{{.NetIO}}", "block_io":"{{.BlockIO}}", "pids":"{{.PIDs}}"}' | jq -s .)

# Hostname

hostname=$(printf '{"hostname": "%s"}' "$hostname" | jq .)

# Print the JSON output
echo "$hostname"
echo "$load"

# Combine disk usage, load averages, and Docker statistics into a single JSON object
output=$(jq -n --argjson hostname "$hostname" --argjson disks "$disks" --argjson load "$load" --argjson docker_stats "$docker_stats" '{disks: $disks, load: $load, docker_stats: $docker_stats, hostname: $hostname}')

# Print the combined JSON object
echo $output
echo $output > /tmp/monitor.json.tmp
mv /tmp/monitor.json.tmp /tmp/monitor.json
echo 'Done'

