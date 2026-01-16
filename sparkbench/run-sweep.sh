#!/bin/bash

# TPC-H Benchmark Sweep
# Runs benchmarks with varying cores and bandwidth configurations

SPARK_SHELL="docker exec -i spark-1 /opt/spark/bin/spark-shell"

run_benchmark() {
    local cores=$1
    local bandwidth=$2
    local output_dir="/data/output/cores${cores}_bw${bandwidth}"

    # Calculate burst as 10% of bandwidth
    # Handle both mbit and gbit units
    if [[ $bandwidth == *gbit ]]; then
        bw_num=${bandwidth%gbit}
        # bits/sec = bw_num * 1e9
        # bytes/sec = bits/sec / 8
        # burst = 10% of bytes/sec
        burst_bytes=$(awk "BEGIN {printf \"%d\", ($bw_num * 1000000000 / 8) * 0.1}")
    elif [[ $bandwidth == *mbit ]]; then
        bw_num=${bandwidth%mbit}
        # bits/sec = bw_num * 1e6
        burst_bytes=$(awk "BEGIN {printf \"%d\", ($bw_num * 1000000 / 8) * 0.1}")
    else
        echo "Unsupported bandwidth format: $bandwidth" >&2
        exit 1
    fi

    # Convert bytes to tc-friendly units (k/m), with a sane minimum
    if (( burst_bytes < 32768 )); then
        burst="32k"
    elif (( burst_bytes < 1048576 )); then
        burst="$((burst_bytes / 1024))k"
    else
        burst="$((burst_bytes / 1048576))m"
    fi

    echo "=========================================="
    echo "Running: cores=$cores, bandwidth=$bandwidth, burst=$burst"
    echo "Output:  $output_dir"
    echo "=========================================="

    # Set bandwidth
    for c in spark-1 spark-2 spark-3; do
        docker exec $c tc qdisc del dev eth0 root 2>/dev/null || true
        docker exec $c tc qdisc add dev eth0 root handle 1: htb default 10 r2q 1
        docker exec $c tc class add dev eth0 parent 1: classid 1:10 htb rate $bandwidth burst $burst cburst $burst quantum 1514
        docker exec $c tc qdisc add dev eth0 parent 1:10 handle 10: netem delay 100us
    done

    # Run benchmark with specified cores
    $SPARK_SHELL --conf spark.executor.cores=$cores <<EOF
:load /workloads/metrics2.scala
:load /workloads/tpch.scala
spark.sql("USE tpch")
executeAll("$output_dir", 2, 3)
:quit
EOF

    echo ""
}

# Sweep 2: Varying bandwidth with 2 cores (10mbit to 1gbit)
# for bw in 10mbit 25mbit 50mbit 100mbit 250mbit 500mbit 1gbit 10gbit 100gbit; do
for bw in 10mbit 100mbit 1gbit 10gbit 100gbit; do
    run_benchmark 2 "$bw"
done

# Sweep 1: Varying cores (1-4) with 1gbit
# for cores in 1 2 3 4; do
#     run_benchmark $cores "1gbit"
# done

# Disable shaping
for c in spark-1 spark-2 spark-3; do
    docker exec $c tc qdisc del dev eth0 root 2>/dev/null || true
done

echo "Sweep complete! Results in ./data/output/"
