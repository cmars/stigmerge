#!/usr/bin/env bash

cd $(dirname $0)/..

set -euxo pipefail

# Delay increments by one until the max poll delay is reached
# So a max poll delay of 30 will allow an overall timeout of
# 1+2+...+25 = 300s
max_poll_delay=25

seed_dir=$(mktemp -d)
fetch_dir=$(mktemp -d)
trap "rm -rf ${seed_dir} ${fetch_dir}" EXIT

cargo build
stigmerge=$(pwd)/target/debug/stigmerge
head -c 2097153 /dev/urandom > ${seed_dir}/testfile
digest=$(sha256sum ${seed_dir}/testfile | cut -d' ' -f1)

function run_seed {
    cd ${seed_dir}
    ${stigmerge} --no-ui --state-dir ${seed_dir}/state seed testfile 2>&1 > seed.log
}

function get_seed_key {
    cat ${seed_dir}/state/share_key
}

run_seed &
seed_pid=$!
trap "kill ${seed_pid}; rm -rf ${seed_dir} ${fetch_dir}" EXIT

for i in $(seq $max_poll_delay); do
    seed_key=$(get_seed_key || true)
    if [ -n "${seed_key}" ]; then
        break
    fi
    sleep ${i}
done

if [ -z "${seed_key}" ]; then
    cat ${seed_dir}/seed.log
    echo "failed to get seed key"
    exit 1
else
    echo "got seed key ${seed_key}"
fi

(cd ${fetch_dir}; ${stigmerge} --no-ui --state-dir ${fetch_dir}/state fetch ${seed_key}) &
fetch_pid=$!
trap "kill ${seed_pid} ${fetch_pid}; rm -rf ${seed_dir} ${fetch_dir}" EXIT

for i in $(seq $max_poll_delay); do
    if [ -e ${fetch_dir}/testfile ]; then
        fetch_digest=$(sha256sum ${fetch_dir}/testfile | cut -d' ' -f1)
        if [ "${digest}" = "${fetch_digest}" ]; then
            break
        fi
    fi
    sleep ${i}
done

[ "${digest}" = "${fetch_digest}" ]
