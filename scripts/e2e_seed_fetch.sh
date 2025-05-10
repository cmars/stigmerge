#!/usr/bin/env bash

cd $(dirname $0)/..

set -euxo pipefail

seed_dir=$(mktemp -d)
fetch_dir=$(mktemp -d)
trap "rm -rf ${seed_dir} ${fetch_dir}" EXIT

cargo build
stigmerge=$(pwd)/target/debug/stigmerge
head -c 2097153 /dev/urandom > ${seed_dir}/testfile
digest=$(sha256sum ${seed_dir}/testfile | cut -d' ' -f1)

function run_seed {
    cd ${seed_dir}
    ${stigmerge} --no-ui seed testfile 2>&1 > seed.log
}

function get_seed_key {
    awk '/announced share, key: /' ${seed_dir}/seed.log | sed 's/.* announced share, key: //;'
}

run_seed &
seed_pid=$!
trap "kill ${seed_pid}; rm -rf ${seed_dir} ${fetch_dir}" EXIT

for i in {1..20}; do
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

(cd ${fetch_dir}; ${stigmerge} --no-ui fetch ${seed_key}) &
fetch_pid=$!
trap "kill ${seed_pid} ${fetch_pid}; rm -rf ${seed_dir} ${fetch_dir}" EXIT

for i in {1..20}; do
    if [ -e ${fetch_dir}/testfile ]; then
        fetch_digest=$(sha256sum ${fetch_dir}/testfile | cut -d' ' -f1)
        if [ "${digest}" = "${fetch_digest}" ]; then
            break
        fi
    fi
    sleep ${i}
done

[ "${digest}" = "${fetch_digest}" ]
