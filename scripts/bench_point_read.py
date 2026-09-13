"""Measure ``GetItem`` against ``Query`` for a kydb point read.

``ak-server/optimisation_todo.md`` section 2 asks for a *controlled*
before/after measurement and explicitly refuses the 1.41 ms vs 2.21 ms
figure already in the plan, because that number compares different call
sites, different item sizes, and some reads that set
``ConsistentRead=True``.  This script removes all three variables: the
same item, the same table, the same client, alternating operations, and
no consistent reads on either side.

It needs a real table -- Moto answers from local memory, so it can only
tell you which code path ran, never what it costs.  Point it at a
disposable table, never a production one::

    PYTHONPATH=. AWS_PROFILE=<profile> AWS_DEFAULT_REGION=ap-northeast-1 \\
    python scripts/bench_point_read.py --table kydb-real-tests-YYYYMMDD

Reads and writes one object under ``/benchmarks/point_read/`` and deletes
it on the way out.  Operations alternate rather than running in two
blocks, so a drift in network latency over the run lands on both sides
equally instead of on whichever ran second.
"""
import argparse
import pickle
import statistics
import time

import boto3
from boto3.dynamodb.conditions import Key


KEY = '/benchmarks/point_read/obj'


def _percentile(values, fraction):
    ordered = sorted(values)
    index = min(int(fraction * len(ordered)), len(ordered) - 1)
    return ordered[index]


def _report(name, samples):
    ms = [s * 1000 for s in samples]
    print(f'{name:>8}: n={len(ms)} '
          f'mean={statistics.mean(ms):.3f}ms '
          f'p50={_percentile(ms, 0.50):.3f}ms '
          f'p90={_percentile(ms, 0.90):.3f}ms '
          f'p99={_percentile(ms, 0.99):.3f}ms')
    return statistics.mean(ms), _percentile(ms, 0.50)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', required=True,
                        help='table name; use a disposable one')
    parser.add_argument('--iterations', type=int, default=500)
    parser.add_argument('--warmup', type=int, default=25,
                        help='discarded iterations, to pay TCP/TLS setup '
                             'and the first-call botocore import cost '
                             'outside the measurement')
    parser.add_argument('--payload-bytes', type=int, default=1024,
                        help='approximate stored size; item size affects '
                             'both operations, so keep it fixed when '
                             'comparing runs')
    args = parser.parse_args()

    table = boto3.resource('dynamodb').Table(args.table)

    contents = pickle.dumps(b'x' * args.payload_bytes)
    table.put_item(Item={
        'path': KEY,
        'folder': KEY.rsplit('/', 1)[0] + '/',
        'contents': contents,
    })

    def do_get_item():
        return table.get_item(Key={'path': KEY})['Item']['contents'].value

    def do_query():
        return table.query(
            KeyConditionExpression=Key('path').eq(KEY)
        )['Items'][0]['contents'].value

    try:
        for _ in range(args.warmup):
            do_get_item()
            do_query()

        get_item_samples = []
        query_samples = []
        for iteration in range(args.iterations):
            # Alternate the order as well as the operations, so neither
            # one is always the second (warmer) call in a pair.
            operations = [(do_get_item, get_item_samples),
                          (do_query, query_samples)]
            if iteration % 2:
                operations.reverse()

            for operation, samples in operations:
                start = time.perf_counter()
                value = operation()
                samples.append(time.perf_counter() - start)
                assert value == contents

        print(f'table={args.table} payload={args.payload_bytes}B '
              f'iterations={args.iterations}')
        get_mean, get_p50 = _report('GetItem', get_item_samples)
        query_mean, query_p50 = _report('Query', query_samples)
        print(f'    diff: mean {query_mean - get_mean:+.3f}ms, '
              f'p50 {query_p50 - get_p50:+.3f}ms '
              f'(positive means GetItem is faster)')
    finally:
        table.delete_item(Key={'path': KEY})


if __name__ == '__main__':
    main()
