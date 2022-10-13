#!/usr/bin/env python3

import argparse
import fcntl
import os
import sys
import tempfile

from hashlib import sha512

from .util import (
    add_common_args,
    file_sha512,
    s3_client,
)


def s3_fetch(client, bucket, source, dest):
    obj = client.get_object(
        Bucket=bucket,
        Key=source,
    )

    s3_hash = obj['Metadata']['sha512']

    local_hash = file_sha512(dest)

    if local_hash == s3_hash:
        print(f'{dest} is up to date')
        return {'changed': False, 'error': False}

    dir_name = os.path.dirname(dest)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name, mode=0o755)

    with tempfile.NamedTemporaryFile(
        dir=dir_name,
        prefix=os.path.basename(dest) + '.',
        suffix='.tmp'
    ) as f:
        h = sha512()
        for chunk in obj['Body'].iter_chunks(chunk_size=1024 * 1024):
            f.write(chunk)
            h.update(bytearray(chunk))

        new_hash = h.hexdigest()
        if new_hash != s3_hash:
            print(f'{source} failed verification: sha512 is {new_hash}, expected {s3_hash}')
            return {'changed': False, 'error': True}

        f.flush()
        os.fchmod(f.fileno(), 0o644)
        os.fsync(f.fileno())
        os.replace(f.name, dest)
        # relink it so the context manager can unlink it
        os.link(dest, f.name)

        print(f'{dest} updated')
        return {'changed': True, 'error': False}


def main():
    parser = argparse.ArgumentParser()
    add_common_args(parser)
    parser.add_argument('--lockfile')
    parser.add_argument('--recurse', '-r', action='store_true')

    args = parser.parse_args()

    if args.lockfile:
        try:
            f = open(args.lockfile, 'wb')
            fcntl.flock(f)
        except OSError:
            sys.exit(1)

    client = s3_client(args)

    failed = False
    if args.recurse:
        result = client.list_objects_v2(
            Bucket=args.s3_bucket,
            Prefix=args.source,
        )
        for obj in result['Contents']:
            failed = s3_fetch(
                client,
                args.s3_bucket,
                obj['Key'],
                os.path.join(
                    args.dest,
                    obj['Key'][len(args.source):].lstrip('/'),
                ),
            )['error'] or failed
    else:
        failed = s3_fetch(client, args.s3_bucket, args.source, args.dest)['error']

    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
