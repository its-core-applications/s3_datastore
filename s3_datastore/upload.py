#!/usr/bin/env python3

import argparse
import os
import socket
import sys

from datetime import datetime, timezone

from botocore.exceptions import ClientError

from .util import (
    add_common_args,
    file_sha512,
    s3_client,
)


def main():
    parser = argparse.ArgumentParser()
    add_common_args(parser)
    parser.add_argument('--heartbeat')
    parser.add_argument('--refresh', action='store_true')

    args = parser.parse_args()

    client = s3_client(args)

    localhash = file_sha512(args.source)
    if not localhash:
        print(f'{args.source} not found or not readable')
        sys.exit(1)

    if args.heartbeat:
        heartbeat = f'heartbeat_{args.heartbeat}'
        myname = socket.getfqdn()
        try:
            head = client.head_object(
                Bucket=args.s3_bucket,
                Key=heartbeat,
            )
        except ClientError:
            pass
        else:
            # Only take over if it's been more than 50 minutes since the
            # leader was alive.
            leader = head['Metadata']['leader']
            if (datetime.now(timezone.utc) - head['LastModified']).total_seconds() < 3000 and leader != myname:
                print(f'I am not the leader for {heartbeat}, {leader} is')
                sys.exit(0)

        client.upload_file(
            '/dev/null',
            args.s3_bucket,
            heartbeat,
            ExtraArgs={
                'ACL': 'public-read',
                'Metadata': {
                    'leader': myname,
                },
            }
        )

    try:
        head = client.head_object(
            Bucket=args.s3_bucket,
            Key=args.dest,
        )
    except ClientError:
        pass
    else:
        if head['Metadata']['sha512'] == localhash:
            print(f'{args.dest} is up to date')
            sys.exit(0)

        if args.refresh:
            s = os.stat(args.source)
            if head['LastModified'] > datetime.fromtimestamp(s.st_mtime, timezone.utc):
                print(f'{args.dest} is newer than {args.source}')
                sys.exit(0)

    client.upload_file(
        args.source,
        args.s3_bucket,
        args.dest,
        ExtraArgs={
            'Metadata': {
                'sha512': localhash,
            },
        }
    )
    print(f'{args.dest} updated')


if __name__ == '__main__':
    main()
