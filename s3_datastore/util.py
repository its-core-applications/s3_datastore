#!/usr/bin/env python3

from hashlib import sha512

import boto3
import requests


def add_common_args(parser):
    parser.add_argument('source')
    parser.add_argument('dest')
    parser.add_argument('--vault-url', default='https://vault.x.mail.umich.edu:8200')
    parser.add_argument('--vault-role')
    parser.add_argument('--s3-profile', default='datastore')
    parser.add_argument('--s3-region', default='us-east-2')
    parser.add_argument('--s3-bucket', default='datastore.x.mail.umich.edu')


def file_sha512(path):
    try:
        h = sha512()
        with open(path, 'rb') as f:
            while True:
                block = f.read(64 * 1024)
                if block:
                    h.update(block)
                else:
                    break
        return h.hexdigest()
    except OSError:
        pass
    return ''


def s3_client(args):
    kwargs = {
        'region_name': args.s3_region,
    }

    if args.vault_role:
        result = requests.post(
            f'{args.vault_url}/v1/auth/approle/login',
            json={'role_id': args.vault_role},
        )
        token = result.json()['auth']['client_token']
        result = requests.post(
            f'{args.vault_url}/v1/aws/sts/s3_datastore',
            json={'ttl': '15m'},
            headers={'Authorization': f'Bearer {token}'},
        ).json()
        kwargs['aws_access_key_id'] = result['data']['access_key']
        kwargs['aws_secret_access_key'] = result['data']['secret_key']
        kwargs['aws_session_token'] = result['data']['security_token']
    else:
        boto3.setup_default_session(profile_name=args.s3_profile)

    return boto3.client('s3', **kwargs)
