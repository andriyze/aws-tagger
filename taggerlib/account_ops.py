"""
AWS account identity and filename-safety helpers.
"""

import boto3


def sanitize_filename(value):
    """Strip characters unsafe for filenames, replacing them with hyphens."""
    return ''.join(
        c if c.isalnum() or c in ('-', '_') else '-'
        for c in value
    ).strip('-') or 'unknown-account'


def get_account_name(*, boto_config=None):
    """
    Return the AWS account alias if set, otherwise the numeric account ID.
    """
    account_id = 'unknown-account'
    try:
        sts = boto3.client('sts', config=boto_config)
        account_id = sts.get_caller_identity().get('Account', account_id)
    except Exception:
        pass
    try:
        iam = boto3.client('iam', config=boto_config)
        aliases = iam.list_account_aliases().get('AccountAliases', [])
        if aliases:
            return aliases[0]
    except Exception:
        pass
    return account_id
