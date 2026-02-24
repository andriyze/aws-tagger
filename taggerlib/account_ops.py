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


def get_account_id(*, boto_config=None, strict=False):
    """
    Return numeric AWS account ID from sts:GetCallerIdentity.

    strict=True raises on lookup failures. strict=False returns
    'unknown-account' when identity cannot be determined.
    """
    try:
        sts = boto3.client('sts', config=boto_config)
        return sts.get_caller_identity().get('Account', 'unknown-account')
    except Exception:
        if strict:
            raise
        return 'unknown-account'


def get_account_identity(*, boto_config=None, strict=False):
    """
    Return account identity metadata:
      {
        'account_id': '123456789012',
        'account_alias': 'prod' or None,
        'account': alias_or_account_id
      }
    """
    account_id = get_account_id(boto_config=boto_config, strict=strict)
    account_alias = None
    try:
        iam = boto3.client('iam', config=boto_config)
        aliases = iam.list_account_aliases().get('AccountAliases', [])
        if aliases:
            account_alias = aliases[0]
    except Exception:
        pass
    return {
        'account_id': account_id,
        'account_alias': account_alias,
        'account': account_alias or account_id,
    }


def get_account_name(*, boto_config=None):
    """
    Return the AWS account alias if set, otherwise the numeric account ID.
    """
    return get_account_identity(boto_config=boto_config).get('account', 'unknown-account')
