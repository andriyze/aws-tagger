"""
Shared utility helpers for tagger modules.
"""

import re

from . import constants


def chunked(items, size):
    """Yield successive fixed-size chunks from a list."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def arn_parts(arn):
    """Split an ARN into 6 standard components, padding with empty strings."""
    parts = (arn or '').split(':', 5)
    while len(parts) < 6:
        parts.append('')
    return parts


def arn_service(arn):
    """Extract AWS service segment from an ARN."""
    parts = arn_parts(arn)
    if parts[0] == 'arn' and parts[2]:
        return parts[2]
    return 'unknown'


def arn_resource_id(arn):
    """
    Extract resource identifier from ARN resource component.
    Examples:
      arn:...:instance/i-123 -> i-123
      arn:...:db:mydb        -> mydb
    """
    resource = arn_parts(arn)[5]
    if '/' in resource:
        return resource.split('/', 1)[1]
    if ':' in resource:
        return resource.split(':', 1)[1]
    return resource or None


def s3_bucket_from_arn(arn):
    """Return plain bucket name for arn:...:s3:::bucket ARNs, else None."""
    parts = arn_parts(arn)
    if parts[2] != 's3':
        return None
    bucket = parts[5]
    if not bucket or '/' in bucket or ':' in bucket:
        return None
    return bucket


def unique_nonempty(values):
    """Return deduplicated values preserving order, removing empty entries."""
    out = []
    seen = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def extract_first_id(text, pattern):
    """Return first regex match from text, or None."""
    if not text:
        return None
    match = re.search(pattern, text)
    return match.group(0) if match else None


def is_verbose(verbose):
    """Resolve a verbose value from bool or callable."""
    if callable(verbose):
        try:
            return bool(verbose())
        except Exception:
            return False
    return bool(verbose)


def tagging_region_for_arn(
    arn,
    *,
    default_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    global_overrides=None,
):
    """
    Resolve which region to use for Tagging API calls for a given ARN.
    Global ARNs route through per-service override map when configured.
    """
    parts = arn_parts(arn)
    service = parts[2]
    arn_region = parts[3]
    overrides = (
        constants.GLOBAL_TAGGING_REGION_OVERRIDES
        if global_overrides is None
        else global_overrides
    )
    if not arn_region or arn_region == 'global':
        return (overrides or {}).get(service, default_region)
    return arn_region
