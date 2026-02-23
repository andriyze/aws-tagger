"""
Tag write helpers and native tagging adapters for tagger.

Refactored to explicit dependency injection (no module-level runtime wiring).
"""

from collections import defaultdict
from threading import Lock
import time

import boto3
from botocore.exceptions import ClientError

from . import constants
from .utils import (
    arn_parts as _arn_parts,
    arn_resource_id as _arn_resource_id,
    arn_service as _default_arn_service,
    chunked as _default_chunked,
    s3_bucket_from_arn as _s3_bucket_from_arn,
    tagging_region_for_arn as _default_tagging_region_for_arn,
)


def _require_dependency(name, value):
    if value is None:
        raise TypeError(f"Missing required dependency: {name}")
    return value


def is_tagresources_unsupported_error(code, message):
    """
    Best-effort classifier for failures that indicate tag:TagResources cannot
    write the resource type and a service-native API is required.
    """
    code = (code or '').strip()
    msg = (message or '').lower()
    if not msg and code not in {'InvalidParameterException', 'ValidationException'}:
        return False

    patterns = (
        'not supported',
        'does not support',
        'unsupported resource type',
        'use the service',
        'service-specific',
        'native service api',
    )
    return (
        code in {'InvalidParameterException', 'ValidationException'}
        and any(p in msg for p in patterns)
    )


def _tag_dict_to_aws_tag_list(tags):
    """Convert {key:value} tags to AWS [{'Key','Value'}] list format."""
    return [{'Key': k, 'Value': v} for k, v in sorted(tags.items())]

_NATIVE_CLIENT_CACHE = {}
_NATIVE_CLIENT_LOCK = Lock()


def native_client(service, region, *, boto_config=None):
    """Create/cache boto3 clients used by native tag adapters."""
    key = (service, region)

    # Maintain per-(service,region) client lists keyed by config object identity.
    entries = _NATIVE_CLIENT_CACHE.get(key, [])
    for config_obj, client in entries:
        if config_obj is boto_config:
            return client

    # Thread-safe lazy initialization for future parallel adapter usage.
    with _NATIVE_CLIENT_LOCK:
        entries = _NATIVE_CLIENT_CACHE.setdefault(key, [])
        for config_obj, client in entries:
            if config_obj is boto_config:
                return client
        client = boto3.client(service, region_name=region, config=boto_config)
        entries.append((boto_config, client))
        return client


def clear_native_client_cache():
    """Reset native client cache (used by long-lived processes/tests)."""
    with _NATIVE_CLIENT_LOCK:
        _NATIVE_CLIENT_CACHE.clear()


# Backward-compatibility alias for existing callers.
_native_client = native_client


def _native_tag_ec2(arn, region, tags, *, native_client_fn):
    resource_id = _arn_resource_id(arn)
    if not resource_id:
        raise ValueError(f"Cannot parse EC2 resource ID from ARN: {arn}")
    client = native_client_fn('ec2', region)
    client.create_tags(Resources=[resource_id], Tags=_tag_dict_to_aws_tag_list(tags))


def _native_tag_s3_bucket(arn, _region, tags, *, native_client_fn, global_tagging_region):
    bucket = _s3_bucket_from_arn(arn)
    if not bucket:
        raise ValueError(f"S3 native adapter only supports bucket ARNs: {arn}")

    client = native_client_fn('s3', global_tagging_region)
    existing = {}
    try:
        response = client.get_bucket_tagging(Bucket=bucket)
        existing = {t['Key']: t['Value'] for t in response.get('TagSet', [])}
    except ClientError as e:
        err = e.response.get('Error', {})
        code = err.get('Code', '')
        if code not in {'NoSuchTagSet', 'NoSuchTagSetError'}:
            raise

    merged = dict(existing)
    merged.update(tags)
    client.put_bucket_tagging(Bucket=bucket, Tagging={'TagSet': _tag_dict_to_aws_tag_list(merged)})


def _native_tag_lambda(arn, region, tags, *, native_client_fn):
    client = native_client_fn('lambda', region)
    client.tag_resource(Resource=arn, Tags=tags)


def _native_tag_logs(arn, region, tags, *, native_client_fn):
    client = native_client_fn('logs', region)
    client.tag_resource(resourceArn=arn, tags=tags)


def _native_tag_rds(arn, region, tags, *, native_client_fn):
    client = native_client_fn('rds', region)
    client.add_tags_to_resource(ResourceName=arn, Tags=_tag_dict_to_aws_tag_list(tags))


def _native_tag_elbv2(arn, region, tags, *, native_client_fn):
    resource = _arn_parts(arn)[5]
    tag_list = _tag_dict_to_aws_tag_list(tags)

    # Classic ELB uses the "elb" API with load balancer names, not ARNs.
    if (
        resource.startswith('loadbalancer/')
        and not resource.startswith('loadbalancer/app/')
        and not resource.startswith('loadbalancer/net/')
        and not resource.startswith('loadbalancer/gwy/')
    ):
        lb_name = resource.split('/', 1)[1]
        if not lb_name:
            raise ValueError(f"Cannot parse Classic ELB name from ARN: {arn}")
        client = native_client_fn('elb', region)
        client.add_tags(LoadBalancerNames=[lb_name], Tags=tag_list)
        return

    client = native_client_fn('elbv2', region)
    client.add_tags(ResourceArns=[arn], Tags=tag_list)


def _native_tag_eks(arn, region, tags, *, native_client_fn):
    client = native_client_fn('eks', region)
    client.tag_resource(resourceArn=arn, tags=tags)


def _native_tag_dynamodb(arn, region, tags, *, native_client_fn):
    client = native_client_fn('dynamodb', region)
    client.tag_resource(ResourceArn=arn, Tags=_tag_dict_to_aws_tag_list(tags))


def has_native_tag_adapter(
    arn,
    *,
    arn_service=None,
    native_tag_adapter_services=None,
):
    """Return True if this ARN's service has a native fallback adapter."""
    arn_service = arn_service or _default_arn_service
    native_tag_adapter_services = set(
        native_tag_adapter_services or constants.NATIVE_TAG_ADAPTER_SERVICES
    )
    return arn_service(arn) in native_tag_adapter_services


def apply_native_tag_adapter(
    arn,
    tags,
    fallback_region=None,
    *,
    arn_service=None,
    tagging_region_for_arn=None,
    native_client_fn=None,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
):
    """
    Try service-native tagging API for a single ARN.
    Returns (ok: bool, error_message: str).
    """
    arn_service = arn_service or _default_arn_service
    tagging_region_for_arn = tagging_region_for_arn or _default_tagging_region_for_arn
    native_client_fn = _require_dependency('native_client_fn', native_client_fn)

    service = arn_service(arn)
    region = fallback_region or tagging_region_for_arn(arn)
    try:
        if service == 'ec2':
            _native_tag_ec2(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 's3':
            _native_tag_s3_bucket(
                arn,
                region,
                tags,
                native_client_fn=native_client_fn,
                global_tagging_region=global_tagging_region,
            )
        elif service == 'lambda':
            _native_tag_lambda(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 'logs':
            _native_tag_logs(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 'rds':
            _native_tag_rds(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 'elasticloadbalancing':
            _native_tag_elbv2(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 'eks':
            _native_tag_eks(arn, region, tags, native_client_fn=native_client_fn)
        elif service == 'dynamodb':
            _native_tag_dynamodb(arn, region, tags, native_client_fn=native_client_fn)
        else:
            return False, f"No native adapter for service '{service}'"
        return True, ''
    except Exception as e:
        return False, str(e)


def compute_tagging_decisions(resources, new_tags, replace_rules, force=False):
    """
    Evaluate tagging decisions for every resource without making any API calls.

    Returns a list of tuples:
      (resource, tags_to_set, conflicts, replace_applied)
    """
    decisions = []
    for r in resources:
        existing = r['tags']
        # Inherited tags are write candidates (child gaps only), but explicit
        # --tag/--replace arguments can override them.
        tags_to_set = dict(r.get('inherited_tags') or {})
        conflicts = {}
        replace_applied = {}

        for key, value in (new_tags or {}).items():
            if key in existing and not force:
                conflicts[key] = existing[key]
            else:
                tags_to_set[key] = value

        for key, old_value, new_value in (replace_rules or []):
            if existing.get(key) == old_value:
                tags_to_set[key] = new_value
                replace_applied[key] = (old_value, new_value)

        decisions.append((r, tags_to_set, conflicts, replace_applied))

    return decisions


def tag_resources(
    resources,
    new_tags,
    replace_rules,
    force=False,
    dry_run=False,
    native_tag_adapters=False,
    decisions=None,
    *,
    compute_tagging_decisions=None,
    phase,
    boto_config,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    tag_write_batch_size=10,
    tag_write_batch_delay=0.5,
    tag_write_max_retries=6,
    native_tag_adapter_services=None,
    retry_backoff_seconds,
    is_throttling_error,
    is_throttling_exception,
    arn_service,
    tagging_region_for_arn,
    chunked=_default_chunked,
):
    """
    Apply tagging operations to a list of discovered resources.

    new_tags:     dict of {key: value} to set. Skips existing keys unless --force.
    replace_rules: list of (key, old_value, new_value) tuples. Only fires when
                   the resource's current value for 'key' exactly matches old_value.
    force:        overwrite existing tag values (applies to new_tags only,
                  not replace_rules — replace_rules always require an exact match).
    dry_run:      print what would happen without making any AWS API calls.

    Write operations are batched (tag_write_batch_size ARNs per call) and
    grouped by (region, tag_set) to minimise API calls. Throttling is handled
    with exponential backoff up to tag_write_max_retries attempts.

    TAG/SKIP decisions are NOT printed here — call print_asset_decisions() first
    for a clean pre-write summary. This function prints only write-phase events
    (region start, retry, failure, final count).
    """
    phase = _require_dependency('phase', phase)
    retry_backoff_seconds = _require_dependency('retry_backoff_seconds', retry_backoff_seconds)
    is_throttling_error = _require_dependency('is_throttling_error', is_throttling_error)
    is_throttling_exception = _require_dependency('is_throttling_exception', is_throttling_exception)
    arn_service = arn_service or _default_arn_service
    tagging_region_for_arn = tagging_region_for_arn or _default_tagging_region_for_arn
    chunked = chunked or _default_chunked

    native_tag_adapter_services = set(
        native_tag_adapter_services or constants.NATIVE_TAG_ADAPTER_SERVICES
    )

    if decisions is None:
        compute_tagging_decisions = _require_dependency(
            'compute_tagging_decisions', compute_tagging_decisions
        )
        decisions = compute_tagging_decisions(resources, new_tags, replace_rules, force)

    # Build write map: {region: {tag_tuple: [arns]}}
    to_tag = {}
    skipped = 0

    for r, tags_to_set, _conflicts, _replace_applied in decisions:
        if not tags_to_set:
            skipped += 1
            continue
        arn = r['arn']
        region = r['region'] or global_tagging_region
        tag_key = tuple(sorted(tags_to_set.items()))
        to_tag.setdefault(region, {}).setdefault(tag_key, []).append(arn)

    total_to_tag = len(resources) - skipped

    if dry_run:
        print("DRY RUN — no changes made")
        return

    if total_to_tag == 0:
        print("Nothing to tag.")
        return

    phase("Tag Writes")

    failed_count = 0
    success_count = 0
    unsupported_by_service = defaultdict(int)
    remaining_failed_by_service = defaultdict(int)
    native_adapter_attempted = 0
    native_adapter_succeeded = 0
    native_adapter_failed = 0

    native_client_fn = lambda service, region: _native_client(
        service,
        region,
        boto_config=boto_config,
    )

    for region, groups in to_tag.items():
        total = sum(len(arns) for arns in groups.values())
        print(f"\nTagging {total} resources in {region}...")
        client = boto3.client('resourcegroupstaggingapi', region_name=region, config=boto_config)

        for tag_key, arns in groups.items():
            tags = dict(tag_key)
            for batch in chunked(arns, tag_write_batch_size):
                pending = list(batch)
                attempt = 0

                while pending:
                    try:
                        response = client.tag_resources(ResourceARNList=pending, Tags=tags)
                        failed = response.get('FailedResourcesMap', {})

                        if not failed:
                            success_count += len(pending)
                            pending = []
                            break

                        success_count += len(pending) - len(failed)
                        throttled = []
                        non_throttled = []
                        for fail_arn, error in failed.items():
                            code = error.get('ErrorCode', '')
                            msg = error.get('ErrorMessage', '')
                            if is_throttling_error(code, msg):
                                throttled.append(fail_arn)
                            else:
                                non_throttled.append((fail_arn, error))

                        for fail_arn, error in non_throttled:
                            code = error.get('ErrorCode', '')
                            msg = error.get('ErrorMessage', 'Unknown')
                            service = arn_service(fail_arn)
                            unsupported = is_tagresources_unsupported_error(code, msg)
                            if unsupported:
                                unsupported_by_service[service] += 1

                            # Optional fallback: retry non-throttled unsupported
                            # failures with service-native tagging APIs.
                            should_try_native = (
                                native_tag_adapters
                                and has_native_tag_adapter(
                                    fail_arn,
                                    arn_service=arn_service,
                                    native_tag_adapter_services=native_tag_adapter_services,
                                )
                                and (unsupported or code in {'InvalidParameterException', 'ValidationException'})
                            )
                            if should_try_native:
                                native_adapter_attempted += 1
                                ok, native_err = apply_native_tag_adapter(
                                    fail_arn,
                                    tags,
                                    fallback_region=region,
                                    arn_service=arn_service,
                                    tagging_region_for_arn=tagging_region_for_arn,
                                    native_client_fn=native_client_fn,
                                    global_tagging_region=global_tagging_region,
                                )
                                if ok:
                                    native_adapter_succeeded += 1
                                    success_count += 1
                                    print(f"  NATIVE OK  {fail_arn}")
                                    continue
                                native_adapter_failed += 1
                                msg = f"{msg}; native fallback failed: {native_err}"

                            print(f"  FAIL  {fail_arn} — {msg}")
                            failed_count += 1
                            remaining_failed_by_service[service] += 1

                        if throttled:
                            attempt += 1
                            if attempt > tag_write_max_retries:
                                for fail_arn in throttled:
                                    print(f"  FAIL  {fail_arn} — throttled after {tag_write_max_retries} retries")
                                    failed_count += 1
                                pending = []
                            else:
                                sleep_s = retry_backoff_seconds(attempt)
                                print(
                                    f"  RETRY {len(throttled)} throttled resources in "
                                    f"{sleep_s:.2f}s (attempt {attempt}/{tag_write_max_retries})"
                                )
                                time.sleep(sleep_s)
                                pending = throttled
                        else:
                            pending = []

                    except Exception as e:
                        if is_throttling_exception(e):
                            attempt += 1
                            if attempt > tag_write_max_retries:
                                print(f"  ERROR batch ({len(pending)} resources) — {e}")
                                failed_count += len(pending)
                                pending = []
                            else:
                                sleep_s = retry_backoff_seconds(attempt)
                                print(
                                    f"  RETRY batch ({len(pending)} resources) after throttling in "
                                    f"{sleep_s:.2f}s (attempt {attempt}/{tag_write_max_retries})"
                                )
                                time.sleep(sleep_s)
                        else:
                            print(f"  ERROR batch ({len(pending)} resources) — {e}")
                            failed_count += len(pending)
                            pending = []

                time.sleep(tag_write_batch_delay)

    print(f"\nTagging complete: {success_count} succeeded, {failed_count} failed.")
    unsupported_total = sum(unsupported_by_service.values())
    if unsupported_total:
        print(
            f"  TagResources unsupported: {unsupported_total} resources across "
            f"{len(unsupported_by_service)} services"
        )
        if native_tag_adapters:
            print(
                f"  Native adapter fallback: attempted {native_adapter_attempted}, "
                f"succeeded {native_adapter_succeeded}, failed {native_adapter_failed}"
            )
        else:
            print("  Hint: rerun with --native-tag-adapters to try service-native API fallbacks.")

        top = ", ".join(
            f"{svc}={count}" for svc, count in
            sorted(unsupported_by_service.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        )
        if top:
            print(f"  Unsupported by service (top): {top}")

    if remaining_failed_by_service:
        top = ", ".join(
            f"{svc}={count}" for svc, count in
            sorted(remaining_failed_by_service.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
        )
        print(f"  Remaining write failures by service (top): {top}")

    return {
        'success_count': success_count,
        'failed_count': failed_count,
        'unsupported_by_service': dict(unsupported_by_service),
        'native_adapter_attempted': native_adapter_attempted,
        'native_adapter_succeeded': native_adapter_succeeded,
        'native_adapter_failed': native_adapter_failed,
        'remaining_failed_by_service': dict(remaining_failed_by_service),
    }


def compute_removal_decisions(resources, remove_keys):
    """
    Return list of (resource, keys_to_remove) for resources that currently have
    at least one key from remove_keys. Resources without any matching key are omitted.
    """
    remove_set = set(remove_keys)
    result = []
    for r in resources:
        present = [k for k in r['tags'] if k in remove_set]
        if present:
            result.append((r, present))
    return result


def remove_tags_from_resources(
    resources,
    remove_keys,
    dry_run=False,
    *,
    phase,
    boto_config,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    tag_write_batch_size=10,
    tag_write_batch_delay=0.5,
    tag_write_max_retries=6,
    retry_backoff_seconds,
    is_throttling_error,
    is_throttling_exception,
    chunked=_default_chunked,
):
    """
    Remove specified tag keys from all resources that have them.
    Uses the same batching and retry infrastructure as tag_resources().
    """
    phase = _require_dependency('phase', phase)
    retry_backoff_seconds = _require_dependency('retry_backoff_seconds', retry_backoff_seconds)
    is_throttling_error = _require_dependency('is_throttling_error', is_throttling_error)
    is_throttling_exception = _require_dependency('is_throttling_exception', is_throttling_exception)
    chunked = chunked or _default_chunked

    decisions = compute_removal_decisions(resources, remove_keys)

    phase("Removal Decisions")
    remove_count = 0
    keep_count = 0
    for r, keys_present in decisions:
        print(f"  REMOVE  {r['arn']}  {', '.join(keys_present)}")
        remove_count += 1
    for r in resources:
        remove_set = set(remove_keys)
        if not any(k in remove_set for k in r['tags']):
            keep_count += 1

    print(f"\n  Total: {len(resources)} resources  |  "
          f"{remove_count} will have tags removed  |  {keep_count} unchanged")

    if dry_run:
        print("\nDRY RUN — no changes made")
        return {
            'targeted': remove_count,
            'unchanged': keep_count,
            'success_count': 0,
            'failed_count': 0,
        }

    if remove_count == 0:
        print("Nothing to remove.")
        return {
            'targeted': 0,
            'unchanged': keep_count,
            'success_count': 0,
            'failed_count': 0,
        }

    phase("Tag Removals")

    # Group by region
    by_region = {}
    for r, keys_present in decisions:
        region = r['region'] or global_tagging_region
        by_region.setdefault(region, []).append((r['arn'], keys_present))

    failed_count = 0
    success_count = 0

    for region, arn_keys in by_region.items():
        print(f"\nRemoving tags from {len(arn_keys)} resources in {region}...")
        client = boto3.client('resourcegroupstaggingapi', region_name=region, config=boto_config)

        # Group ARNs by the exact set of keys to remove (minimise API calls)
        key_groups = {}
        for arn, keys in arn_keys:
            key_tuple = tuple(sorted(keys))
            key_groups.setdefault(key_tuple, []).append(arn)

        for key_tuple, arns in key_groups.items():
            tag_keys_list = list(key_tuple)
            for batch in chunked(arns, tag_write_batch_size):
                pending = list(batch)
                attempt = 0

                while pending:
                    try:
                        response = client.untag_resources(
                            ResourceARNList=pending, TagKeys=tag_keys_list
                        )
                        failed = response.get('FailedResourcesMap', {})

                        if not failed:
                            success_count += len(pending)
                            pending = []
                            break

                        success_count += len(pending) - len(failed)
                        throttled = []
                        for fail_arn, error in failed.items():
                            code = error.get('ErrorCode', '')
                            msg = error.get('ErrorMessage', '')
                            if is_throttling_error(code, msg):
                                throttled.append(fail_arn)
                            else:
                                print(f"  FAIL  {fail_arn} — {msg}")
                                failed_count += 1

                        if throttled:
                            attempt += 1
                            if attempt > tag_write_max_retries:
                                for fail_arn in throttled:
                                    print(f"  FAIL  {fail_arn} — throttled after {tag_write_max_retries} retries")
                                    failed_count += 1
                                pending = []
                            else:
                                sleep_s = retry_backoff_seconds(attempt)
                                print(f"  RETRY {len(throttled)} throttled resources in "
                                      f"{sleep_s:.2f}s (attempt {attempt}/{tag_write_max_retries})")
                                time.sleep(sleep_s)
                                pending = throttled
                        else:
                            pending = []

                    except Exception as e:
                        if is_throttling_exception(e):
                            attempt += 1
                            if attempt > tag_write_max_retries:
                                print(f"  ERROR batch ({len(pending)} resources) — {e}")
                                failed_count += len(pending)
                                pending = []
                            else:
                                sleep_s = retry_backoff_seconds(attempt)
                                print(f"  RETRY batch ({len(pending)} resources) after throttling in "
                                      f"{sleep_s:.2f}s (attempt {attempt}/{tag_write_max_retries})")
                                time.sleep(sleep_s)
                        else:
                            print(f"  ERROR batch ({len(pending)} resources) — {e}")
                            failed_count += len(pending)
                            pending = []

                time.sleep(tag_write_batch_delay)

    print(f"\nRemoval complete: {success_count} succeeded, {failed_count} failed.")
    return {
        'targeted': remove_count,
        'unchanged': keep_count,
        'success_count': success_count,
        'failed_count': failed_count,
    }
