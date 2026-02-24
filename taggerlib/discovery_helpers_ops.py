"""
Resource discovery helper operations for tagger.

Refactored to explicit dependency injection (no module-level runtime wiring).
"""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import boto3

from . import constants
from .utils import (
    arn_service as _default_arn_service,
    chunked as _default_chunked,
    is_verbose as _is_verbose,
    tagging_region_for_arn as _default_tagging_region_for_arn,
    unique_nonempty as _default_unique_nonempty,
)


def _default_retry_backoff_seconds(attempt):
    # Conservative fallback; caller should pass production backoff.
    return min(8.0, 0.5 * (2 ** max(0, attempt - 1)))


def _default_is_throttling_exception(_exc):
    return False


def _default_is_access_denied_exception(_exc):
    return False


def re_search(
    client,
    query,
    view_arn=None,
    *,
    re_query_delay=constants.DEFAULT_RE_QUERY_DELAY,
):
    """
    Execute a single Resource Explorer search, paginating through all pages.

    Returns (results, is_complete, total_estimate).
    """
    resources = []
    next_token = None
    is_complete = True
    total = None

    while True:
        params = {'QueryString': query, 'MaxResults': 1000}
        if next_token:
            params['NextToken'] = next_token
        if view_arn:
            params['ViewArn'] = view_arn

        response = client.search(**params)
        resources.extend(response.get('Resources', []))
        time.sleep(re_query_delay)

        if total is None:
            count = response.get('Count', {})
            is_complete = count.get('Complete', True)
            total = count.get('TotalResources', 0)

        next_token = response.get('NextToken')
        if not next_token:
            break

    return resources, is_complete, total or len(resources)


def re_search_with_splits(
    client,
    resource_type,
    region,
    view_arn,
    stats,
    *,
    split_tag_keys,
    verbose=False,
    re_query_delay=constants.DEFAULT_RE_QUERY_DELAY,
    re_search_fn=None,
):
    """
    Search RE for a (resource_type, region) pair, splitting when capped.
    """
    split_tag_keys = list(split_tag_keys or [])

    if re_search_fn is None:
        def re_search_fn(query, search_view_arn):
            return re_search(
                client,
                query,
                view_arn=search_view_arn,
                re_query_delay=re_query_delay,
            )

    if resource_type:
        base = f"resourcetype:{resource_type} region:{region}"
        context = f"{resource_type} in {region}"
    else:
        base = f"region:{region}"
        context = f"all types in {region}"

    results, complete, total = re_search_fn(base, view_arn)
    if complete:
        return results

    if _is_verbose(verbose):
        print(f"    Splitting {context} (est. {total} resources)...")

    all_results = []

    r_none, c_none, t_none = re_search_fn(f"{base} tag:none", view_arn)
    all_results.extend(r_none)
    if not c_none:
        stats['warnings'].append(f"{context} untagged: {t_none} est, got {len(r_none)} (capped)")

    r_tagged, c_tagged, t_tagged = re_search_fn(f"{base} -tag:none", view_arn)
    if c_tagged:
        all_results.extend(r_tagged)
        return all_results

    if _is_verbose(verbose):
        print(f"    Further splitting tagged {context} (est. {t_tagged})...")
    seen_arns = set()
    exclusion_parts = []

    for tag_key in split_tag_keys:
        r_with, c_with, _ = re_search_fn(f"{base} tag:{tag_key}", view_arn)
        for r in r_with:
            arn = r.get('Arn')
            if arn and arn not in seen_arns:
                seen_arns.add(arn)
                all_results.append(r)
        exclusion_parts.append(f"-tag:{tag_key}")

        if not c_with:
            stats['warnings'].append(
                f"{context} tag:{tag_key}: results capped at 1000")

    remaining_query = f"{base} -tag:none " + " ".join(exclusion_parts)
    r_rest, c_rest, t_rest = re_search_fn(remaining_query, view_arn)
    for r in r_rest:
        arn = r.get('Arn')
        if arn and arn not in seen_arns:
            seen_arns.add(arn)
            all_results.append(r)

    if not c_rest:
        stats['warnings'].append(
            f"{context} remaining tagged: {t_rest} est, got {len(r_rest)} (capped)")

    return all_results


def re_auto_setup(re_region, regions, *, boto_config=None):
    """
    Ensure RE is configured with an aggregator index and default view.
    """
    try:
        client = boto3.client('resource-explorer-2', region_name=re_region, config=boto_config)
        print(f"Running CreateResourceExplorerSetup (aggregator: {re_region})...")
        response = client.create_resource_explorer_setup(
            RegionList=regions,
            ViewName='default-tagger-view',
            AggregatorRegions=[re_region],
        )
        task_id = response.get('TaskId', 'unknown')
        print(f"  Setup initiated (task: {task_id}). Indexing may take up to 36 hours.")
        return True
    except Exception as e:
        if 'ConflictException' in type(e).__name__ or 'Conflict' in str(e):
            print("  Resource Explorer already configured — skipping setup.")
            return True
        err_str = str(e)
        if 'AccessDenied' in err_str or 'CreateServiceLinkedRole' in err_str or 'UnauthorizedAccess' in err_str:
            print("  ERROR RE auto-setup failed — missing IAM permission: iam:CreateServiceLinkedRole")
            print(f"  Detail: {e}")
            print("  Add iam:CreateServiceLinkedRole to your role, or run without --auto-setup.")
            print("  Without RE indexing, untagged resources may not be discovered.")
        else:
            print(f"  WARN  RE auto-setup failed: {e}")
        print("  Continuing with existing configuration.")
        return False


def re_get_indexed_regions(re_region, *, boto_config=None):
    """
    Return set of regions with active RE indexes, or None on failure.
    """
    try:
        client = boto3.client('resource-explorer-2', region_name=re_region, config=boto_config)
        regions = set()
        next_token = None
        while True:
            params = {}
            if next_token:
                params['NextToken'] = next_token
            response = client.list_indexes(**params)
            for idx in response.get('Indexes', []):
                arn = idx.get('Arn', '')
                idx_parts = arn.split(':')
                if len(idx_parts) > 3:
                    regions.add(idx_parts[3])
            next_token = response.get('NextToken')
            if not next_token:
                break
        return regions
    except Exception as e:
        print(f"  WARN  Unable to list RE indexes: {e}")
        return None


def re_list_supported_resource_types(re_region, *, boto_config=None):
    """
    Return (supported_types_set, error_string_or_none) for RE supported types.
    """
    try:
        client = boto3.client('resource-explorer-2', region_name=re_region, config=boto_config)
        supported = set()
        next_token = None
        while True:
            params = {'MaxResults': 1000}
            if next_token:
                params['NextToken'] = next_token
            response = client.list_supported_resource_types(**params)
            for entry in response.get('ResourceTypes', []):
                rt = entry.get('ResourceType')
                if rt:
                    supported.add(rt)
            next_token = response.get('NextToken')
            if not next_token:
                break
        return supported, None
    except Exception as e:
        return set(), str(e)


def build_re_type_coverage(
    re_region,
    configured_re_types,
    *,
    re_list_supported_resource_types_fn=None,
    boto_config=None,
):
    """
    Compare configured RE types against AWS-supported RE types.
    """
    if re_list_supported_resource_types_fn is None:
        def re_list_supported_resource_types_fn(region):
            return re_list_supported_resource_types(region, boto_config=boto_config)

    supported, error = re_list_supported_resource_types_fn(re_region)
    summary = {
        'error': error,
        'supported_count': len(supported),
    }

    if configured_re_types is None:
        summary['mode'] = 'all-types'
        summary['configured_count'] = None
        summary['missing_from_config'] = []
        summary['unknown_in_config'] = []
        return summary

    configured = set(configured_re_types or [])
    summary['mode'] = 'curated-types'
    summary['configured_count'] = len(configured)
    summary['missing_from_config'] = sorted(supported - configured)
    summary['unknown_in_config'] = sorted(configured - supported)
    return summary


def discover_via_resource_explorer(
    regions,
    re_types,
    re_region,
    view_arn=None,
    *,
    boto_config=None,
    re_query_delay=constants.DEFAULT_RE_QUERY_DELAY,
    split_tag_keys=None,
    global_resource_types=None,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    global_tagging_region_overrides=None,
    arn_service=None,
    verbose=False,
    re_get_indexed_regions_fn=None,
    re_search_with_splits_fn=None,
):
    """
    Discover resources via Resource Explorer.
    Returns (items, stats).
    """
    split_tag_keys = list(split_tag_keys or [])
    global_resource_types = set(global_resource_types or [])
    global_tagging_region_overrides = dict(global_tagging_region_overrides or {})
    arn_service = arn_service or _default_arn_service

    if re_get_indexed_regions_fn is None:
        def re_get_indexed_regions_fn(region):
            return re_get_indexed_regions(region, boto_config=boto_config)

    if re_search_with_splits_fn is None:
        def re_search_with_splits_fn(client, resource_type, region, view_arn, stats):
            return re_search_with_splits(
                client,
                resource_type,
                region,
                view_arn,
                stats,
                split_tag_keys=split_tag_keys,
                verbose=verbose,
                re_query_delay=re_query_delay,
            )

    client = boto3.client('resource-explorer-2', region_name=re_region, config=boto_config)
    if not view_arn:
        try:
            view_arn = client.get_default_view().get('ViewArn')
        except Exception as e:
            raise RuntimeError(f"Cannot get RE default view in {re_region}: {e}")

    indexed_regions = re_get_indexed_regions_fn(re_region)

    items = []
    seen = set()
    stats = {'warnings': [], 'missing_regions': []}

    if indexed_regions is not None:
        missing = [r for r in regions if r not in indexed_regions]
        if missing:
            stats['missing_regions'] = missing
            print(f"  WARN  RE not indexed for: {', '.join(missing)}")

    search_regions = list(regions)
    if indexed_regions is not None:
        search_regions = [r for r in regions if r in indexed_regions]

    types_to_scan = list(re_types) if re_types else [None]
    total_types = len(types_to_scan)

    total_queries = 0
    for rt in types_to_scan:
        if rt is None:
            total_queries += len(search_regions) + 1
        elif rt in global_resource_types:
            total_queries += 1
        else:
            total_queries += len(search_regions)
    query_num = 0

    if re_types:
        print(f"  Scanning {total_types} resource types across {len(search_regions)} regions...")
    else:
        print(f"  Scanning all resource types across {len(search_regions)} regions (+ global)...")
    if not _is_verbose(verbose):
        print("  (use --verbose for per-type detail)")

    for resource_type in types_to_scan:
        if resource_type is None:
            query_regions = list(search_regions)
            if 'global' not in query_regions:
                query_regions.append('global')
            type_label = 'ALL_TYPES'
        else:
            query_regions = ['global'] if resource_type in global_resource_types else search_regions
            type_label = resource_type
        type_found = 0

        for region in query_regions:
            query_num += 1
            results = re_search_with_splits_fn(client, resource_type, region, view_arn, stats)
            found_in_region = 0

            for r in results:
                arn = r.get('Arn')
                if not arn or arn in seen:
                    continue
                res_region = r.get('Region') or ''
                if res_region and res_region != 'global' and res_region not in regions:
                    continue
                if res_region in ('', 'global'):
                    svc = arn_service(arn)
                    tag_region = global_tagging_region_overrides.get(svc, global_tagging_region)
                else:
                    tag_region = res_region
                items.append({
                    'arn': arn,
                    'region': tag_region,
                    'resource_region': res_region,
                })
                seen.add(arn)
                found_in_region += 1

            type_found += found_in_region

            if _is_verbose(verbose):
                pct = int(100 * query_num / total_queries) if total_queries else 0
                print(f"  [{query_num:4d}/{total_queries} {pct:3d}%] "
                      f"{type_label} in {region}: {found_in_region}")

        if not _is_verbose(verbose):
            print(f"  {type_label}: {type_found}")

    return items, stats


def _discover_tagging_api_region(
    region,
    tag_types,
    *,
    boto_config=None,
    arn_service=None,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    global_tagging_region_overrides=None,
    tag_api_region_delay=constants.DEFAULT_TAG_API_REGION_DELAY,
):
    """
    Fetch tagged resources in one region via Tagging API.
    Returns (items, tags_by_arn, errors).
    """
    arn_service = arn_service or _default_arn_service
    global_tagging_region_overrides = dict(global_tagging_region_overrides or {})

    items = []
    tags_by_arn = {}
    errors = []

    type_batches = [None] if not tag_types else list(_default_chunked(tag_types, 100))

    try:
        client = boto3.client('resourcegroupstaggingapi', region_name=region, config=boto_config)
    except Exception as e:
        errors.append(f"{region} — {e}")
        return items, tags_by_arn, errors

    for type_batch in type_batches:
        paginator = client.get_paginator('get_resources')
        paginate_args = {}
        if type_batch:
            paginate_args['ResourceTypeFilters'] = type_batch

        # Keep partial results if a later page fails.
        page_iterator = iter(paginator.paginate(**paginate_args))
        while True:
            try:
                page = next(page_iterator)
            except StopIteration:
                break
            except Exception as e:
                if type_batch:
                    batch_label = ','.join(type_batch[:3])
                    if len(type_batch) > 3:
                        batch_label += ',...'
                    errors.append(f"{region} [types: {batch_label}] — {e}")
                else:
                    errors.append(f"{region} — {e}")
                break

            for r in page.get('ResourceTagMappingList', []):
                arn = r.get('ResourceARN')
                if not arn:
                    continue
                tags = {t['Key']: t['Value'] for t in r.get('Tags', [])}
                svc = arn_service(arn)
                arn_region = arn.split(':')[3] if ':' in arn else region
                if not arn_region or arn_region == 'global':
                    tag_region = global_tagging_region_overrides.get(svc, global_tagging_region)
                else:
                    tag_region = region
                items.append({'arn': arn, 'region': tag_region, 'resource_region': region})
                tags_by_arn[arn] = tags
                if tag_api_region_delay > 0:
                    time.sleep(tag_api_region_delay)

    return items, tags_by_arn, errors


def discover_via_tagging_api(
    regions,
    tag_types=None,
    max_workers=None,
    *,
    boto_config=None,
    tag_discovery_workers=constants.DEFAULT_TAG_DISCOVERY_WORKERS,
    verbose=False,
    discover_tagging_api_region_fn=None,
    arn_service=None,
    global_tagging_region=constants.DEFAULT_GLOBAL_TAGGING_REGION,
    global_tagging_region_overrides=None,
    tag_api_region_delay=constants.DEFAULT_TAG_API_REGION_DELAY,
):
    """
    Discover tagged resources via Resource Groups Tagging API in parallel.
    Returns (items, tags_by_arn, errors).
    """
    if discover_tagging_api_region_fn is None:
        def discover_tagging_api_region_fn(region, types):
            return _discover_tagging_api_region(
                region,
                types,
                boto_config=boto_config,
                arn_service=arn_service,
                global_tagging_region=global_tagging_region,
                global_tagging_region_overrides=global_tagging_region_overrides,
                tag_api_region_delay=tag_api_region_delay,
            )

    all_items = []
    all_tags = {}
    all_errors = []
    seen_arns = set()

    if not regions:
        return [], {}, []

    if max_workers is None:
        max_workers = tag_discovery_workers
    effective_workers = min(max_workers, len(regions))
    if _is_verbose(verbose):
        print(f"  Parallel Tagging API discovery: {len(regions)} regions, {effective_workers} workers")

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(discover_tagging_api_region_fn, region, tag_types): region
            for region in regions
        }
        for future in as_completed(futures):
            region = futures[future]
            try:
                items, tags_by_arn, errors = future.result()
                all_errors.extend(errors)
                added = 0
                for item in items:
                    arn = item['arn']
                    if arn not in seen_arns:
                        seen_arns.add(arn)
                        all_items.append(item)
                        all_tags[arn] = tags_by_arn.get(arn, {})
                        added += 1
                if _is_verbose(verbose):
                    print(f"    {region}: {added} resources")
                if errors:
                    for err in errors:
                        print(f"    ERROR {err}")
            except Exception as e:
                all_errors.append(f"{region} — {e}")
                print(f"    ERROR {region} — {e}")

    return all_items, all_tags, all_errors


def _fetch_tags_for_region(region, arns, *, boto_config=None, chunked=_default_chunked):
    """
    Fetch current tags for ARNs in one region via Tagging API.
    Returns (tags_by_arn, errors).
    """
    tags_by_arn = {}
    errors = []
    client = boto3.client('resourcegroupstaggingapi', region_name=region, config=boto_config)

    for batch in chunked(arns, 100):
        try:
            response = client.get_resources(ResourceARNList=batch)
            for r in response.get('ResourceTagMappingList', []):
                tags_by_arn[r['ResourceARN']] = {
                    t['Key']: t['Value'] for t in r.get('Tags', [])
                }
        except Exception as e:
            errors.append(f"{region} — {e}")

    return tags_by_arn, errors


def fetch_tags_for_arns(
    items,
    max_workers=None,
    *,
    boto_config=None,
    chunked=_default_chunked,
    tag_fetch_workers=constants.DEFAULT_TAG_FETCH_WORKERS,
    verbose=False,
    fetch_tags_for_region_fn=None,
):
    """
    Fetch current tags for resource items via Tagging API in parallel.
    Returns (tags_by_arn, errors).
    """
    if fetch_tags_for_region_fn is None:
        def fetch_tags_for_region_fn(region, arns):
            return _fetch_tags_for_region(
                region,
                arns,
                boto_config=boto_config,
                chunked=chunked,
            )

    tags_by_arn = {}
    errors = []

    by_region = defaultdict(list)
    for item in items:
        by_region[item['region']].append(item['arn'])

    if not by_region:
        return {}, []

    if max_workers is None:
        max_workers = tag_fetch_workers
    effective_workers = min(max_workers, len(by_region))
    if _is_verbose(verbose):
        print(f"  Parallel tag fetch: {len(by_region)} regions, {effective_workers} workers")

    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        futures = {
            executor.submit(fetch_tags_for_region_fn, region, arns): region
            for region, arns in by_region.items()
        }
        for future in as_completed(futures):
            region = futures[future]
            try:
                region_tags, region_errors = future.result()
                tags_by_arn.update(region_tags)
                errors.extend(region_errors)
                if _is_verbose(verbose):
                    print(f"    Tags fetched: {region} ({len(region_tags)} resources)")
            except Exception as e:
                errors.append(f"{region} — {e}")
                print(f"    ERROR {region} — {e}")

    return tags_by_arn, errors


def fetch_tags_for_specific_arns(
    arns,
    *,
    boto_config=None,
    unique_nonempty=None,
    tagging_region_for_arn=None,
    chunked=_default_chunked,
    tag_api_arn_lookup_batch_size=constants.DEFAULT_TAG_API_ARN_LOOKUP_BATCH_SIZE,
    tag_write_max_retries=constants.DEFAULT_TAG_WRITE_MAX_RETRIES,
    is_throttling_exception=None,
    retry_backoff_seconds=None,
    is_access_denied_exception=None,
):
    """
    Fetch current tags for specific ARNs via tag:GetResources.
    Returns (tags_by_arn, status_by_arn, errors).
    """
    unique_nonempty = unique_nonempty or _default_unique_nonempty
    tagging_region_for_arn = tagging_region_for_arn or _default_tagging_region_for_arn
    is_throttling_exception = is_throttling_exception or _default_is_throttling_exception
    retry_backoff_seconds = retry_backoff_seconds or _default_retry_backoff_seconds
    is_access_denied_exception = is_access_denied_exception or _default_is_access_denied_exception

    tags_by_arn = {}
    status_by_arn = {}
    errors = []

    by_region = defaultdict(list)
    for arn in unique_nonempty(arns):
        by_region[tagging_region_for_arn(arn)].append(arn)

    for region, region_arns in by_region.items():
        client = boto3.client('resourcegroupstaggingapi', region_name=region, config=boto_config)

        for batch in chunked(region_arns, tag_api_arn_lookup_batch_size):
            attempt = 0
            while True:
                try:
                    response = client.get_resources(ResourceARNList=batch)
                    returned_arns = set()
                    for item in response.get('ResourceTagMappingList', []):
                        item_arn = item.get('ResourceARN')
                        if not item_arn:
                            continue
                        tags = {
                            t['Key']: t['Value'] for t in item.get('Tags', [])
                        }
                        tags_by_arn[item_arn] = tags
                        status_by_arn[item_arn] = (
                            'resolved_with_tags' if tags else 'resolved_no_tags'
                        )
                        returned_arns.add(item_arn)

                    for batch_arn in batch:
                        if batch_arn not in returned_arns and batch_arn not in status_by_arn:
                            status_by_arn[batch_arn] = 'not_found'
                    break
                except Exception as e:
                    if is_throttling_exception(e):
                        attempt += 1
                        if attempt > tag_write_max_retries:
                            errors.append(
                                f"tag:GetResources parent lookup ({region}) — throttled after "
                                f"{tag_write_max_retries} retries"
                            )
                            for batch_arn in batch:
                                status_by_arn.setdefault(batch_arn, 'error')
                            break
                        time.sleep(retry_backoff_seconds(attempt))
                    else:
                        # If mixed batch fails, retry per-ARN to isolate unsupported entries.
                        if len(batch) > 1:
                            for single_arn in batch:
                                try:
                                    single_resp = client.get_resources(ResourceARNList=[single_arn])
                                    mappings = single_resp.get('ResourceTagMappingList', [])
                                    for item in mappings:
                                        item_arn = item.get('ResourceARN')
                                        if not item_arn:
                                            continue
                                        tags = {
                                            t['Key']: t['Value'] for t in item.get('Tags', [])
                                        }
                                        tags_by_arn[item_arn] = tags
                                        status_by_arn[item_arn] = (
                                            'resolved_with_tags' if tags else 'resolved_no_tags'
                                        )
                                    if not mappings and single_arn not in status_by_arn:
                                        status_by_arn[single_arn] = 'not_found'
                                except Exception as single_exc:
                                    status_by_arn[single_arn] = (
                                        'access_denied' if is_access_denied_exception(single_exc) else 'error'
                                    )
                                    errors.append(
                                        f"tag:GetResources parent lookup ({region}, {single_arn}) — "
                                        f"{single_exc}"
                                    )
                        else:
                            status_by_arn[batch[0]] = (
                                'access_denied' if is_access_denied_exception(e) else 'error'
                            )
                            errors.append(f"tag:GetResources parent lookup ({region}) — {e}")
                        break

    return tags_by_arn, status_by_arn, errors
