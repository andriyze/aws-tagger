"""
Discovery pipeline orchestration for tagger.

Extracted from tagger.py to reduce monolith size while preserving behavior.
"""

from collections import defaultdict

from . import constants


def _require_dependency(name, value):
    if value is None:
        raise TypeError(f"Missing required dependency: {name}")
    return value


def get_all_resources(
    regions,
    re_types=None,
    tag_types=None,
    discovery='auto',
    re_region=None,
    re_view_arn=None,
    name_filter=None,
    no_cache=False,
    cache_ttl_minutes=None,
    tag_fetch_workers=None,
    tag_discovery_workers=None,
    inherit_parent_tags=False,
    *,
    phase=None,
    build_re_cache_key=None,
    get_account_name=None,
    re_cache_path=None,
    load_re_cache=None,
    discover_via_resource_explorer=None,
    save_re_cache=None,
    fetch_tags_for_arns=None,
    discover_via_tagging_api=None,
    service_counts_from_arns=None,
    should_skip=None,
    resolve_parent_arns=None,
    fetch_tags_for_specific_arns=None,
    fetch_tags_for_specific_arns_native=None,
    classify_unresolved_inheritance_reason=None,
):
    """
    Main discovery entry point. Combines Resource Explorer and Tagging API
    results into a single deduplicated list of resources with their current tags.

    Discovery modes:
      auto             — RE primary, Tagging API supplements gaps (default)
      resource-explorer — RE only (may miss resources in unindexed regions)
      tagging-api      — Tagging API only (faster, but misses untagged resources)

    The Tagging API supplement runs automatically in 'auto' mode when:
      - RE has unindexed regions (missing_regions non-empty)
      - RE hit 1000-result caps (warnings non-empty)
      - There are resource types only discoverable via Tagging API (eks:nodegroup)
      - All-type mode is active (no type filters), for max tagged-asset breadth
      - RE failed entirely (fallback)

    Returns (resources, stats).
    """
    phase = _require_dependency('phase', phase)
    build_re_cache_key = _require_dependency('build_re_cache_key', build_re_cache_key)
    get_account_name = _require_dependency('get_account_name', get_account_name)
    re_cache_path = _require_dependency('re_cache_path', re_cache_path)
    load_re_cache = _require_dependency('load_re_cache', load_re_cache)
    discover_via_resource_explorer = _require_dependency(
        'discover_via_resource_explorer', discover_via_resource_explorer
    )
    save_re_cache = _require_dependency('save_re_cache', save_re_cache)
    fetch_tags_for_arns = _require_dependency('fetch_tags_for_arns', fetch_tags_for_arns)
    discover_via_tagging_api = _require_dependency(
        'discover_via_tagging_api', discover_via_tagging_api
    )
    service_counts_from_arns = _require_dependency(
        'service_counts_from_arns', service_counts_from_arns
    )
    should_skip = _require_dependency('should_skip', should_skip)
    resolve_parent_arns = _require_dependency('resolve_parent_arns', resolve_parent_arns)
    fetch_tags_for_specific_arns = _require_dependency(
        'fetch_tags_for_specific_arns', fetch_tags_for_specific_arns
    )
    fetch_tags_for_specific_arns_native = _require_dependency(
        'fetch_tags_for_specific_arns_native', fetch_tags_for_specific_arns_native
    )
    classify_unresolved_inheritance_reason = _require_dependency(
        'classify_unresolved_inheritance_reason',
        classify_unresolved_inheritance_reason,
    )

    if re_region is None:
        re_region = constants.DEFAULT_RESOURCE_EXPLORER_DEFAULT_REGION
    if cache_ttl_minutes is None:
        cache_ttl_minutes = constants.DEFAULT_RE_CACHE_DEFAULT_TTL_MINUTES
    if tag_fetch_workers is None:
        tag_fetch_workers = constants.DEFAULT_TAG_FETCH_WORKERS
    if tag_discovery_workers is None:
        tag_discovery_workers = constants.DEFAULT_TAG_DISCOVERY_WORKERS

    resources = []
    stats = {
        'discovery_mode': None,
        'total_scanned': 0,
        'enforced': 0,
        'skipped': 0,
        'filtered_out': 0,
        'name_filter': name_filter or None,
        'errors': [],
        'warnings': [],
        'missing_regions': [],
        're_found': 0,
        'tagging_api_added': 0,
        'inherited': 0,
        'inheritance_failed': 0,
        'inheritance_parent_lookups': 0,
        'inheritance_parent_lookup_hits': 0,
        'inheritance_parent_lookup_no_tags': 0,
        'inheritance_parent_native_lookups': 0,
        'inheritance_parent_native_lookup_hits': 0,
        'inheritance_parent_native_lookup_no_tags': 0,
        'inherited_from_external_parent': 0,
        'inherited_from_native_parent': 0,
        'inheritance_unresolved_reason_counts': {},
        'unique_services_discovered': 0,
        'unique_services_targeted': 0,
        'service_counts_discovered': {},
        'service_counts_targeted': {},
        'skipped_items': [],   # list of {'arn': str, 'reason': str} for display
    }

    re_items = None
    tags_by_arn = {}

    # ---- Phase 1: Resource Explorer discovery --------------------------------
    if discovery in ('auto', 'resource-explorer'):
        phase("Phase 1: Resource Explorer Discovery")
        try:
            cache_used = False
            cache_path = None
            cache_inputs = None

            if not no_cache:
                cache_key, cache_inputs = build_re_cache_key(
                    regions, re_types, re_region, re_view_arn,
                )
                account_name = get_account_name()
                cache_path = re_cache_path(constants.DEFAULT_RE_CACHE_DIR, account_name, cache_key)
                cached = load_re_cache(cache_path, cache_ttl_minutes)
                if cached:
                    re_items, re_stats, generated_at = cached
                    re_stats = re_stats or {}
                    stats['discovery_mode'] = 'resource-explorer-cache'
                    stats['warnings'].extend(re_stats.get('warnings', []))
                    stats['missing_regions'] = re_stats.get('missing_regions', [])
                    stats['re_found'] = len(re_items)
                    cache_used = True
                    ts = generated_at.isoformat() if generated_at else 'unknown'
                    print(
                        f"  Using cached RE results ({len(re_items)} resources) "
                        f"from {cache_path} (generated {ts})"
                    )

            if not cache_used:
                print(f"  Querying Resource Explorer ({re_region})...")
                re_items, re_stats = discover_via_resource_explorer(
                    regions, re_types, re_region, view_arn=re_view_arn,
                )
                stats['discovery_mode'] = 'resource-explorer'
                stats['warnings'].extend(re_stats.get('warnings', []))
                stats['missing_regions'] = re_stats.get('missing_regions', [])
                stats['re_found'] = len(re_items)
                print(f"  RE total: {len(re_items)} resources found")

                if not no_cache:
                    if cache_path is None or cache_inputs is None:
                        cache_key, cache_inputs = build_re_cache_key(
                            regions, re_types, re_region, re_view_arn,
                        )
                        account_name = get_account_name()
                        cache_path = re_cache_path(
                            constants.DEFAULT_RE_CACHE_DIR,
                            account_name,
                            cache_key,
                        )
                    save_re_cache(cache_path, re_items, re_stats, cache_inputs)
                    print(f"  Saved RE cache to {cache_path}")

            if re_items is not None:
                phase("Phase 2: Live Tag Fetch")
                print(f"  Fetching current tags for {len(re_items)} resources...")
                tags_by_arn, tag_errors = fetch_tags_for_arns(
                    re_items, max_workers=tag_fetch_workers,
                )
                print(f"  Tag fetch complete ({len(tags_by_arn)} resources with tags)")
                stats['errors'].extend(tag_errors)

        except Exception as e:
            stats['errors'].append(f"resource-explorer — {e}")
            print(f"  ERROR Resource Explorer failed: {e}")
            if discovery == 'resource-explorer':
                raise RuntimeError(f"Resource Explorer discovery failed: {e}") from e
            re_items = None

    # ---- Phase 3: Tagging API supplement -------------------------------------
    re_base_types = {rt.split('/')[0] for rt in (re_types or [])}
    tag_only_types = set(tag_types or []) - re_base_types - {'s3'}
    if 's3:bucket' in (re_types or []):
        tag_only_types.discard('s3')

    # If one side has no type filter (all-types mode), always run the Tagging
    # API supplement in auto mode to maximize coverage of tagged assets.
    all_types_mode = (re_types is None) or (tag_types is None)

    re_needs_supplement = (
        re_items is None               # RE failed entirely
        or stats['missing_regions']    # some regions have no RE index
        or stats['warnings']           # RE hit 1000-result cap somewhere
        or tag_only_types              # types not in RE need Tagging API
        or all_types_mode              # all-types mode prefers max breadth
    )
    should_supplement = (
        discovery == 'tagging-api'
        or (discovery == 'auto' and re_needs_supplement)
    )

    if should_supplement:
        phase("Phase 3: Tagging API Supplement")
        if tag_only_types and re_items is not None:
            print(f"  Tag API-only types: {', '.join(sorted(tag_only_types))}")
        elif all_types_mode and re_items is not None:
            print("  Tag API supplement: all resource types (no type filters)")

        print(f"  Running parallel Tagging API scan ({len(regions)} regions)...")
        tag_items, tag_tags, tag_errors = discover_via_tagging_api(
            regions, tag_types, max_workers=tag_discovery_workers,
        )
        stats['errors'].extend(tag_errors)
        tags_by_arn.update(tag_tags)

        if re_items is None:
            re_items = tag_items
            stats['discovery_mode'] = 'tagging-api-only'
            stats['re_found'] = 0
            stats['tagging_api_added'] = len(tag_items)
            print(f"  Tagging API: {len(tag_items)} resources found (RE unavailable)")
        else:
            seen = {i['arn'] for i in re_items}
            added = 0
            for item in tag_items:
                if item['arn'] not in seen:
                    re_items.append(item)
                    seen.add(item['arn'])
                    added += 1
            stats['tagging_api_added'] = added
            print(f"  Tagging API added {added} resources not found by RE")
            stats['discovery_mode'] = 'hybrid'

    else:
        if re_items is not None:
            stats['discovery_mode'] = 'resource-explorer'
            print("  RE complete — skipping Tagging API supplement")

    discovered_counts = service_counts_from_arns(i['arn'] for i in (re_items or []))
    stats['service_counts_discovered'] = discovered_counts
    stats['unique_services_discovered'] = len(discovered_counts)

    # ---- Phase 4: Filter and build final resource list -----------------------
    phase("Phase 4: Filter + Deduplicate")
    name_filter_norm = (name_filter or '').strip().lower()

    for item in (re_items or []):
        arn = item['arn']
        region = item['region'] or constants.DEFAULT_GLOBAL_TAGGING_REGION
        tags = tags_by_arn.get(arn, {})
        stats['total_scanned'] += 1

        skip, reason = should_skip(arn, tags, allow_inheritable=inherit_parent_tags)
        if skip:
            stats['skipped'] += 1
            stats['skipped_items'].append({'arn': arn, 'reason': reason})
            continue

        # Case-insensitive substring match on the full ARN.
        # With inheritance enabled, we defer final name scoping until parent
        # relationships are resolved so children of matched parents are kept.
        name_matches = (not name_filter_norm) or (name_filter_norm in arn.lower())
        if name_filter_norm and not inherit_parent_tags and not name_matches:
            stats['filtered_out'] += 1
            continue

        resources.append({
            'arn': arn,
            'region': region,
            'resource_region': item.get('resource_region', region),
            'tags': tags,
            'parent_arn': None,       # filled in Phase 5 when parent is resolvable
            'inherited_tags': {},     # tags to write from parent (never in-place on live tags)
            'inheritance_unresolved_reason': None,
            '_name_match': name_matches,
            '_parent_unresolved': False,
        })

    print(f"  {len(resources)} candidate resources after exclusions")

    # ---- Phase 5: Tag Inheritance (opt-in) -----------------------------------
    if inherit_parent_tags and resources:
        phase("Phase 5: Tag Inheritance")
        print(f"  Resolving parent tags for {len(resources)} resources...")

        # Build an index of known parent ARNs -> tags for O(1) lookup
        parent_index = {r['arn']: r['tags'] for r in resources}
        unresolved_resources = []
        unresolved_parent_arns = set()
        inherited_from_external_parent = 0
        inherited_from_native_parent = 0

        for resource in resources:
            child_arn = resource['arn']
            child_region = resource['region']
            child_tags = resource['tags']

            parent_arns = resolve_parent_arns(child_arn, child_region, child_tags=child_tags)
            if not parent_arns:
                continue   # Not a child resource with a resolvable parent

            # Keep first candidate for diagnostics, then prefer the first
            # candidate that exists in this run's discovered parent set.
            resource['parent_arn'] = parent_arns[0]
            parent_arn = next((p for p in parent_arns if p in parent_index), None)
            if parent_arn is None:
                unresolved_resources.append((resource, parent_arns))
                unresolved_parent_arns.update(parent_arns)
                continue
            resource['parent_arn'] = parent_arn

            # Fill missing keys only — child's own tags always take precedence.
            # aws:* prefixed tags are never propagated (they'd fail writes anyway).
            inherited = {
                k: v for k, v in parent_index[parent_arn].items()
                if k not in resource['tags'] and not k.startswith('aws:')
            }
            if inherited:
                resource['inherited_tags'].update(inherited)

        # Parent fallback lookup:
        # If a parent wasn't in the discovered set, do a best-effort direct
        # tag:GetResources lookup by parent ARN so inheritance still works in
        # filtered or partial scans.
        external_parent_tags = {}
        tagapi_status_by_arn = {}
        if unresolved_parent_arns:
            stats['inheritance_parent_lookups'] = len(unresolved_parent_arns)
            print(
                f"  Parent fallback lookup: querying {len(unresolved_parent_arns)} "
                f"parent ARNs via tag:GetResources..."
            )
            (
                external_parent_tags,
                tagapi_status_by_arn,
                parent_lookup_errors,
            ) = fetch_tags_for_specific_arns(
                sorted(unresolved_parent_arns)
            )
            stats['inheritance_parent_lookup_hits'] = sum(
                1 for s in tagapi_status_by_arn.values()
                if s in {'resolved_with_tags', 'resolved_no_tags'}
            )
            stats['inheritance_parent_lookup_no_tags'] = sum(
                1 for s in tagapi_status_by_arn.values()
                if s == 'resolved_no_tags'
            )
            if parent_lookup_errors:
                stats['warnings'].extend(parent_lookup_errors)
            print(
                f"  Parent fallback lookup: {len(external_parent_tags)} parent ARNs "
                f"returned tag mappings"
            )

        native_parent_tags = {}
        native_status_by_arn = {}
        unresolved_after_tagapi = sorted(
            arn for arn in unresolved_parent_arns
            if tagapi_status_by_arn.get(arn) not in {'resolved_with_tags', 'resolved_no_tags'}
        )
        if unresolved_after_tagapi:
            stats['inheritance_parent_native_lookups'] = len(unresolved_after_tagapi)
            print(
                f"  Parent native fallback: querying {len(unresolved_after_tagapi)} "
                f"parent ARNs via service-native APIs..."
            )
            (
                native_parent_tags,
                native_status_by_arn,
                native_lookup_errors,
            ) = fetch_tags_for_specific_arns_native(unresolved_after_tagapi)
            stats['inheritance_parent_native_lookup_hits'] = sum(
                1 for s in native_status_by_arn.values()
                if s in {'resolved_with_tags', 'resolved_no_tags'}
            )
            stats['inheritance_parent_native_lookup_no_tags'] = sum(
                1 for s in native_status_by_arn.values()
                if s == 'resolved_no_tags'
            )
            if native_lookup_errors:
                stats['warnings'].extend(native_lookup_errors)
            print(
                f"  Parent native fallback: {len(native_parent_tags)} parent ARNs "
                f"returned tag mappings"
            )

        unresolved_reason_counts = defaultdict(int)
        for resource, parent_arns in unresolved_resources:
            parent_arn = next(
                (
                    p for p in parent_arns
                    if tagapi_status_by_arn.get(p) in {'resolved_with_tags', 'resolved_no_tags'}
                ),
                None
            )
            parent_tags = external_parent_tags.get(parent_arn) if parent_arn else None

            if parent_arn is None:
                parent_arn = next(
                    (
                        p for p in parent_arns
                        if native_status_by_arn.get(p) in {'resolved_with_tags', 'resolved_no_tags'}
                    ),
                    None
                )
                parent_tags = native_parent_tags.get(parent_arn) if parent_arn else None

            if parent_arn is None:
                # Parent still unresolved (not taggable, not visible, or missing perms)
                resource['_parent_unresolved'] = True
                reason = classify_unresolved_inheritance_reason(
                    parent_arns,
                    tagapi_status_by_arn,
                    native_status_by_arn,
                )
                resource['inheritance_unresolved_reason'] = reason
                unresolved_reason_counts[reason] += 1
                continue

            resource['parent_arn'] = parent_arn
            parent_tags = parent_tags or {}

            inherited = {
                k: v for k, v in parent_tags.items()
                if k not in resource['tags'] and not k.startswith('aws:')
            }
            if inherited:
                resource['inherited_tags'].update(inherited)
                if parent_arn in external_parent_tags:
                    inherited_from_external_parent += 1
                elif parent_arn in native_parent_tags:
                    inherited_from_native_parent += 1

        stats['inherited_from_external_parent'] = inherited_from_external_parent
        stats['inherited_from_native_parent'] = inherited_from_native_parent
        stats['inheritance_unresolved_reason_counts'] = dict(
            sorted(unresolved_reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        )

    # Final scope when using --name + --inherit-parent-tags:
    # include direct name matches plus any child whose resolved parent matched.
    if name_filter_norm and inherit_parent_tags:
        matched_parent_arns = {
            r['arn']
            for r in resources
            if r.get('_name_match')
        }
        scoped = []
        filtered_out = 0
        for resource in resources:
            include = resource.get('_name_match', False)
            if not include:
                parent_arn = resource.get('parent_arn')
                include = bool(parent_arn and parent_arn in matched_parent_arns)
            if include:
                scoped.append(resource)
            else:
                filtered_out += 1
        resources = scoped
        stats['filtered_out'] += filtered_out

    if inherit_parent_tags:
        stats['inherited'] = sum(1 for r in resources if r.get('inherited_tags'))
        stats['inheritance_failed'] = sum(1 for r in resources if r.get('_parent_unresolved'))
        print(
            f"  Inheritance complete: {stats['inherited']} resources received parent tags, "
            f"{stats['inheritance_failed']} parents unresolved"
        )
        if stats.get('inheritance_unresolved_reason_counts'):
            top = ", ".join(
                f"{reason}={count}"
                for reason, count in list(stats['inheritance_unresolved_reason_counts'].items())[:8]
            )
            print(f"  Inheritance unresolved reasons: {top}")

    # Internal helper fields are not part of the public resource payload.
    for resource in resources:
        resource.pop('_name_match', None)
        resource.pop('_parent_unresolved', None)

    targeted_counts = service_counts_from_arns(r['arn'] for r in resources)
    stats['service_counts_targeted'] = targeted_counts
    stats['unique_services_targeted'] = len(targeted_counts)
    stats['enforced'] = len(resources)
    print(f"  {stats['enforced']} resources to process, {stats['skipped']} excluded")

    return resources, stats
