"""
Reporting and decision-display helpers for tagger.

Extracted from tagger.py to reduce monolith size while preserving behavior.
"""

import json
from datetime import datetime, timezone

from .utils import is_verbose as _is_verbose


def write_report(resources, stats, output_file):
    """
    Write two output files:
      <output_file>            — plain text list of ARNs, one per line
      <output_file>_report.json — full JSON with stats and per-resource tag data

    Only called when --debug is passed.
    """
    with open(output_file, 'w') as f:
        f.write('\n'.join(r['arn'] for r in resources))
    print(f"Saved {len(resources)} ARNs to {output_file}")

    report_file = output_file.rsplit('.', 1)[0] + '_report.json'
    report = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'stats': {k: v for k, v in stats.items() if k != 'skipped_items'},
        'resources': [
            {
                'arn': r['arn'],
                'region': r['region'],
                'resource_region': r.get('resource_region', r['region']),
                'tags': r['tags'],   # live tags fetched from AWS before any writes
                'inherited_tags': r.get('inherited_tags', {}),
                'effective_tags': {**r['tags'], **(r.get('inherited_tags') or {})},
                'parent_arn': r.get('parent_arn'),
                'inheritance_unresolved_reason': r.get('inheritance_unresolved_reason'),
            }
            for r in resources
        ],
    }
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Saved report to {report_file}")


def print_coverage_report(
    stats,
    top_n=20,
    re_type_coverage=None,
    *,
    phase,
    native_tag_adapter_services,
):
    """Print service-level discovery/targeting coverage from collected stats."""
    discovered_counts = stats.get('service_counts_discovered') or {}
    targeted_counts = stats.get('service_counts_targeted') or {}

    phase("Coverage Report")
    print(f"  Services discovered: {stats.get('unique_services_discovered', 0)}")
    print(f"  Services targeted:   {stats.get('unique_services_targeted', 0)}")

    if not discovered_counts:
        print("  No discovered resources to summarize.")
        return

    discovered_top = list(discovered_counts.items())[:top_n]
    print(f"\n  Top discovered services ({len(discovered_top)} shown):")
    for svc, count in discovered_top:
        print(f"    {svc:<24} {count}")

    if targeted_counts:
        targeted_top = list(targeted_counts.items())[:top_n]
        print(f"\n  Top targeted services ({len(targeted_top)} shown):")
        for svc, count in targeted_top:
            print(f"    {svc:<24} {count}")

        targeted_services = set(targeted_counts.keys())
        adapter_services = targeted_services.intersection(set(native_tag_adapter_services or []))
        if targeted_services:
            print("\n  Native adapter coverage (fallback when TagResources is unsupported):")
            print(
                f"    Targeted services with fallback adapters: "
                f"{len(adapter_services)}/{len(targeted_services)}"
            )
            if adapter_services:
                sample = ", ".join(sorted(adapter_services)[:12])
                print(f"    Adapter service sample: {sample}")

    if re_type_coverage is not None:
        print("\n  Resource Explorer Type Coverage:")
        if re_type_coverage.get('error'):
            print(f"    WARN unable to list supported RE types: {re_type_coverage['error']}")
        else:
            print(f"    Supported RE types:  {re_type_coverage.get('supported_count', 0)}")
            configured_count = re_type_coverage.get('configured_count')
            if configured_count is None:
                print("    Configured RE types: all-types mode (no curated filter)")
            else:
                missing = re_type_coverage.get('missing_from_config', [])
                unknown = re_type_coverage.get('unknown_in_config', [])
                print(f"    Configured RE types: {configured_count}")
                print(f"    Supported but not configured: {len(missing)}")
                print(f"    Configured but unsupported:   {len(unknown)}")
                if missing:
                    preview = ", ".join(missing[:12])
                    print(f"    Missing sample: {preview}")
                if unknown:
                    preview = ", ".join(unknown[:12])
                    print(f"    Unsupported sample: {preview}")


def print_type_drift_check_summary(re_type_coverage):
    """
    Print concise warnings when curated RE type filters drift from AWS-supported
    Resource Explorer types.
    """
    if re_type_coverage is None:
        return
    if re_type_coverage.get('configured_count') is None:
        return   # all-types mode has no curated filter to drift-check

    error = re_type_coverage.get('error')
    if error:
        print(f"Type drift check: WARN unable to validate RE type coverage ({error})")
        return

    missing = re_type_coverage.get('missing_from_config', [])
    unknown = re_type_coverage.get('unknown_in_config', [])
    if not missing and not unknown:
        print("Type drift check: OK (curated RE types align with AWS-supported types)")
        return

    print(
        "Type drift check: WARN "
        f"{len(missing)} supported RE types not in curated defaults, "
        f"{len(unknown)} configured types not currently supported"
    )


def print_asset_decisions(
    resources,
    skipped_items,
    new_tags,
    replace_rules,
    force,
    decisions=None,
    *,
    phase,
    compute_tagging_decisions=None,
    verbose,
):
    """
    Print a clean TAG/SKIP decision line for every discovered resource.

    Called after discovery but before writes. Shows:
      TAG   <arn>  key:value [key2:value2 ...]  [(from <parent-arn>)]
      SKIP  <arn>  [(<reason>)]  — reason only shown with --verbose

    skipped_items: resources excluded by SKIP_ARN_PATTERNS or SKIP_TAG_KEYS
    resources:     resources that passed the filter (may have tags from inheritance)
    new_tags / replace_rules / force: same as passed to tag_resources()
    """
    phase("Asset Decisions")

    if decisions is None:
        if compute_tagging_decisions is None:
            raise TypeError("Missing required dependency: compute_tagging_decisions")
        decisions = compute_tagging_decisions(resources, new_tags, replace_rules, force)

    # ---- Excluded resources (SKIP_ARN_PATTERNS / SKIP_TAG_KEYS) ----
    for item in skipped_items:
        arn = item['arn']
        if _is_verbose(verbose):
            print(f"  SKIP  {arn}  ({item['reason']})")
        else:
            print(f"  SKIP  {arn}")

    # ---- Tagging decisions ----
    tag_count = 0
    skip_count = 0

    for r, tags_to_set, conflicts, _replace_applied in decisions:
        arn = r['arn']
        parent_arn = r.get('parent_arn')
        inherited_tags = r.get('inherited_tags') or {}
        parent_note = f"  (from {parent_arn})" if parent_arn and inherited_tags else ""

        if tags_to_set:
            tags_str = '  '.join(f"{k}:{v}" for k, v in sorted(tags_to_set.items()))
            print(f"  TAG   {arn}  {tags_str}{parent_note}")
            tag_count += 1
        else:
            if _is_verbose(verbose):
                if conflicts and not force:
                    reason = f"existing keys: {', '.join(sorted(conflicts.keys()))}"
                else:
                    reason = "no applicable tags"
                print(f"  SKIP  {arn}  ({reason}){parent_note}")
            else:
                print(f"  SKIP  {arn}{parent_note}")
            skip_count += 1

    total_excluded = len(skipped_items)
    total_all = total_excluded + len(resources)
    print(
        f"\n  Total: {total_all} resources  |  "
        f"{tag_count} will be tagged  |  "
        f"{total_excluded + skip_count} skipped"
    )
