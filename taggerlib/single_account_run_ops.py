"""
Single-account execution flow for tagger CLI.

Extracted from tagger.py so main() stays focused on parse/validate/routing.
"""

import sys
from datetime import datetime, timezone


def run_single_account(
    args,
    tags,
    replace_rules,
    *,
    enable_logging,
    re_auto_setup,
    get_all_resources,
    build_re_type_coverage,
    print_type_drift_check_summary,
    print_coverage_report,
    write_report,
    print_asset_decisions,
    get_account_name,
    save_state,
    compute_removal_decisions,
    remove_tags_from_resources,
    compute_tagging_decisions,
    tag_resources,
    regions,
    re_resource_types,
    tag_resource_types,
    invocation_argv=None,
):
    """
    Run the single-account discovery/reporting/tagging flow.
    Preserves existing CLI behavior including prompts and logging.
    """
    log_enabled = bool(args.dry_run or args.tag or args.replace or args.remove_tag)
    log_file = None
    original_stdout = None
    original_stderr = None

    try:
        log_file, original_stdout, original_stderr = enable_logging(log_enabled)

        print(f"AWS Resource Tagger — {datetime.now(timezone.utc).isoformat()}")
        print(f"Discovery mode:       {args.discovery}")
        print(f"Tag fetch workers:    {args.tag_fetch_workers}")
        print(f"Discovery workers:    {args.tag_discovery_workers}")
        if args.verbose:
            print("Verbose:              enabled")
        if args.inherit_parent_tags:
            print("Tag inheritance:      enabled")
        if args.native_tag_adapters:
            print("Native adapters:      enabled")
        print()

        if not args.no_auto_setup:
            re_auto_setup(args.resource_explorer_region, regions)
            print()

        re_types = None if args.all_types else re_resource_types
        tag_types = None if args.all_types else tag_resource_types
        re_type_coverage = None

        try:
            resources, stats = get_all_resources(
                regions,
                re_types=re_types,
                tag_types=tag_types,
                discovery=args.discovery,
                re_region=args.resource_explorer_region,
                re_view_arn=args.resource_explorer_view_arn,
                name_filter=args.name,
                no_cache=args.no_cache,
                cache_ttl_minutes=args.cache_ttl,
                tag_fetch_workers=args.tag_fetch_workers,
                tag_discovery_workers=args.tag_discovery_workers,
                inherit_parent_tags=args.inherit_parent_tags,
            )
        except RuntimeError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(2)

        if args.coverage_report or (not args.no_type_drift_check and re_types is not None):
            re_type_coverage = build_re_type_coverage(args.resource_explorer_region, re_types)
        if not args.no_type_drift_check and re_types is not None:
            print_type_drift_check_summary(re_type_coverage)

        print(f"\n{'=' * 60}")
        print("Scan complete:")
        print(f"  Discovery mode:       {stats['discovery_mode']}")
        print(f"  Total scanned:        {stats['total_scanned']}")
        print(f"  Skipped (excluded):   {stats['skipped']}")
        print(f"  To be processed:      {stats['enforced']}")
        print(f"  Found via RE:         {stats['re_found']}")
        print(f"  Added by Tagging API: {stats['tagging_api_added']}")
        print(f"  Services discovered:  {stats.get('unique_services_discovered', 0)}")
        print(f"  Services targeted:    {stats.get('unique_services_targeted', 0)}")
        if args.inherit_parent_tags:
            print(f"  Tags inherited:       {stats.get('inherited', 0)}")
            if stats.get('inherited_from_external_parent', 0):
                print(f"  Inherited via fallback: {stats['inherited_from_external_parent']}")
            if stats.get('inherited_from_native_parent', 0):
                print(f"  Inherited via native fallback: {stats['inherited_from_native_parent']}")
            if stats.get('inheritance_parent_lookups', 0):
                print(
                    f"  Parent lookups:        {stats['inheritance_parent_lookup_hits']}/"
                    f"{stats['inheritance_parent_lookups']} resolved via tag:GetResources"
                )
                if stats.get('inheritance_parent_lookup_no_tags', 0):
                    print(
                        f"  Parent lookups (no tags): {stats['inheritance_parent_lookup_no_tags']}"
                    )
            if stats.get('inheritance_parent_native_lookups', 0):
                print(
                    f"  Native parent lookups: {stats['inheritance_parent_native_lookup_hits']}/"
                    f"{stats['inheritance_parent_native_lookups']} resolved"
                )
                if stats.get('inheritance_parent_native_lookup_no_tags', 0):
                    print(
                        f"  Native lookups (no tags): {stats['inheritance_parent_native_lookup_no_tags']}"
                    )
            if stats.get('inheritance_failed', 0):
                print(f"  Inheritance failed:   {stats['inheritance_failed']}")
                reason_counts = stats.get('inheritance_unresolved_reason_counts') or {}
                if reason_counts:
                    top = ", ".join(
                        f"{reason}={count}"
                        for reason, count in list(reason_counts.items())[:8]
                    )
                    print(f"  Inheritance fail reasons: {top}")
        if stats.get('name_filter'):
            print(f"  Name filter:          {stats['name_filter']}")
            print(f"  Filtered out:         {stats['filtered_out']}")
        if stats['errors']:
            print(f"  Errors:               {len(stats['errors'])}")
        if stats['warnings']:
            print(f"  Warnings:             {len(stats['warnings'])}")
            for w in stats['warnings']:
                print(f"    WARN  {w}")
        if stats['missing_regions']:
            print(f"  RE missing regions:   {', '.join(stats['missing_regions'])}")
        print(f"{'=' * 60}\n")

        if args.coverage_report:
            print_coverage_report(stats, re_type_coverage=re_type_coverage)

        if args.debug:
            write_report(resources, stats, args.output)

        decisions = None
        total_to_tag = 0
        if tags or replace_rules:
            decisions = compute_tagging_decisions(resources, tags, replace_rules, args.force)
            total_to_tag = sum(1 for _, ts, _, _ in decisions if ts)
            skipped_items = stats.get('skipped_items', [])
            print_asset_decisions(
                resources,
                skipped_items,
                tags,
                replace_rules,
                args.force,
                decisions=decisions,
            )
        elif not resources:
            print("No resources found.")
        else:
            print("No --tag or --replace specified. Scan-only mode complete.")
            print("Run with --tag KEY:VALUE or --replace KEY:OLD=NEW to apply tags.")

        if args.save_state and resources:
            account = get_account_name()
            source_argv = invocation_argv if invocation_argv is not None else sys.argv
            invocation = ' '.join(source_argv)
            save_state(resources, args.save_state, account, invocation)

        if args.remove_tag and resources:
            removal_decisions = compute_removal_decisions(resources, args.remove_tag)
            total_to_remove = len(removal_decisions)

            if not args.dry_run and total_to_remove > 0 and sys.stdin.isatty():
                prompt = (
                    f"\nAbout to remove tag(s) {args.remove_tag} from {total_to_remove} resources.\n"
                    "Type 'yes' to proceed, anything else to abort: "
                )
                try:
                    answer = input(prompt).strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\nAborted.")
                    return
                if answer != 'yes':
                    print("Aborted.")
                    return

            remove_tags_from_resources(resources, args.remove_tag, dry_run=args.dry_run)

        elif tags or replace_rules:
            if not args.dry_run and total_to_tag > 0 and sys.stdin.isatty():
                qualifier = " (--force: will OVERWRITE existing tag values)" if args.force else ""
                prompt = (
                    f"\nAbout to tag {total_to_tag} resources{qualifier}.\n"
                    "Type 'yes' to proceed, anything else to abort: "
                )
                try:
                    answer = input(prompt).strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\nAborted.")
                    return
                if answer != 'yes':
                    print("Aborted.")
                    return

            tag_resources(
                resources,
                tags,
                replace_rules,
                force=args.force,
                dry_run=args.dry_run,
                native_tag_adapters=args.native_tag_adapters,
                decisions=decisions,
            )

    finally:
        if original_stdout is not None:
            sys.stdout = original_stdout
        if original_stderr is not None:
            sys.stderr = original_stderr
        if log_file is not None:
            log_file.close()
