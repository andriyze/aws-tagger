"""
State snapshot and restore operations for tagger.

This module is intentionally dependency-light and receives runtime helpers from
the CLI module so behavior stays identical while reducing monolith size.
"""

from datetime import datetime, timezone
import json


def save_state(resources, output_file, account, invocation):
    """
    Persist the current tag state of all resources to a JSON file.
    Call this BEFORE tag writes to enable rollback via --restore-state.

    The state file captures the tag values BEFORE any changes are made.
    Use --restore-state <file> to re-apply these values and undo a run.
    """
    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'account': account,
        'invocation': invocation,
        'resources': [
            {
                'arn': r['arn'],
                'region': r['region'],
                'tags_before': dict(r['tags']),
            }
            for r in resources
        ],
    }
    with open(output_file, 'w') as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"State saved to {output_file} ({len(resources)} resources)")


def load_state(state_file, arg_error):
    """
    Load a previously saved state file.
    Returns list of {'arn', 'region', 'tags_before'} dicts.
    Exits with code 2 via arg_error if the file is missing or malformed.
    """
    try:
        with open(state_file, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        arg_error(f"State file not found: {state_file}")
    except Exception as e:
        arg_error(f"Cannot parse state file {state_file}: {e}")

    state_resources = data.get('resources')
    if not isinstance(state_resources, list):
        arg_error(f"State file missing 'resources' list: {state_file}")

    generated_at = data.get('generated_at', 'unknown')
    account = data.get('account', 'unknown')
    print(
        f"Loaded state from {state_file}: "
        f"{len(state_resources)} resources, generated {generated_at}, account {account}"
    )
    return state_resources


def restore_tags_from_state(
    state_resources,
    *,
    dry_run,
    fetch_tags_for_specific_arns,
    tag_resources,
    remove_tags_from_resources,
):
    """
    Restore tags to the exact state captured by save_state().
    Force-writes all saved tag values to their respective resources.

    Uses the shared tag write engine for non-empty restores so retry,
    throttling, and error behavior stay consistent with normal write runs.
    """
    normalized = []
    skipped_invalid = 0
    nonempty_targets = 0
    empty_targets = 0
    for entry in state_resources:
        arn = entry.get('arn')
        region = entry.get('region')
        tags = entry.get('tags_before', {})
        if tags is None:
            tags = {}
        if not arn or not region or not isinstance(tags, dict):
            skipped_invalid += 1
            continue
        normalized.append({
            'arn': arn,
            'region': region,
            'tags_before': tags,
        })
        if tags:
            nonempty_targets += 1
        else:
            empty_targets += 1

    if dry_run:
        print(f"DRY RUN — would restore tags for {len(normalized)} resources")
        for entry in normalized[:5]:
            print(f"  {entry['arn']}: {entry.get('tags_before', {})}")
        if len(normalized) > 5:
            print(f"  ... and {len(normalized) - 5} more")
        if skipped_invalid:
            print(f"  WARN skipped {skipped_invalid} malformed state entries")
        print(
            f"  Snapshot composition: {nonempty_targets} with saved tags, "
            f"{empty_targets} with empty tag sets"
        )
        return

    print(f"Restoring tags for {len(normalized)} resources...")
    if skipped_invalid:
        print(f"  WARN skipped {skipped_invalid} malformed state entries")

    # Build explicit restore decisions and delegate writes to the shared
    # tagging engine so retry/error behavior stays aligned with normal writes.
    restore_resources = []
    restore_decisions = []
    empty_state_entries = []
    for entry in normalized:
        arn = entry.get('arn')
        region = entry.get('region')
        tags = entry.get('tags_before', {})
        if tags:
            resource = {
                'arn': arn,
                'region': region,
                'tags': {},
                'inherited_tags': {},
            }
            restore_resources.append(resource)
            restore_decisions.append((resource, dict(tags), {}, {}))
        else:
            empty_state_entries.append({'arn': arn, 'region': region})

    success_count = 0
    failed_count = 0
    if restore_decisions:
        restore_stats = tag_resources(
            restore_resources,
            {},
            [],
            force=True,
            dry_run=dry_run,
            decisions=restore_decisions,
        )
        success_count += restore_stats.get('success_count', 0)
        failed_count += restore_stats.get('failed_count', 0)

    # If a resource was untagged in the snapshot, clear all current user-managed
    # tags so rollback can return to an empty tag set.
    if empty_state_entries:
        print(
            f"Restoring empty tag state for {len(empty_state_entries)} resources "
            "(clearing current non-aws:* tags)..."
        )

        lookup_arns = [entry['arn'] for entry in empty_state_entries]
        current_tags_by_arn, status_by_arn, lookup_errors = fetch_tags_for_specific_arns(lookup_arns)
        for err in lookup_errors:
            print(f"  WARN  {err}")

        to_clear = []
        already_empty_count = 0
        unknown_count = 0
        for entry in empty_state_entries:
            arn = entry['arn']
            lookup_status = status_by_arn.get(arn)
            current_tags = current_tags_by_arn.get(arn, {})
            removable = {
                k: v for k, v in current_tags.items()
                if not k.startswith('aws:')
            }

            if removable:
                to_clear.append({
                    'arn': arn,
                    'region': entry['region'],
                    'tags': removable,
                })
                continue

            if lookup_status in {'resolved_no_tags', 'not_found', 'resolved_with_tags'}:
                # resolved_with_tags can still mean "already empty for user tags"
                # if only aws:* keys are present.
                already_empty_count += 1
            else:
                unknown_count += 1
                failed_count += 1
                print(
                    f"  WARN  {arn} — unable to verify current tags for empty-state restore "
                    f"(lookup status: {lookup_status or 'unknown'})"
                )

        if to_clear:
            clear_keys = sorted({k for r in to_clear for k in r['tags']})
            # dry_run is guaranteed False here (early-return above), but pass
            # through explicitly so future refactors cannot desync behavior.
            removal_stats = remove_tags_from_resources(to_clear, clear_keys, dry_run=dry_run)
            success_count += removal_stats.get('success_count', 0)
            failed_count += removal_stats.get('failed_count', 0)
        success_count += already_empty_count

        if unknown_count:
            print(f"  WARN empty-state restore unresolved for {unknown_count} resources")

    print(f"Restore complete: {success_count} succeeded, {failed_count} failed.")
