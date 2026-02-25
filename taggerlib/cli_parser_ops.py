"""
CLI parser construction for tagger.

Extracted from tagger.py to keep main() focused on execution flow.
"""

import argparse


def build_parser(
    *,
    resource_explorer_default_region,
    re_cache_default_ttl_minutes,
    tag_fetch_workers,
    tag_discovery_workers,
):
    """Build and return the argparse parser for the tagger CLI."""
    parser = argparse.ArgumentParser(
        description='AWS Resource Tagger — discover all resources and enforce ownership tags',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Discovery:
  By default (--discovery auto), the script uses Resource Explorer as primary
  discovery (finds untagged resources) and supplements with the Tagging API
  to catch anything RE missed. If RE is unavailable, falls back to Tagging
  API only.

  RE and Tagging API use DIFFERENT resource type names. This script maintains
  both mappings automatically. For example:
    RE:          elasticloadbalancing:loadbalancer/app  (ALB only)
    Tagging API: elasticloadbalancing:loadbalancer      (all LBs)
    RE:          s3:bucket                              (S3 buckets)
    Tagging API: s3                                     (S3 buckets)

Performance:
  Tag fetching and Tagging API discovery both run in parallel across regions
  (default: 15 workers each). Use --tag-fetch-workers and
  --tag-discovery-workers to tune if needed.

Examples:
  %(prog)s --tag Owner:platform-team --dry-run
  %(prog)s --tag Owner:platform-team --tag Environment:production
  %(prog)s --replace Owner:old-team=new-team
  %(prog)s --tag Owner:platform-team --force
  %(prog)s --auto-setup --dry-run
  %(prog)s --discovery tagging-api --dry-run
  %(prog)s --all-types --dry-run
  %(prog)s --full --dry-run
  %(prog)s --all-types --coverage-report --dry-run
  %(prog)s --name product1 --tag Owner:platform-team --dry-run
  %(prog)s --inherit-parent-tags --tag Owner:platform-team --dry-run
  %(prog)s --tag Owner:team --save-state before.json --dry-run
  %(prog)s --restore-state before.json --dry-run
  %(prog)s --accounts-file accounts.txt --assume-role-name OrganizationAccountAccessRole --dry-run
  %(prog)s --accounts-file accounts.txt --assume-role-name OrganizationAccountAccessRole --tag Owner:platform
        """,
    )

    # --- Tagging operations ---
    parser.add_argument('--tag', action='append',
                        help='Tag as key:value. Added if missing; skipped if exists '
                             '(use --force to overwrite). Can specify multiple times.')
    parser.add_argument('--replace', action='append',
                        help='Replace tag as key:old=new. Only fires when current value '
                             'exactly matches old. Can specify multiple times.')
    parser.add_argument('--force', action='store_true',
                        help='Overwrite existing tag values (applies to --tag, not --replace).')
    parser.add_argument('--yes', action='store_true',
                        help='Skip interactive confirmation prompts and allow write operations '
                             'in non-interactive environments (use with care).')
    parser.add_argument('--expected-account-id', metavar='ACCOUNT_ID',
                        help='12-digit AWS account ID required for non-dry-run writes. '
                             'Writes are refused if current caller identity does not match.')
    parser.add_argument('--allow-partial-discovery-write', action='store_true',
                        help='Allow non-dry-run writes even when discovery may be partial '
                             '(for example RE 1000-result caps, unindexed RE regions, '
                             'or tagging-api-only discovery). Use with extreme care.')

    # --- Output and reporting ---
    parser.add_argument('--output', default='arns.txt',
                        help='Output file for ARN list when --debug is used (default: arns.txt).')
    parser.add_argument('--debug', action='store_true',
                        help='Save ARN list and full JSON report to disk (requires --output path).')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would happen without AWS tag writes. '
                             'Note: --auto-setup can still perform RE setup calls.')
    parser.add_argument('--verbose', action='store_true',
                        help='Show per-resource SKIP reasons, per-type/region RE detail, '
                             'and per-region tag-fetch progress. Without this flag, '
                             'discovery runs silently and only aggregate counts are shown.')
    parser.add_argument('--coverage-report', action='store_true',
                        help='Print service-level coverage summary (top discovered/targeted services).')
    parser.add_argument('--no-type-drift-check', action='store_true',
                        help='Skip automatic curated-type drift validation against '
                             'Resource Explorer ListSupportedResourceTypes.')

    # --- Resource scope ---
    parser.add_argument('--all-types', action='store_true',
                        help='Include all resource types supported by RE / Tagging API '
                             '(no resource type filter). Significantly increases scan time.')
    parser.add_argument('--full', action='store_true',
                        help='Broad discovery/write mode: implies --auto-setup, --all-types, '
                             'and --inherit-parent-tags.')
    parser.add_argument('--name',
                        help='Only target resources whose ARN contains this substring '
                             '(case-insensitive). Useful for scoping to a single product or team.')

    # --- Discovery mode ---
    parser.add_argument('--discovery', choices=['auto', 'resource-explorer', 'tagging-api'],
                        default='auto',
                        help='Discovery mode. auto (default) = RE primary + Tagging API supplement. '
                             'resource-explorer = RE only (may miss unindexed regions). '
                             'tagging-api = Tagging API only (faster but misses untagged resources).')
    parser.add_argument('--auto-setup', action='store_true',
                        help='Run Resource Explorer indexing setup before discovery '
                             '(CreateResourceExplorerSetup). Disabled by default.')
    parser.add_argument('--no-auto-setup', dest='auto_setup', action='store_false',
                        help=argparse.SUPPRESS)
    parser.set_defaults(auto_setup=False)
    parser.add_argument('--resource-explorer-region', default=resource_explorer_default_region,
                        help='Region for the RE aggregator index (default: us-east-1).')
    parser.add_argument('--resource-explorer-view-arn',
                        help='Use a specific RE view ARN instead of the default view.')

    # --- Cache control ---
    parser.add_argument('--no-cache', action='store_true',
                        help='Ignore any cached RE results and force a full rescan.')
    parser.add_argument('--cache-ttl', type=int, default=re_cache_default_ttl_minutes,
                        help='RE cache TTL in minutes. 0 (default) = never expire. '
                             'Cache is stored in .tagger_cache/ and keyed by account + input hash.')

    # --- Performance tuning ---
    parser.add_argument('--tag-fetch-workers', type=int, default=tag_fetch_workers,
                        help=f'Parallel workers for fetching live tags after RE discovery '
                             f'(default: {tag_fetch_workers}). One worker per region.')
    parser.add_argument('--tag-discovery-workers', type=int, default=tag_discovery_workers,
                        help=f'Parallel workers for Tagging API supplemental discovery '
                             f'(default: {tag_discovery_workers}). One worker per region.')

    # --- Tag inheritance ---
    parser.add_argument('--inherit-parent-tags', action='store_true',
                        help='For child resources (log groups, EKS node groups, EFS access points, '
                             'ALB listeners/rules, automated snapshots, Backup recovery points, '
                             'EC2 snapshots/AMIs/SG rules, and ENIs), '
                             'inherit missing tags from the parent resource. '
                             'Opt-in — does not change default behavior. '
                             'The tag written to AWS is the plain parent value (e.g. Owner:team). '
                             'The output shows "(from <parent-arn>)" for traceability. '
                             'Requires read permissions for inheritance lookup APIs (for example: '
                             'tag:GetResources (parent fallback lookup), '
                             'lambda:ListTags, ec2:DescribeTags, rds:ListTagsForResource '
                             '(native parent-tag fallback), '
                             'elasticfilesystem:DescribeAccessPoints, backup:DescribeRecoveryPoint, '
                             'ec2:DescribeNetworkInterfaces, ec2:DescribeSnapshots, '
                             'ec2:DescribeImages, ec2:DescribeSecurityGroupRules, '
                             'autoscaling:DescribeAutoScalingGroups, dlm:GetLifecyclePolicy).')

    # --- Rollback ---
    parser.add_argument('--save-state', metavar='FILE',
                        help='Before any tag writes, snapshot current tag state of all targeted '
                             'resources to FILE (JSON). Works with --dry-run to preview the '
                             'snapshot without writing tags. Use --restore-state to undo.')
    parser.add_argument('--restore-state', metavar='FILE',
                        help='Restore tags from a previously saved --save-state snapshot. '
                             'Force-writes all saved tag values. Does not run discovery — '
                             'applies directly to the ARNs in the state file. '
                             'Supports --dry-run to preview without making changes.')
    parser.add_argument('--restore-clear-empty', action='store_true',
                        help='With --restore-state, also clear current non-aws:* tags for '
                             'snapshot entries where tags_before was empty ({}). '
                             'Without this flag, empty snapshot entries are not cleared.')
    parser.add_argument('--allow-restore-account-mismatch', action='store_true',
                        help='Allow --restore-state even when the state file account ID does not '
                             'match the current caller account ID. Use only for intentional '
                             'cross-account restore scenarios.')
    parser.add_argument('--remove-tag', action='append', metavar='KEY',
                        help='Remove a tag key from every discovered resource that has it. '
                             'Repeatable for multiple keys. Supports --dry-run to preview. '
                             'Useful for cleanup after accidental tag writes.')
    parser.add_argument('--native-tag-adapters', action='store_true',
                        help='When Tagging API write/removal calls reject a resource as unsupported, '
                             'retry that ARN with service-native tagging APIs '
                             '(supported: ec2, s3, lambda, logs, rds, '
                             'elasticloadbalancing, eks, dynamodb).')

    # --- Multi-account orchestration ---
    parser.add_argument('--accounts-file', metavar='FILE',
                        help='Run the script across multiple AWS accounts. FILE supports '
                             '12-digit account IDs and/or IAM role ARNs (one per line).')
    parser.add_argument('--assume-role-name',
                        help='Default IAM role name to assume for account IDs in --accounts-file '
                             '(for example OrganizationAccountAccessRole).')
    parser.add_argument('--assume-role-session-prefix', default='tagger',
                        help='STS session name prefix for multi-account runs (default: tagger).')
    parser.add_argument('--assume-role-external-id',
                        help='Optional external ID used for sts:AssumeRole in multi-account mode.')
    parser.add_argument('--assume-role-duration', type=int, default=3600,
                        help='AssumeRole session duration in seconds for multi-account mode '
                             '(default: 3600).')
    parser.add_argument('--account-run-timeout', type=int, default=0,
                        help='Per-account child run timeout in seconds for multi-account mode. '
                             '0 (default) means no timeout.')
    parser.add_argument('--continue-on-account-error', action='store_true',
                        help='In multi-account mode, continue with remaining accounts even if one '
                             'account fails. By default, execution stops on first account failure.')

    return parser
