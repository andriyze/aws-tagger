# AWS Resource Tagger

Discovers **all** AWS resources — including untagged ones — and enforces ownership tagging at scale. Uses a hybrid approach combining **Resource Explorer** (primary, finds untagged resources) and the **Resource Groups Tagging API** (supplement, catches anything RE missed). Both discovery and tag-fetching run in parallel across regions for fast execution.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Requirements](#requirements)
- [IAM Permissions](#iam-permissions)
- [Quick Start](#quick-start)
- [All Options](#all-options)
- [Command Examples](#command-examples)
- [Discovery Modes](#discovery-modes)
- [Resource Types](#resource-types)
- [Exclusion Rules](#exclusion-rules)
- [Tag Inheritance](#tag-inheritance)
- [Rollback](#rollback)
- [Caching](#caching)
- [Performance](#performance)
- [Output and Logging](#output-and-logging)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)

---

## How It Works

### Discovery pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: Resource Explorer                                 │
│  - Queries every (resource_type × region) combination       │
│  - Finds tagged AND untagged resources                      │
│  - Auto-splits queries that hit the 1000-result cap         │
│  - Results cached to .tagger_cache/ by default              │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 2: Fetch live tags (parallel, 15 workers)            │
│  - Gets current tag state for RE-discovered resources       │
│  - Runs in parallel across regions                          │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼ if: RE missed regions, hit caps,
                       or resource types only in Tag API
┌─────────────────────────────────────────────────────────────┐
│  Phase 3: Tagging API supplement (parallel, 15 workers)     │
│  - Runs in parallel across all 18 regions                   │
│  - Catches resources RE missed (unindexed regions, lag)     │
│  - Adds types not in RE (e.g. eks:nodegroup)                │
│  - Only returns tagged resources                            │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: Filter + deduplicate                              │
│  - Union of RE and Tagging API results                      │
│  - Apply SKIP_ARN_PATTERNS and SKIP_TAG_KEYS exclusions     │
│  - Apply --name filter if specified                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼ (only when --inherit-parent-tags)
┌─────────────────────────────────────────────────────────────┐
│  Phase 5: Tag Inheritance (optional)                        │
│  - Re-admits child resources skipped by exclusion rules     │
│  - Resolves parent ARN for each child via ARN parsing or    │
│    API call (EFS, Backup)                                   │
│  - Copies parent tags into child (gaps only; never          │
│    overwrites child's own tags or aws:* keys)               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│  Asset Decisions                                            │
│  - Every discovered resource is shown with TAG or SKIP      │
│  - TAG lines include the tags that will be set              │
│  - Inherited tags show (from <parent-arn>) for traceability │
│  - SKIP lines show reason when --verbose is set             │
└─────────────────────────────────────────────────────────────┘
```

### Output format

Discovery runs silently. After all phases complete, a clean decision list is printed:

```
--- Asset Decisions -------------------------------------------------------
  TAG   arn:aws:s3:::my-bucket                                 Owner:team
  TAG   arn:aws:lambda:us-east-1:123:function:my-func          Owner:team
  TAG   arn:aws:logs:us-east-1:123:log-group:/aws/lambda/y     Owner:team  (from arn:aws:lambda:us-east-1:123:function:y)
  SKIP  arn:aws:kms:us-east-1:123:alias/aws/ebs
  SKIP  arn:aws:logs:us-east-1:123:log-group:/aws/lambda/x

  Total: 170 resources  |  158 will be tagged  |  12 skipped
```

Add `--verbose` to see per-resource skip reasons, per-type/region RE detail, and per-region tag-fetch progress.

### RE result-cap splitting

Resource Explorer hard-caps results at **1000 per query**. When a query hits this cap, the script automatically splits it:

1. **Level 0** — base query: `resourcetype:X region:Y`
2. **Level 1** — split by tag presence: `tag:none` / `-tag:none`
3. **Level 2** — split tagged resources by common tag keys (`Name`, `Environment`, `Owner`, etc.)

If results are still capped after all splits, a warning is logged and the Tagging API supplement picks up the remainder.

---

## Requirements

- Python 3.8+
- `boto3` — `pip install boto3`
- AWS credentials configured (`~/.aws/credentials`, environment variables, or IAM role)
- Resource Explorer enabled in the aggregator region (auto-configured by default — see [IAM Permissions](#iam-permissions))

---

## IAM Permissions

Minimum IAM permissions required:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "STSIdentity",
      "Effect": "Allow",
      "Action": ["sts:GetCallerIdentity"],
      "Resource": "*"
    },
    {
      "Sid": "AccountAlias",
      "Effect": "Allow",
      "Action": ["iam:ListAccountAliases"],
      "Resource": "*"
    },
    {
      "Sid": "ResourceExplorer",
      "Effect": "Allow",
      "Action": [
        "resource-explorer-2:Search",
        "resource-explorer-2:GetDefaultView",
        "resource-explorer-2:ListIndexes"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ResourceExplorerSetup",
      "Effect": "Allow",
      "Action": [
        "resource-explorer-2:CreateResourceExplorerSetup",
        "iam:CreateServiceLinkedRole"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "iam:AWSServiceName": "resource-explorer-2.amazonaws.com"
        }
      }
    },
    {
      "Sid": "TaggingAPIRead",
      "Effect": "Allow",
      "Action": ["tag:GetResources"],
      "Resource": "*"
    },
    {
      "Sid": "TaggingAPIWrite",
      "Effect": "Allow",
      "Action": ["tag:TagResources"],
      "Resource": "*"
    }
  ]
}
```

> RE auto-setup is opt-in. If your role lacks `iam:CreateServiceLinkedRole`, do not pass `--auto-setup`. Discovery still works for already-indexed regions; unindexed regions fall back to the Tagging API.

### Additional permissions for `--inherit-parent-tags`

| Feature | IAM action | When needed |
|---|---|---|
| Parent tag fallback lookup | `tag:GetResources` | When parent is not in discovered set and inheritance fallback runs |
| Parent tag fallback (service-native) | `lambda:ListTags`, `ec2:DescribeTags`, `rds:ListTagsForResource`, `ecs:ListTagsForResource`, `eks:ListTagsForResource`, `states:ListTagsForResource`, `codebuild:ListTagsForResource`, `cloudtrail:ListTags`, `firehose:ListTagsForDeliveryStream`, `elasticloadbalancing:DescribeTags`, `elb:DescribeTags`, `efs:ListTagsForResource`, `route53:ListTagsForResource`, `s3:GetBucketTagging` | When `tag:GetResources` misses parent ARNs and native parent fallback runs |
| EFS access point resolution | `elasticfilesystem:DescribeAccessPoints` | When any EFS access point is discovered |
| Backup recovery point resolution | `backup:DescribeRecoveryPoint` | When any Backup recovery point is discovered |
| ENI parent resolution | `ec2:DescribeNetworkInterfaces` | When ENIs are re-admitted with inheritance |
| EC2 snapshot parent resolution | `ec2:DescribeSnapshots` | When EC2 snapshots are discovered with inheritance enabled |
| EC2 AMI parent resolution | `ec2:DescribeImages` | When EC2 AMIs are discovered with inheritance enabled |
| EC2 security-group-rule parent resolution | `ec2:DescribeSecurityGroupRules` | When security-group-rule resources are discovered with inheritance enabled |
| Auto Scaling group resolution | `autoscaling:DescribeAutoScalingGroups` | When child resources carry `aws:autoscaling:groupName` |
| DLM policy resolution | `dlm:GetLifecyclePolicy` | When child resources carry `aws:dlm:lifecycle-policy-id` |
| Elastic Beanstalk log group resolution | `elasticbeanstalk:DescribeEnvironments` | When `/aws/elasticbeanstalk/...` log groups are discovered |

These are non-fatal: if lookup calls fail, the child resource is tagged independently (without parent tags), unresolved reason counters are reported, and warnings are printed.

---

## Quick Start

```bash
# Install dependency
pip install boto3

# Dry run — scan everything, show what would be tagged, make no changes
python3 tagger.py --dry-run

# Tag all resources missing an Owner tag
python3 tagger.py --tag Owner:platform-team

# Preview before tagging
python3 tagger.py --tag Owner:platform-team --dry-run
```

---

## All Options

```
usage: tagger.py [-h]
                 [--tag KEY:VALUE]
                 [--replace KEY:OLD=NEW]
                 [--force]
                 [--yes]
                 [--output FILE]
                 [--debug]
                 [--dry-run]
                 [--verbose]
                 [--coverage-report]
                 [--no-type-drift-check]
                 [--all-types]
                 [--full]
                 [--name SUBSTRING]
                 [--discovery {auto,resource-explorer,tagging-api}]
                 [--auto-setup]
                 [--resource-explorer-region REGION]
                 [--resource-explorer-view-arn ARN]
                 [--no-cache]
                 [--cache-ttl MINUTES]
                 [--tag-fetch-workers N]
                 [--tag-discovery-workers N]
                 [--inherit-parent-tags]
                 [--save-state FILE]
                 [--restore-state FILE]
                 [--restore-clear-empty]
                 [--allow-restore-account-mismatch]
                 [--remove-tag KEY]
                 [--native-tag-adapters]
                 [--accounts-file FILE]
                 [--assume-role-name ROLE]
                 [--assume-role-session-prefix PREFIX]
                 [--assume-role-external-id ID]
                 [--assume-role-duration SECONDS]
                 [--account-run-timeout SECONDS]
                 [--continue-on-account-error]
```

| Option | Description |
|---|---|
| `--tag KEY:VALUE` | Add a tag if the key is missing. Skips if the key already exists (unless `--force`). Repeatable. |
| `--replace KEY:OLD=NEW` | Replace a tag value only when the current value exactly matches `OLD`. Repeatable. |
| `--force` | When used with `--tag`, overwrites existing tag values. Has no effect on `--replace` (which always requires an exact match). |
| `--yes` | Explicitly approve write operations in non-interactive environments and skip interactive confirmation prompts. |
| `--dry-run` | Show all planned tag changes without AWS tag writes. Enables log file output. If you also pass `--auto-setup`, RE setup calls can still run. |
| `--verbose` | Show per-resource SKIP reasons, per-type/region RE detail, and per-region tag-fetch progress. Without this flag, discovery runs silently and only aggregate counts are shown. |
| `--coverage-report` | Print a service-level coverage summary (top discovered and targeted services) and detailed RE type coverage (supported vs configured). |
| `--no-type-drift-check` | Disable automatic curated-type drift checks against `ListSupportedResourceTypes`. |
| `--output FILE` | Destination file for the ARN list when `--debug` is used. Default: `arns.txt`. |
| `--debug` | Save the ARN list and a full JSON report (including tags and stats) to disk. |
| `--all-types` | Remove resource type filters — scan all types supported by RE / Tagging API. Significantly increases scan time. |
| `--full` | Broad mode: implies `--auto-setup`, `--all-types`, and `--inherit-parent-tags`. |
| `--name SUBSTRING` | Case-insensitive substring match on the full ARN. Only matching ARNs are processed. With `--inherit-parent-tags`, children of matching parents are also included even when the child ARN itself does not contain the substring. |
| `--discovery MODE` | `auto` (default): RE + Tagging API hybrid. `resource-explorer`: RE only. `tagging-api`: Tagging API only. |
| `--auto-setup` | Run Resource Explorer auto-setup (`CreateResourceExplorerSetup`) before discovery. Disabled by default. |
| `--resource-explorer-region` | AWS region for the RE aggregator index. Default: `us-east-1`. |
| `--resource-explorer-view-arn` | Use a specific RE view ARN. Uses the account default view if omitted. |
| `--no-cache` | Ignore cached RE results and force a full rescan. |
| `--cache-ttl MINUTES` | RE cache time-to-live in minutes. `0` (default) = never expire. Cache is stored in `.tagger_cache/`. |
| `--tag-fetch-workers N` | Parallel workers for fetching live tags after RE discovery. Default: `15`. |
| `--tag-discovery-workers N` | Parallel workers for Tagging API supplemental discovery. Default: `15`. |
| `--inherit-parent-tags` | For child resources (log groups, node groups, listeners, snapshots, etc.), inherit missing tags from the parent resource. See [Tag Inheritance](#tag-inheritance). |
| `--save-state FILE` | Before any tag writes, snapshot current tags of all targeted resources to `FILE` (JSON). Compatible with `--dry-run`. Use `--restore-state` to undo. |
| `--restore-state FILE` | Restore tags from a `--save-state` snapshot. Short-circuits discovery — applies directly to the ARNs in the file. Supports `--dry-run`. |
| `--restore-clear-empty` | With `--restore-state`, also clear current non-`aws:*` tags for entries where `tags_before` was empty. Disabled by default for safety. |
| `--allow-restore-account-mismatch` | Override account-ID safety guard for `--restore-state` when snapshot account differs from current credentials. |
| `--remove-tag KEY` | Remove one tag key from all discovered resources that currently have it. Repeatable. |
| `--native-tag-adapters` | When Tagging API writes/removals are unsupported, retry those ARNs using service-native APIs (supported: `ec2`, `s3`, `lambda`, `logs`, `rds`, `elasticloadbalancing`, `eks`, `dynamodb`). |
| `--accounts-file FILE` | Run one child scan/write per account in `FILE` using `sts:AssumeRole`. Preserves existing single-account behavior for each child run. |
| `--assume-role-name ROLE` | Default role name for account IDs in `--accounts-file` (for example `OrganizationAccountAccessRole`). |
| `--assume-role-session-prefix PREFIX` | Session name prefix used for STS in multi-account mode. Default: `tagger`. |
| `--assume-role-external-id ID` | Optional external ID for `sts:AssumeRole` in multi-account mode. |
| `--assume-role-duration SECONDS` | STS session duration for multi-account mode. Range: `900`–`43200`. Default: `3600`. |
| `--account-run-timeout SECONDS` | Per-account child process timeout in multi-account mode. `0` (default) means no timeout. |
| `--continue-on-account-error` | In multi-account mode, continue remaining accounts if one fails. Default behavior is fail-fast. |

---

## Command Examples

### Basics

```bash
# Show help
python3 tagger.py --help

# Dry run — full scan, no changes, enables log file
python3 tagger.py --dry-run

# Dry run with verbose detail (shows skip reasons, RE progress)
python3 tagger.py --dry-run --verbose

# Coverage audit: broad scan + service/type coverage summary
python3 tagger.py --all-types --coverage-report --dry-run
```

`--coverage-report` summarizes discovery/type coverage. Actual writeability gaps (resources discovered but not writable through `tag:TagResources`) are reported during write phase, with per-service counts and optional native fallback results when `--native-tag-adapters` is enabled.

### Multi-account

```bash
# Accounts file with 12-digit IDs, assuming the same role name in each account
python3 tagger.py \
  --accounts-file accounts.txt \
  --assume-role-name OrganizationAccountAccessRole \
  --dry-run

# Apply tags across all accounts in the file
python3 tagger.py \
  --accounts-file accounts.txt \
  --assume-role-name OrganizationAccountAccessRole \
  --tag Owner:platform-team

# Continue with remaining accounts if one account fails
python3 tagger.py \
  --accounts-file accounts.txt \
  --assume-role-name OrganizationAccountAccessRole \
  --tag Owner:platform-team \
  --continue-on-account-error
```

`accounts.txt` supports these formats (one per line, `#` comments allowed):

```text
# 1) account ID (requires --assume-role-name)
123456789012

# 2) full role ARN
arn:aws:iam::123456789012:role/OrganizationAccountAccessRole

# 3) account ID + role name
123456789012,OrganizationAccountAccessRole
```

In multi-account mode:
- The parent process assumes each account role and launches the same script as a child process.
- Existing single-account behavior is preserved inside each child run.
- `--output` (with `--debug`) and `--save-state` are automatically suffixed per account to avoid file collisions.
- `--account-run-timeout` can prevent a single stuck account from blocking the full org run.
- `--restore-state` is intentionally blocked (state files are account-specific and should be restored per account).

### Tagging

```bash
# Add a tag (skips if key already exists)
python3 tagger.py --tag Owner:platform-team

# Add multiple tags at once
python3 tagger.py --tag Owner:platform-team --tag Environment:production

# Overwrite an existing tag value
python3 tagger.py --tag Owner:platform-team --force

# Preview before writing (interactive confirm gate prompts when running in a TTY)
python3 tagger.py --tag Owner:platform-team --dry-run

# If TagResources rejects some types as unsupported, enable native fallbacks
python3 tagger.py --tag Owner:platform-team --native-tag-adapters
```

### Replace (exact value match only)

`--replace` only fires when the resource's current tag value **exactly matches** the old value. It never overwrites values that don't match.

```bash
# Replace a specific old value with a new one
python3 tagger.py --replace Owner:old-team=new-team

# Replace multiple old values (each rule is independent)
python3 tagger.py --replace Owner:old1=new-team --replace Owner:old2=new-team

# Replace within a scoped name filter
python3 tagger.py --name product1 --replace Owner:old-team=new-team

# Preview replace operations
python3 tagger.py --replace Owner:old-team=new-team --dry-run
```

> **Note:** If a resource has `Owner=old1`, only the first matching rule fires. Subsequent rules for the same key are checked but won't match since the key's value has already changed in the pending write (not yet committed). Run the script again if chaining replacements.

### Filtering by name

`--name` does a **case-insensitive substring match on the full ARN**. Useful for scoping operations to a single product, team, or environment without changing the resource type list.

When combined with `--inherit-parent-tags`, the scope expands to include child resources of matching parents (for example, log groups or snapshots), even if the child ARN does not contain the `--name` substring.

```bash
# Only tag resources whose ARN contains "product1"
python3 tagger.py --name product1 --tag Owner:platform-team --dry-run

# Only replace within a specific prefix
python3 tagger.py --name payments --replace Owner:old-team=payments-team
```

### Tag inheritance

```bash
# Tag parent resources AND their child log groups / snapshots / listeners
python3 tagger.py --tag Owner:platform-team --inherit-parent-tags --dry-run

# Inherit tags for a specific product scope only
python3 tagger.py --name my-service --tag Owner:platform-team --inherit-parent-tags --dry-run
```

### Rollback

```bash
# Step 1: Capture current tag state before making changes (dry-run safe)
python3 tagger.py --tag Owner:new-team --save-state before.json --dry-run

# Inspect the snapshot
cat before.json

# Step 2: Apply the actual change
python3 tagger.py --tag Owner:new-team --save-state before.json

# Step 3: If you need to undo, preview the restore
python3 tagger.py --restore-state before.json --dry-run

# Step 4: Restore original tags
python3 tagger.py --restore-state before.json
```

### Discovery modes

```bash
# Default: RE primary + Tagging API supplement (recommended)
python3 tagger.py --dry-run

# Resource Explorer only (may miss resources in unindexed regions)
python3 tagger.py --discovery resource-explorer --dry-run

# Tagging API only (faster, but misses ALL untagged resources)
python3 tagger.py --discovery tagging-api --dry-run

# Run RE auto-setup explicitly
python3 tagger.py --auto-setup --dry-run

# Full mode: auto-setup + all-types + parent inheritance
python3 tagger.py --full --dry-run

# Use a specific RE aggregator region
python3 tagger.py --resource-explorer-region eu-west-1 --dry-run

# Use a specific RE view
python3 tagger.py --resource-explorer-view-arn arn:aws:resource-explorer-2:us-east-1:123456789012:view/my-view/abc123 --dry-run

# Scan all supported resource types (no type filter)
python3 tagger.py --all-types --dry-run
```

### Caching

RE discovery is the slowest part of a scan. Results are cached in `.tagger_cache/` and reused automatically on subsequent runs. The cache key includes the region list, resource types, RE region, and view ARN — different inputs always produce separate cache files.

```bash
# Use cached RE results if available (default behaviour)
python3 tagger.py --replace Owner:old-team=new-team

# Force a full rescan (ignore cache)
python3 tagger.py --no-cache --replace Owner:old-team=new-team

# Expire cache after 60 minutes
python3 tagger.py --cache-ttl 60 --replace Owner:old-team=new-team

# Cache never expires (default: ttl=0)
python3 tagger.py --cache-ttl 0 --replace Owner:old-team=new-team
```

### Debug output

```bash
# Save ARN list and full JSON report (requires --debug, uses --output path)
python3 tagger.py --debug --output arns.txt --dry-run

# Combine with a name filter to scope the report
python3 tagger.py --debug --name product1 --output product1_arns.txt --dry-run
```

### Performance tuning

```bash
# Reduce workers if you're hitting throttling in a specific region
python3 tagger.py --tag-fetch-workers 5 --tag-discovery-workers 5 --dry-run

# Increase workers for very large accounts (up to ~20 is safe)
python3 tagger.py --tag-fetch-workers 20 --tag-discovery-workers 20 --dry-run
```

---

## Discovery Modes

| Mode | Finds untagged resources | Speed | When to use |
|---|---|---|---|
| `auto` (default) | ✅ Yes | Medium | Always recommended |
| `resource-explorer` | ✅ Yes | Slower (RE queries are serial per type×region) | When you want RE only and know all regions are indexed |
| `tagging-api` | ❌ No | Fastest | Quick audits of already-tagged resources |

The Tagging API supplement in `auto` mode activates automatically when:
- RE has unindexed regions (`--missing_regions` non-empty in stats)
- RE hit the 1000-result cap for any (type, region) pair
- There are resource types only discoverable via Tagging API (`eks:nodegroup`, `ec2:network-interface`, `dlm:lifecycle-policy`)
- RE failed entirely (fallback)

---

## Resource Types

The script scans a curated list of ~119 resource types by default. RE and the Tagging API use **different type name formats** for the same resources — the script maintains both lists and normalizes automatically.

| Service | RE type | Tagging API type |
|---|---|---|
| S3 buckets | `s3:bucket` | `s3` |
| ALB | `elasticloadbalancing:loadbalancer/app` | `elasticloadbalancing:loadbalancer` |
| NLB | `elasticloadbalancing:loadbalancer/net` | `elasticloadbalancing:loadbalancer` |
| Classic LB | `elasticloadbalancing:loadbalancer` | `elasticloadbalancing:loadbalancer` |
| EKS node groups | *(not supported)* | `eks:nodegroup` |

To add resource types, append them to `RE_RESOURCE_TYPES` in the script. `TAG_RESOURCE_TYPES` is derived automatically.

Use `--all-types` to remove all type filters and scan everything the APIs return.

By default, each run performs an automatic drift check against
`resource-explorer-2:ListSupportedResourceTypes` and warns when:
- AWS now supports RE types not present in the curated defaults
- A configured curated type is no longer supported

Disable this check with `--no-type-drift-check`.

---

## Exclusion Rules

Resources matching any of the following patterns are automatically skipped and never tagged:

**By ARN pattern** (`SKIP_ARN_PATTERNS`):
- AWS-managed KMS keys (`alias/aws/`)
- AWS-managed SSM parameters (`:parameter/aws/`)
- Service-managed secrets (`:secret:rds!`, `:secret:events!`, etc.)
- IAM service-linked roles and policies
- AWS Backup recovery points
- Auto-created child resources: ENIs, automated RDS snapshots, Backup-created EBS snapshots
- Auto-created CloudWatch log groups (`/aws/lambda/`, `/aws/ecs/`, etc.)
- AWS-managed EventBridge rules and Config rules
- Ephemeral resources: ECS tasks, CodeBuild builds, SageMaker training jobs, CloudFormation changesets, pipeline executions
- Resources not taggable via the Tagging API (Route53 registered domains)

**By tag key** (`SKIP_TAG_KEYS`):
- `aws:backup:source-resource` — AWS Backup copies
- `aws:dlm:lifecycle-policy-id` — DLM-managed EC2 snapshots/AMIs
- `aws:ec2launchtemplate:id` — fleet/template-launched instances
- `aws:autoscaling:groupName` — ASG-managed instances (tag the ASG instead)

To modify exclusion rules, edit `SKIP_ARN_PATTERNS` and `SKIP_TAG_KEYS` at the top of the script.

> **Inheritance override**: A subset of `SKIP_ARN_PATTERNS` can be selectively bypassed when `--inherit-parent-tags` is used. See [Tag Inheritance](#tag-inheritance) for details.

---

## Tag Inheritance

Many AWS resources are auto-created as children of a primary resource. By default these are excluded from tagging (they're managed by AWS, ephemeral, or impractical to tag individually). With `--inherit-parent-tags`, the script re-admits these child resources and copies missing tags from the parent — making them billable-and-visible in cost allocation without requiring manual tagging.

### What gets inherited

The following child types are re-admitted when `--inherit-parent-tags` is set. Tags are resolved by parsing the child ARN (no API call) except where noted.

| Child resource | Parent resolved via |
|---|---|
| CloudWatch log group `/aws/lambda/<name>` | Lambda function (ARN construction) |
| CloudWatch log group `/aws/codebuild/<project>` | CodeBuild project (ARN construction) |
| CloudWatch log group `/aws/eks/<cluster>/...` | EKS cluster (ARN construction) |
| CloudWatch log group `/aws/rds/instance/<id>/...` | RDS DB instance (ARN construction) |
| CloudWatch log group `/aws/rds/cluster/<id>/...` | RDS Aurora cluster (ARN construction) |
| CloudWatch log group `/aws/ecs/<cluster>/...` | ECS cluster (ARN construction) |
| CloudWatch log group `/aws/containerinsights/<cluster>/...` | ECS or EKS cluster (ARN construction) |
| CloudWatch log group `/aws/states/<machine>` | Step Functions state machine (ARN construction) |
| CloudWatch log group `/aws/kinesisfirehose/<stream>` | Kinesis Data Firehose stream (ARN construction) |
| CloudWatch log group `/aws/cloudtrail/<trail>` | CloudTrail trail (ARN construction) |
| CloudWatch log group `/aws/opensearch/<domain>` | OpenSearch domain (ARN construction) |
| CloudWatch log group `/aws/apigateway/<api-id>` | API Gateway REST API or HTTP API (ARN construction; tries both) |
| CloudWatch log group `/aws/route53/<hosted-zone-id>` | Route 53 hosted zone (ARN construction) |
| VPC flow log group `/aws/vendedlogs/traffic-log/<vpc-id>` | VPC (ARN construction) |
| CloudWatch log group `/aws/elasticbeanstalk/<env>/...` | Elastic Beanstalk environment (**API call**: `DescribeEnvironments`) |
| EKS node group | EKS cluster (ARN parsing) |
| ALB/NLB listener | Load balancer (ARN parsing) |
| ALB/NLB listener rule | Load balancer (ARN parsing) |
| EFS access point | EFS file system (**API call**: `DescribeAccessPoints`) |
| ENI (`ec2:network-interface/eni-*`) | Attached EC2 instance / NAT gateway / load balancer / VPC endpoint / EFS file system / instance-connect endpoint (**API call**: `DescribeNetworkInterfaces`, best effort) |
| EC2 snapshot (`ec2:snapshot/snap-*`) | Source EBS volume (**API call**: `DescribeSnapshots`, skips copy-snapshot volume IDs) |
| EC2 AMI (`ec2:image/ami-*`) | Source EC2 instance (**API call**: `DescribeImages`, via `SourceInstanceId`) |
| EC2 security-group-rule (`ec2:security-group-rule/sgr-*`) | Security group (**API call**: `DescribeSecurityGroupRules`) |
| Automated RDS snapshot (`rds:<id>-YYYY-MM-DD-HH-MM`) | RDS DB instance (regex strip date suffix) |
| Automated Aurora snapshot (`rds:<cluster>-YYYY-MM-DD-HH-MM`) | Aurora cluster (regex strip date suffix) |
| AWS Backup recovery point | Source resource (**API call**: `DescribeRecoveryPoint`) |
| Child resources with `aws:backup:source-resource` tag | Source resource ARN from tag |
| Child EC2 instances with `aws:autoscaling:groupName` tag | Auto Scaling group (**API call**: `DescribeAutoScalingGroups`) |
| Child EC2 instances with `aws:ec2launchtemplate:id` tag | EC2 launch template ARN (derived from tag) |
| Child EC2 snapshots/AMIs with `aws:dlm:lifecycle-policy-id` tag | DLM lifecycle policy ARN (**API call**: `GetLifecyclePolicy`, ARN fallback when API is denied) |

### Merge semantics

- The tag **written to AWS** is the plain parent value — `Owner:team`, not `Inherited-Owner:team` or any modified form.
- The tool's console output annotates inherited tags with `(from <parent-arn>)` for traceability. This annotation is never written to AWS.
- A child's **own tags are never overwritten** — parent tags fill only keys the child doesn't already have.
- `aws:*` prefixed tags are never propagated from parent to child (AWS reserved prefix).
- Some child resources can resolve to multiple parent candidates (for example ContainerInsights ECS vs EKS cluster). The script picks the first candidate that exists in the same run's discovered parent set.
- If no discovered parent matches, the script performs fallback parent-tag lookups in two stages:
  - `tag:GetResources(ResourceARNList=...)`
  - service-native read APIs (for common services) if Tagging API lookup misses
- If parent lookup still fails, the run summary reports reason codes (for example `lookup_access_denied`, `lookup_error`, `unsupported_parent_type`, `parent_not_visible`) and the child is tagged independently.

### Example output

```
--- Asset Decisions -------------------------------------------------------
  TAG   arn:aws:lambda:us-east-1:123:function:my-func         Owner:team
  TAG   arn:aws:logs:us-east-1:123:log-group:/aws/lambda/my-func  Owner:team  (from arn:aws:lambda:us-east-1:123:function:my-func)
  TAG   arn:aws:rds:us-east-1:123:db:my-db                    Owner:team
  TAG   arn:aws:rds:us-east-1:123:snapshot:rds:my-db-2026-02-19-04-00  Owner:team  (from arn:aws:rds:us-east-1:123:db:my-db)
```

### Required IAM permissions

When `--inherit-parent-tags` is used, additional read permissions may be needed for API-assisted parent resolution:

```json
{
  "Sid": "InheritanceResolution",
  "Effect": "Allow",
  "Action": [
    "tag:GetResources",
    "lambda:ListTags",
    "ec2:DescribeTags",
    "rds:ListTagsForResource",
    "ecs:ListTagsForResource",
    "eks:ListTagsForResource",
    "states:ListTagsForResource",
    "codebuild:ListTagsForResource",
    "cloudtrail:ListTags",
    "firehose:ListTagsForDeliveryStream",
    "elasticloadbalancing:DescribeTags",
    "elb:DescribeTags",
    "efs:ListTagsForResource",
    "route53:ListTagsForResource",
    "s3:GetBucketTagging",
    "elasticfilesystem:DescribeAccessPoints",
    "backup:DescribeRecoveryPoint",
    "ec2:DescribeNetworkInterfaces",
    "ec2:DescribeSnapshots",
    "ec2:DescribeImages",
    "ec2:DescribeSecurityGroupRules",
    "autoscaling:DescribeAutoScalingGroups",
    "dlm:GetLifecyclePolicy",
    "elasticbeanstalk:DescribeEnvironments"
  ],
  "Resource": "*"
}
```

These calls are non-fatal: if denied, the child resource is tagged independently, a warning is printed, and unresolved reason counters are included in the summary.

---

## Rollback

`--save-state` captures the current tag state of all targeted resources before any writes. `--restore-state` replays a saved snapshot to undo changes.

### State file format

```json
{
  "generated_at": "2026-02-19T12:00:00+00:00",
  "account": "platform-team",
  "invocation": "tagger.py --tag Owner:new-team --save-state before.json",
  "resources": [
    {
      "arn": "arn:aws:s3:::my-bucket",
      "region": "us-east-1",
      "tags_before": {"Owner": "old-team", "Environment": "production"}
    }
  ]
}
```

### Rollback workflow

```bash
# 1. Preview what would change, save state for rollback
python3 tagger.py --tag Owner:new-team --save-state before.json --dry-run

# 2. Apply the change (state is saved before writes)
python3 tagger.py --tag Owner:new-team --save-state before.json

# 3. Preview the rollback
python3 tagger.py --restore-state before.json --dry-run

# 4. Execute the rollback
python3 tagger.py --restore-state before.json
```

> `--restore-state` force-writes **all** saved tag values to their respective ARNs. For resources that were untagged in the snapshot (`tags_before: {}`), current non-`aws:*` tags are **not** cleared unless you pass `--restore-clear-empty`. It does not run discovery — it applies directly to the ARN list in the state file. By default, restore also verifies that the snapshot account ID matches your current caller account ID; override with `--allow-restore-account-mismatch` only for intentional cross-account restores.

---

## Caching

RE discovery results are cached to `.tagger_cache/<account>_<hash>.json`. The hash is derived from the region list, resource type list, RE region, and view ARN — so different scan configurations always use separate cache files.

| Behaviour | Detail |
|---|---|
| Default TTL | `0` (never expires — invalidate manually with `--no-cache`) |
| Cache location | `.tagger_cache/` in the current working directory |
| Cache format | JSON with `generated_at`, `inputs`, `items`, and `stats` |
| What is cached | RE discovery results only. Live tag values are always fetched fresh. |
| Cache invalidation | Pass `--no-cache` to force a full RE rescan |
| Write-intent safety | Non-dry-run runs with `--tag`, `--replace`, or `--remove-tag` automatically bypass RE cache (`--no-cache` implied) |
| TTL-based expiry | Pass `--cache-ttl 60` to expire cache after 60 minutes |

> Tags are **never** cached — only the list of ARNs and their regions. Every run fetches live tag state so `--replace` and `--force` always operate on current values.

---

## Performance

### Parallelism

| Operation | Workers | Behaviour |
|---|---|---|
| Live tag fetch (post-RE) | 15 (configurable) | One thread per region, all regions run concurrently |
| Tagging API discovery | 15 (configurable) | One thread per region, all regions run concurrently |
| RE discovery | Serial | RE queries are per (type × region) — parallelizing hits RE rate limits |

### Throttling safety

The script uses boto3's **adaptive retry mode** (`mode='adaptive'`), which:
- Monitors AWS throttle responses (HTTP 429, `Throttling`, `TooManyRequestsException`, etc.)
- Automatically increases backoff between retries when throttled
- Applies to all parallel workers independently

15 workers across 18 regions means each region sees approximately 1 concurrent request. The Tagging API allows 100 TPS per region — 1 TPS is well within limits. You can safely push `--tag-fetch-workers` and `--tag-discovery-workers` to 20 without issue.

For very large accounts (100k+ resources), consider using `--cache-ttl 60` or `--no-cache` strategically and running tag writes in batched `--name`-scoped passes rather than one giant run.

---

## Output and Logging

### Console output

Discovery runs silently. When it completes, a clean **Asset Decisions** block prints every resource with its TAG or SKIP decision. The summary block follows:

```
--- Resource Explorer Discovery -----------------------------------------
  Discovering... 3,421 resources found
--- Live Tag Fetch -------------------------------------------------------
  Fetching tags for 3,421 resources...
--- Tagging API Supplement -----------------------------------------------
  Discovering... 3,589 resources found (168 added)
--- Filtering + Deduplication -------------------------------------------
  Filtering... done
--- Asset Decisions ------------------------------------------------------
  TAG   arn:aws:s3:::my-bucket                                 Owner:team
  TAG   arn:aws:lambda:us-east-1:123:function:my-func          Owner:team
  SKIP  arn:aws:kms:us-east-1:123:alias/aws/ebs
  ...

  Total: 3,589 resources  |  3,201 will be tagged  |  388 skipped
```

Add `--verbose` to see:
- Per-type/region RE query detail
- Per-region tag-fetch progress
- SKIP reason on every SKIP line

### Log files

When `--dry-run`, `--tag`, `--replace`, or `--remove-tag` is passed, a log file is automatically created named after the AWS account alias (or account ID if no alias is set):

```
platform-team.txt
123456789012.txt
```

The log file captures everything printed to stdout and stderr for that run.

### Debug report (`--debug`)

When `--debug` is passed, two files are written after the scan:

| File | Contents |
|---|---|
| `arns.txt` (or `--output` path) | Plain text list of ARNs, one per line |
| `arns_report.json` | Full JSON: discovery stats + per-resource ARN, region, and current tags |

> `--output` only takes effect when `--debug` is also passed. Without `--debug`, `--output` has no effect.

---

## Limitations

- **Resource Explorer indexing lag** — RE can take up to 36 hours after initial setup before all resources are indexed. New resources may not appear in RE for several minutes. The Tagging API supplement covers the gap for tagged resources.
- **Untagged resources in unindexed regions** — If a region has no RE index, untagged resources there are invisible. RE auto-setup will index the region, but only after the 36-hour indexing window.
- **1000-result cap** — RE's hard cap is 1000 results per query. The script splits queries automatically, but extremely large result sets (>1000 per tag-key split) will still be capped with a warning. The Tagging API supplement catches the tagged portion of any overflow.
- **Tagging API misses untagged resources** — `--discovery tagging-api` mode cannot find resources with no tags at all. Use `auto` or `resource-explorer` mode to catch untagged resources.
- **Global service tagging regions** — CloudFront, Route53, and WAFv2 must be tagged via `us-east-1`. Global Accelerator must be tagged via `us-west-2`. The script handles these routing rules automatically via `GLOBAL_TAGGING_REGION_OVERRIDES`.
- **`--replace` same-key multiple rules** — If a resource matches multiple `--replace` rules for the same tag key, only the first matching rule fires in a single run. Re-run the script for subsequent replacements.
- **`--output` requires `--debug`** — The `--output` path is only written when `--debug` is passed. Without `--debug`, `--output` has no effect.
- **`--restore-state` is not strict full-diff restore** — For resources with non-empty snapshots, restore force-writes saved values but does not delete extra keys added later. For `tags_before: {}` entries, empty-state clearing is now opt-in via `--restore-clear-empty`.
- **`--restore-state` + multi-account** — `--restore-state` is blocked when `--accounts-file` is used. Restore operations must be run per account with account-specific state files.
- **Inheritance fallback limits** — If a parent is not in the discovered set, the script tries `tag:GetResources` and then service-native tag reads for common parent services. Inheritance still fails when lookups are denied, unsupported parent types are encountered, or the parent has no visible tags.

---

## Troubleshooting

### AccessDeniedException errors

Errors of the form `AccessDeniedException: User ... is not authorized to perform: ...` indicate a missing IAM permission.

| Error action | Required permission |
|---|---|
| `resource-explorer-2:Search` | Add to your IAM policy |
| `resource-explorer-2:ListSupportedResourceTypes` | Needed for automatic type-drift checks (or use `--no-type-drift-check`) |
| `tag:GetResources` | Required for Tagging API discovery |
| `lambda:ListTags` / `ec2:DescribeTags` / `rds:ListTagsForResource` (and similar service-native tag-read actions) | Needed only with `--inherit-parent-tags` when parent fallback uses service-native APIs |
| `tag:TagResources` | Required to write tags |
| `tag:UntagResources` | Required for `--remove-tag` and empty-state restore when enabled |
| `sts:AssumeRole` | Required only for `--accounts-file` multi-account mode (on the caller identity) |
| `elasticfilesystem:DescribeAccessPoints` | Only needed with `--inherit-parent-tags` |
| `backup:DescribeRecoveryPoint` | Only needed with `--inherit-parent-tags` |
| `ec2:DescribeNetworkInterfaces` | Only needed with `--inherit-parent-tags` (ENI parent resolution) |
| `ec2:DescribeSnapshots` | Only needed with `--inherit-parent-tags` (EC2 snapshot parent resolution) |
| `ec2:DescribeImages` | Only needed with `--inherit-parent-tags` (EC2 AMI parent resolution) |
| `ec2:DescribeSecurityGroupRules` | Only needed with `--inherit-parent-tags` (security-group-rule parent resolution) |
| `autoscaling:DescribeAutoScalingGroups` | Only needed with `--inherit-parent-tags` (ASG parent resolution) |
| `dlm:GetLifecyclePolicy` | Only needed with `--inherit-parent-tags` (DLM policy parent resolution) |
| `elasticbeanstalk:DescribeEnvironments` | Only needed with `--inherit-parent-tags` (EB log group parent resolution) |

Tag write failures for individual resources (e.g. KMS keys requiring resource-based policies) are logged as warnings but do not abort the run.

When `--native-tag-adapters` is enabled, fallback writes/removals also require the corresponding service-native tagging permissions (for example `ec2:CreateTags`/`ec2:DeleteTags`, `lambda:TagResource`/`lambda:UntagResource`, `logs:TagResource`/`logs:UntagResource`, `rds:AddTagsToResource`/`rds:RemoveTagsFromResource`, `elasticloadbalancing:AddTags`/`elasticloadbalancing:RemoveTags`, `elb:AddTags`/`elb:RemoveTags`, `eks:TagResource`/`eks:UntagResource`, `dynamodb:TagResource`/`dynamodb:UntagResource`, `s3:GetBucketTagging`, `s3:PutBucketTagging`, and `s3:DeleteBucketTagging`).

For multi-account mode, each target account role must trust the caller and allow the same read/write tagging permissions required for single-account runs.

### RE returns 0 results

- **Indexing lag**: RE can take up to 36 hours after initial setup to index all resources. Wait and retry, or use `--discovery tagging-api` for immediate (tagged-only) results.
- **Aggregator region mismatch**: RE indexes must have an aggregator index. If your aggregator is in a different region than `us-east-1`, pass `--resource-explorer-region <your-aggregator-region>`.
- **No RE index in account**: Run with `--auto-setup` to trigger setup, or create an RE index manually in the AWS console.

### "No resources found" or fewer resources than expected

- **Name filter too restrictive**: `--name` matches against the full ARN. If your substring doesn't appear in the ARN, try a shorter or different substring.
- **Resource type excluded**: Check `SKIP_ARN_PATTERNS` and `SKIP_TAG_KEYS` in the script. Add `--verbose` to see SKIP lines with reasons.
- **Untagged resources in tagging-api mode**: `--discovery tagging-api` only returns tagged resources. Switch to `auto` mode.
- **Cache stale**: Pass `--no-cache` to force a full RE rescan.

### Output is very noisy / hard to read

By default, discovery runs silently and only aggregate counts are shown. If you're seeing per-type/region detail, you may have `--verbose` set. Remove it to get the clean default output.

Add `--verbose` intentionally when you want to debug: it shows per-resource SKIP reasons, per-type/region RE query progress, and per-region tag-fetch status.

### Script ran without --dry-run unexpectedly

When running in a TTY (interactive terminal), the script prompts for confirmation before tagging more than 0 resources:

```
About to tag 158 resources. Type 'yes' to proceed:
```

In CI/CD environments (non-TTY), write operations are blocked unless you pass `--yes` explicitly.
Use `--dry-run` to preview, then run with `--yes` only after review.

### Rolling back a bulk write

Use `--save-state` before any write operation to capture a restorable snapshot:

```bash
# Safe pattern: always save state before bulk writes
python3 tagger.py --tag Owner:new-team --save-state rollback-$(date +%Y%m%d).json

# If you need to undo
python3 tagger.py --restore-state rollback-20260219.json --dry-run  # preview
python3 tagger.py --restore-state rollback-20260219.json            # apply
```

### Throttling / rate limit errors

- Reduce workers: `--tag-fetch-workers 5 --tag-discovery-workers 5`
- Adaptive retry mode is on by default — most transient throttles are retried automatically.
- For sustained throttling in a specific region, try running with `--name` scoped to a subset of resources.
- Retry constants (`TAG_WRITE_MAX_RETRIES`, `TAG_WRITE_RETRY_BASE_DELAY`) can be tuned at the top of `tagger.py`.

---

## License

This project is licensed under the MIT License. See `LICENSE`.
