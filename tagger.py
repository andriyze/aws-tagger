#!/usr/bin/env python3
"""
AWS Resource Tagger
Discovers ALL resources (including untagged) via Resource Explorer + Tagging API
hybrid approach, enforces ownership tagging, and logs output to a file named
after the AWS account.

Discovery strategy:
  1. Resource Explorer — finds tagged AND untagged resources
     - Queries split by (resource_type, region, tag presence) to stay under 1000-result cap
     - Further splits by common tag keys if still over limit
  2. Tagging API — supplements RE results for any indexed regions
     - Catches anything RE missed due to result caps or indexing lag
     - Runs in parallel across regions (15 workers by default)
  3. Merge + deduplicate — union of both sources, best-effort completeness

IMPORTANT — RE vs Tagging API type name mismatches:
  RE and the Tagging API use DIFFERENT resource type strings for the same resources.
  This script maintains separate lists and normalizes automatically.

  Examples:
    ALBs:  RE = "elasticloadbalancing:loadbalancer/app"   Tag API = "elasticloadbalancing:loadbalancer"
    S3:    RE = "s3:bucket"                                Tag API = "s3"
    EKS nodegroups: NOT in RE at all                       Tag API = "eks:nodegroup"

Performance:
  Tag fetching and Tagging API discovery both run in parallel across regions
  using ThreadPoolExecutor (default 15 workers). Each worker hits a different
  region so the per-region TPS quota is never stressed. Adaptive retry in
  BOTO_CONFIG handles any throttling automatically.
"""

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import sys
import json
import re
import time
import random
import os
import hashlib
from collections import defaultdict
from datetime import datetime, timezone

from taggerlib import (
    account_ops,
    arg_validation_ops,
    cli_parser_ops,
    constants as tagger_constants,
    discovery_helpers_ops,
    discovery_pipeline_ops,
    inheritance_resolver_ops,
    multi_account_ops,
    parent_tag_read_ops,
    report_ops,
    single_account_run_ops,
    state_ops,
    tag_write_ops,
    utils as tagger_utils,
)

# ---------------------------------------------------------------------------
# Module-level verbosity flag.
# Set from --verbose in main(). Avoids threading verbose through every call.
# ---------------------------------------------------------------------------
_VERBOSE = False

# ---------------------------------------------------------------------------
# Boto3 client configuration
#
# Adaptive retry mode:
#   - Monitors AWS throttle responses (429, Throttling, etc.)
#   - Automatically increases wait time between retries when throttled
#   - Much smarter than standard fixed-delay retry
#   - max_attempts=10 gives plenty of headroom for burst recovery
#
# max_pool_connections:
#   - Must be >= number of parallel workers to avoid connection pool exhaustion
#   - 25 is safe for 15 workers with some headroom
# ---------------------------------------------------------------------------
BOTO_CONFIG = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive',
    },
    max_pool_connections=25,
)

# ---------------------------------------------------------------------------
# Timing constants
#
# RE_QUERY_DELAY:
#   Small sleep between Resource Explorer search calls to avoid burst-firing.
#   Adaptive retry handles actual throttling; this just smooths the request rate.
#
# TAG_API_REGION_DELAY:
#   Sleep between sequential Tagging API region operations. With parallelization
#   this only applies within a single region's batch loop, not across regions.
#
# TAG_WRITE_* constants:
#   Control tagging write behaviour. Batches of 10 ARNs with exponential backoff
#   on throttle. Max delay caps at 8s to avoid indefinite waits.
# ---------------------------------------------------------------------------
RE_QUERY_DELAY = tagger_constants.DEFAULT_RE_QUERY_DELAY
TAG_API_REGION_DELAY = tagger_constants.DEFAULT_TAG_API_REGION_DELAY
TAG_WRITE_BATCH_DELAY = tagger_constants.DEFAULT_TAG_WRITE_BATCH_DELAY
TAG_WRITE_BATCH_SIZE = tagger_constants.DEFAULT_TAG_WRITE_BATCH_SIZE
TAG_WRITE_MAX_RETRIES = tagger_constants.DEFAULT_TAG_WRITE_MAX_RETRIES
TAG_WRITE_RETRY_BASE_DELAY = 0.5
TAG_WRITE_RETRY_MAX_DELAY = 8.0
TAG_WRITE_RETRY_JITTER = 0.1
# Maximum ARNs per tag:GetResources(ResourceARNList=...) request
TAG_API_ARN_LOOKUP_BATCH_SIZE = tagger_constants.DEFAULT_TAG_API_ARN_LOOKUP_BATCH_SIZE

# AWS limits for tag keys and values
TAG_KEY_MAX_LEN   = 128
TAG_VALUE_MAX_LEN = 256

# ---------------------------------------------------------------------------
# Parallelism
#
# TAG_FETCH_WORKERS:  parallel threads for fetching live tags after RE discovery
# TAG_DISCOVERY_WORKERS: parallel threads for Tagging API supplemental discovery
#
# 15 workers across 18 regions means each region gets ~1 concurrent thread —
# AWS Tagging API allows 100 TPS per region so this is well within limits.
# Both constants can be overridden via --tag-fetch-workers / --tag-discovery-workers CLI flags.
# ---------------------------------------------------------------------------
TAG_FETCH_WORKERS = tagger_constants.DEFAULT_TAG_FETCH_WORKERS
TAG_DISCOVERY_WORKERS = tagger_constants.DEFAULT_TAG_DISCOVERY_WORKERS

# Optional service-native write adapters used when tag:TagResources rejects a
# resource type as unsupported. Keep this list conservative and explicit.
NATIVE_TAG_ADAPTER_SERVICES = tagger_constants.NATIVE_TAG_ADAPTER_SERVICES

# Resource Explorer result cache directory and default TTL
RE_CACHE_DIR = tagger_constants.DEFAULT_RE_CACHE_DIR
RE_CACHE_DEFAULT_TTL_MINUTES = tagger_constants.DEFAULT_RE_CACHE_DEFAULT_TTL_MINUTES

# =============================================================================
# Configuration — regions and global service routing
# =============================================================================

# All AWS regions this script will scan. Add/remove as needed.
# Opt-in regions (ap-east-1) must be enabled in your account first.
REGIONS = [
    # US
    'us-east-1',      # N. Virginia
    'us-east-2',      # Ohio
    'us-west-1',      # N. California
    'us-west-2',      # Oregon
    # Canada
    'ca-central-1',   # Montreal
    # Europe
    'eu-central-1',   # Frankfurt
    'eu-west-1',      # Ireland
    'eu-west-2',      # London
    'eu-west-3',      # Paris
    'eu-north-1',     # Stockholm
    # Asia Pacific
    'ap-southeast-1', # Singapore
    'ap-southeast-2', # Sydney
    'ap-northeast-1', # Tokyo
    'ap-northeast-2', # Seoul
    'ap-northeast-3', # Osaka
    'ap-south-1',     # Mumbai
    'ap-east-1',      # Hong Kong (opt-in — must be enabled in your account)
    # South America
    'sa-east-1',      # São Paulo
]

# Region used for tagging global resources (CloudFront, Route53, WAFv2, etc.)
# Most global services accept tag operations through us-east-1.
GLOBAL_TAGGING_REGION = tagger_constants.DEFAULT_GLOBAL_TAGGING_REGION

# Some global services require tagging through a specific region other than us-east-1.
# Global Accelerator is the main exception — it must be tagged via us-west-2.
GLOBAL_TAGGING_REGION_OVERRIDES = tagger_constants.GLOBAL_TAGGING_REGION_OVERRIDES

# Region used as the Resource Explorer aggregator index.
RESOURCE_EXPLORER_DEFAULT_REGION = tagger_constants.DEFAULT_RESOURCE_EXPLORER_DEFAULT_REGION

# Resource types that exist globally (no per-region queries needed in RE).
# RE represents these with region="global".
GLOBAL_RESOURCE_TYPES = {
    'cloudfront:distribution',
    'route53:hostedzone',
    'wafv2:webacl',
    'wafv2:ipset',
    'wafv2:rulegroup',
    'wafv2:regexpatternset',
    'globalaccelerator:accelerator',
}

# =============================================================================
# Resource type definitions — RE vs Tagging API
#
# Resource Explorer and the Tagging API use DIFFERENT type name formats:
#
#   RE uses granular subtypes:
#     "elasticloadbalancing:loadbalancer/app"  → ALB only
#     "elasticloadbalancing:loadbalancer/net"  → NLB only
#     "s3:bucket"                              → S3 buckets
#
#   Tagging API uses base types that cover ALL subtypes automatically:
#     "elasticloadbalancing:loadbalancer"      → Classic + ALB + NLB + GLB
#     "s3"                                     → S3 buckets
#
# RE_RESOURCE_TYPES  — used for Resource Explorer search queries
# TAG_RESOURCE_TYPES — used for Tagging API ResourceTypeFilters (auto-derived below)
# =============================================================================

RE_RESOURCE_TYPES = [
    # -------------------------------------------------------------------------
    # Compute
    # -------------------------------------------------------------------------
    'ec2:instance',
    'ec2:volume',
    'ec2:security-group',
    'ec2:security-group-rule',
    'ec2:vpc',
    'ec2:subnet',
    'ec2:natgateway',
    'ec2:elastic-ip',
    'ec2:image',
    'ec2:snapshot',
    'ec2:launch-template',
    'ec2:key-pair',
    'ec2:placement-group',
    'ec2:dedicated-host',
    'ec2:fleet',
    'ec2:spot-fleet-request',
    'ec2:capacity-reservation',
    'autoscaling:autoScalingGroup',
    'lambda:function',

    # -------------------------------------------------------------------------
    # Containers
    # -------------------------------------------------------------------------
    'ecs:cluster',
    'ecs:service',
    'ecs:task-definition',
    'ecs:capacity-provider',
    # Note: eks:nodegroup is NOT supported by RE — caught by Tagging API supplement
    'eks:cluster',
    'ecr:repository',
    'ecr-public:repository',

    # -------------------------------------------------------------------------
    # Load Balancers
    #
    # RE requires explicit subtype strings — the base type only returns Classic LBs.
    # All four subtypes must be listed to cover the full LB fleet:
    #   /app  = Application Load Balancer (ALB)
    #   /net  = Network Load Balancer (NLB)
    #   /gwy  = Gateway Load Balancer (GLB)
    #   bare  = Classic Load Balancer (ELB v1)
    # -------------------------------------------------------------------------
    'elasticloadbalancing:loadbalancer',          # Classic LB
    'elasticloadbalancing:loadbalancer/app',      # ALB
    'elasticloadbalancing:loadbalancer/net',      # NLB
    'elasticloadbalancing:loadbalancer/gwy',      # Gateway LB
    'elasticloadbalancing:targetgroup',
    'elasticloadbalancing:listener/app',
    'elasticloadbalancing:listener/net',
    'elasticloadbalancing:listener-rule/app',

    # -------------------------------------------------------------------------
    # Networking — VPC & connectivity
    # -------------------------------------------------------------------------
    'ec2:internet-gateway',
    'ec2:route-table',
    'ec2:network-acl',
    'ec2:prefix-list',
    'ec2:transit-gateway',
    'ec2:transit-gateway-attachment',
    'ec2:transit-gateway-route-table',
    'ec2:vpc-endpoint',
    'ec2:vpc-peering-connection',
    'ec2:vpc-flow-log',
    'ec2:vpn-connection',
    'ec2:vpn-gateway',
    'ec2:customer-gateway',
    'ec2:dhcp-options',
    'ec2:egress-only-internet-gateway',

    # -------------------------------------------------------------------------
    # Networking — DNS, CDN, API
    # -------------------------------------------------------------------------
    'apigateway:restapis',
    'apigateway:apis',                            # HTTP / WebSocket APIs (v2)
    'cloudfront:distribution',
    'route53:hostedzone',
    'globalaccelerator:accelerator',
    'directconnect:dx-gateway',
    'route53resolver:resolver-endpoint',
    'route53resolver:resolver-rule',

    # -------------------------------------------------------------------------
    # Storage
    # -------------------------------------------------------------------------
    's3:bucket',                                  # RE uses s3:bucket; Tagging API uses bare "s3"
    'elasticfilesystem:file-system',
    'elasticfilesystem:access-point',
    'backup:backup-vault',
    'backup:backup-plan',
    'fsx:file-system',

    # -------------------------------------------------------------------------
    # Databases
    # -------------------------------------------------------------------------
    'rds:db',
    'rds:cluster',
    'dynamodb:table',
    'redshift:cluster',
    'elasticache:cluster',
    'elasticache:replicationgroup',
    'es:domain',                                  # OpenSearch / Elasticsearch (legacy type name)
    'aoss:collection',                            # OpenSearch Serverless
    'memorydb:cluster',                           # MemoryDB for Redis

    # -------------------------------------------------------------------------
    # Data / Analytics
    # -------------------------------------------------------------------------
    'kinesis:stream',
    'firehose:deliverystream',
    'glue:database',
    'glue:crawler',
    'glue:job',
    'glue:table',
    'athena:workgroup',

    # -------------------------------------------------------------------------
    # Messaging
    # -------------------------------------------------------------------------
    'sqs:queue',
    'sns:topic',

    # -------------------------------------------------------------------------
    # Security
    # -------------------------------------------------------------------------
    'secretsmanager:secret',
    'kms:key',
    'acm:certificate',
    'wafv2:webacl',
    'wafv2:ipset',
    'wafv2:rulegroup',
    'wafv2:regexpatternset',
    'guardduty:detector',
    'shield:protection',
    'ram:resource-share',

    # -------------------------------------------------------------------------
    # Monitoring / Observability
    # -------------------------------------------------------------------------
    'logs:log-group',
    'cloudwatch:alarm',
    'cloudwatch:dashboard',
    'cloudwatch:metric-stream',
    'events:rule',
    'events:event-bus',
    'synthetics:canary',

    # -------------------------------------------------------------------------
    # CI/CD
    # -------------------------------------------------------------------------
    'codebuild:project',
    'codepipeline:pipeline',
    'codecommit:repository',
    'codedeploy:application',

    # -------------------------------------------------------------------------
    # Infrastructure as Code
    # -------------------------------------------------------------------------
    'cloudformation:stack',
    'cloudformation:stackset',

    # -------------------------------------------------------------------------
    # Auth
    # -------------------------------------------------------------------------
    'cognito-idp:userpool',
    'cognito-identity:identitypool',

    # -------------------------------------------------------------------------
    # Integration / Orchestration
    # -------------------------------------------------------------------------
    'states:stateMachine',
    'ssm:parameter',
    'cloudtrail:trail',
    'config:config-rule',

    # -------------------------------------------------------------------------
    # Network security & appliances
    # -------------------------------------------------------------------------
    'network-firewall:firewall',     # AWS Network Firewall
    'mq:broker',                     # Amazon MQ
    'kafka:cluster',                 # MSK (Managed Streaming for Kafka)
    'transfer:server',               # AWS Transfer Family

    # -------------------------------------------------------------------------
    # ML / AI
    # -------------------------------------------------------------------------
    'sagemaker:notebook-instance',
    'sagemaker:endpoint',
    'bedrock:agent',                 # Amazon Bedrock agents
    'bedrock:knowledge-base',        # Amazon Bedrock knowledge bases

    # -------------------------------------------------------------------------
    # Batch / Job scheduling
    # -------------------------------------------------------------------------
    'batch:compute-environment',     # AWS Batch
]


def _build_tag_api_types(re_types):
    """
    Derive the Tagging API resource type list from the RE resource type list.

    The Tagging API uses shorter, base-level type strings that automatically
    cover all subtypes. This function:
      1. Converts s3:bucket → "s3" (Tagging API's format for S3)
      2. Strips RE subtype suffixes: "loadbalancer/app" → "loadbalancer"
      3. Appends types that exist in Tagging API but NOT in RE (e.g. eks:nodegroup)
    """
    types = set()
    for rt in re_types:
        if rt == 's3:bucket':
            # Tagging API uses bare "s3" for S3 buckets, not "s3:bucket"
            types.add('s3')
            continue
        # Strip RE-specific subtypes after "/" — Tagging API base type covers all
        base = rt.split('/')[0]
        types.add(base)

    # Resource types NOT discoverable via Resource Explorer but available in Tagging API
    types.add('eks:nodegroup')          # EKS managed node groups
    types.add('ec2:network-interface')  # ENIs (mostly auto-created; skipped by SKIP_ARN_PATTERNS)
    types.add('dlm:lifecycle-policy')   # DLM policies (for DLM snapshot/AMI inheritance)

    return sorted(types)


# Auto-derived from RE_RESOURCE_TYPES — do not edit manually
TAG_RESOURCE_TYPES = _build_tag_api_types(RE_RESOURCE_TYPES)

# Legacy alias kept for CLI display / external references
RESOURCE_TYPES = RE_RESOURCE_TYPES


# =============================================================================
# Exclusion rules
#
# SKIP_ARN_PATTERNS: ARN substrings that identify resources that should never
#   be tagged directly. Covers AWS-managed resources, auto-created child
#   resources, ephemeral resources, and resources that don't support tagging
#   via the Tagging API. Tag the parent resource instead.
#
# SKIP_TAG_KEYS: Resources carrying these tag keys are auto-managed by AWS
#   services and should not be tagged directly.
# =============================================================================

SKIP_ARN_PATTERNS = [
    # AWS-managed KMS keys, SSM parameters, secrets
    'alias/aws/',                  # AWS-managed KMS key aliases (aws/ebs, aws/rds, etc.)
    ':parameter/aws/',             # AWS-managed SSM parameters
    ':parameter/AmazonCloudWatch-',# CloudWatch agent managed parameters
    ':parameter/CodeBuild/',       # CodeBuild managed parameters
    ':secret:events!',             # EventBridge managed secrets
    ':secret:rds!',                # RDS managed secrets (created by RDS for rotation)
    ':secret:appflow!',            # AppFlow managed secrets
    ':secret:databrew!',           # DataBrew managed secrets

    # IAM service-linked roles and policies (AWS-managed)
    'AWSServiceRoleFor',
    ':policy/aws-service-role/',
    'AutoScalingManagedRule',      # Auto Scaling managed EventBridge rules

    # AWS Backup recovery points (tag the backup vault/plan or use --inherit-parent-tags)
    ':recovery-point:',

    # Auto-created child resources — tag the parent service resource instead
    ':network-interface/eni-',     # ENIs created by Lambda, ELB, RDS, ECS tasks
    ':dbsnapshot:rds:',            # Legacy RDS automated snapshot ARN variant
    ':snapshot:rds:',              # RDS automated snapshots (use --inherit-parent-tags to tag)
    ':cluster-snapshot:rds:',      # Aurora automated cluster snapshots

    # Auto-created CloudWatch log groups — tag originating service, or use --inherit-parent-tags
    '/aws/lambda/',
    '/aws/apigateway/',
    '/aws/ecs/',
    '/aws/rds/',
    '/aws/vendedlogs/',            # VPC flow logs, WAF logs
    '/aws/codebuild/',
    '/aws/elasticbeanstalk/',
    '/aws/eks/',
    '/aws/containerinsights/',
    '/aws/kinesisfirehose/',
    '/aws/route53/',
    '/aws/cloudtrail/',
    '/aws/opensearch/',
    '/aws/states/',

    # AWS-managed Config rules and EventBridge rules
    '/aws-service-rule/',
    ':config-rule/securityhub-',   # Security Hub managed Config rules
    ':rule/aws.',                  # AWS-managed EventBridge rules (aws.health, aws.ec2, etc.)
    ':stack/AWS',                  # AWS-managed CloudFormation stacks

    # Ephemeral resources — tag the parent instead
    ':task/',                      # ECS tasks (tag the ECS service or cluster)
    ':build/',                     # CodeBuild builds (tag the CodeBuild project)
    ':changeset/',                 # CloudFormation changesets (tag the stack)
    ':execution/',                 # Step Functions / CodePipeline executions
    ':container-instance/',        # ECS container instances managed by ASG
    ':training-job/',              # SageMaker training jobs (ephemeral — tag endpoint/notebook)

    # Resources not taggable via the Tagging API
    'route53domains',              # Registered domains (use Route53 Domains API directly)
]

# Resources carrying any of these tag keys are managed by AWS services.
# Skip them — tagging them directly can break service behavior.
SKIP_TAG_KEYS = [
    'aws:backup:source-resource',       # AWS Backup-created copies
    'aws:dlm:lifecycle-policy-id',      # DLM-managed EC2 snapshots / AMIs
    'aws:ec2launchtemplate:id',         # Instances launched by fleet / launch template
    'aws:autoscaling:groupName',        # ASG-managed instances (inherit tags from ASG instead)
]

# =============================================================================
# Tag Inheritance — child-to-parent mapping
#
# When --inherit-parent-tags is used, child resources that are normally excluded
# via SKIP_ARN_PATTERNS are instead discovered and tagged with their parent's tags.
#
# _INHERITABLE_SKIP_PATTERNS: subset of SKIP_ARN_PATTERNS that become opt-in
#   when --inherit-parent-tags is active. These resources have clear parent
#   relationships and their tags can be reliably derived.
#
# _LOG_GROUP_PREFIX_MAP: maps log group name prefixes to parent service info
#   for ARN construction. Format: (prefix, aws_service, resource_type_fragment)
# =============================================================================

_LOG_GROUP_PREFIX_MAP = list(tagger_constants.LOG_GROUP_PREFIX_MAP)

_INHERITABLE_SKIP_PATTERNS = frozenset({
    # CloudWatch log groups for AWS services
    '/aws/lambda/',         '/aws/codebuild/',      '/aws/eks/',
    '/aws/rds/',            '/aws/ecs/',            '/aws/states/',
    '/aws/kinesisfirehose/','/aws/route53/',        '/aws/cloudtrail/',
    '/aws/opensearch/',     '/aws/apigateway/',     '/aws/containerinsights/',
    '/aws/elasticbeanstalk/',
    '/aws/vendedlogs/',
    # Common auto-created network children
    ':network-interface/eni-',
    # Backup and automated snapshots
    ':recovery-point:',
    ':dbsnapshot:rds:',
    ':snapshot:rds:',
    ':cluster-snapshot:rds:',
})

_INHERITABLE_SKIP_TAG_KEYS = frozenset({
    # Parent link available in tags or derivable with lightweight API calls
    'aws:backup:source-resource',
    'aws:autoscaling:groupName',
    'aws:ec2launchtemplate:id',
    'aws:dlm:lifecycle-policy-id',
})

# Tag keys used for secondary RE query splitting when the 1000-result cap is hit.
# RE queries are split by "has this tag key" / "does not have this tag key" pairs
# to divide large result sets into manageable chunks.
SPLIT_TAG_KEYS = ['Name', 'Environment', 'env', 'Owner', 'Project', 'Team',
                  'Application', 'Stack', 'aws:cloudformation:stack-name']


# =============================================================================
# Helpers
# =============================================================================

class TeeStream:
    """
    Wraps a stream (stdout/stderr) so writes go to both the original stream
    and a log file simultaneously. Used to capture all output to account-named
    log files without losing terminal output.
    """
    def __init__(self, primary, log_file):
        self.primary = primary
        self.log_file = log_file

    def write(self, data):
        self.primary.write(data)
        self.log_file.write(data)

    def flush(self):
        self.primary.flush()
        self.log_file.flush()

    def isatty(self):
        return self.primary.isatty()

    def fileno(self):
        return self.primary.fileno()


def sanitize_filename(value):
    """Compatibility wrapper for filename-safe account labels."""
    return account_ops.sanitize_filename(value)


def get_account_name():
    """
    Return the AWS account alias if set, otherwise the numeric account ID.
    Used for naming log files and cache files so outputs are self-identifying.
    """
    return account_ops.get_account_name(boto_config=BOTO_CONFIG)


def get_account_identity(strict=False):
    """Return account metadata (ID, alias, preferred display name)."""
    return account_ops.get_account_identity(
        boto_config=BOTO_CONFIG,
        strict=strict,
    )


def enable_logging(enabled):
    """
    Redirect stdout and stderr through TeeStream so all output is written to
    both the terminal and a log file named after the AWS account.
    Returns (log_file, original_stdout, original_stderr) for cleanup in finally block.
    """
    if not enabled:
        return None, None, None
    account_name = sanitize_filename(get_account_name())
    log_path = f"{account_name}.txt"
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    log_file = os.fdopen(log_fd, 'w')
    os.chmod(log_path, 0o600)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = TeeStream(sys.stdout, log_file)
    sys.stderr = TeeStream(sys.stderr, log_file)
    print(f"Logging to {log_path}")
    return log_file, original_stdout, original_stderr


chunked = tagger_utils.chunked


def _parse_iso_datetime(value):
    """Parse an ISO 8601 datetime string, ensuring UTC timezone."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def build_re_cache_key(regions, re_types, re_region, view_arn):
    """
    Build a deterministic cache key from the discovery inputs.
    SHA1 of a sorted JSON payload ensures the same inputs always produce
    the same key, and different inputs produce different keys.
    """
    payload = {
        'regions': sorted(regions or []),
        're_types': sorted(re_types or []) if re_types else 'ALL',
        're_region': re_region or '',
        'view_arn': view_arn or '',
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()[:16]
    return digest, payload


def re_cache_path(cache_dir, account_name, cache_key):
    """Return the full path to a cache file for the given account and cache key."""
    safe_account = sanitize_filename(account_name or 'unknown-account')
    filename = f"{safe_account}_{cache_key}.json"
    return os.path.join(cache_dir, filename)


def load_re_cache(cache_path, ttl_minutes):
    """
    Load Resource Explorer results from a cache file.
    Returns (items, stats, generated_at) if valid, or None if missing/expired.
    TTL of 0 means the cache never expires (only invalidated by --no-cache).
    """
    try:
        with open(cache_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None

    items = data.get('items')
    stats = data.get('stats') or {}
    generated_at = _parse_iso_datetime(data.get('generated_at'))

    if not isinstance(items, list):
        return None

    # Enforce TTL if set (ttl_minutes=0 means no expiry)
    if ttl_minutes and generated_at:
        age_seconds = (datetime.now(timezone.utc) - generated_at).total_seconds()
        if age_seconds > ttl_minutes * 60:
            return None

    return items, stats, generated_at


def save_re_cache(cache_path, items, stats, inputs):
    """Persist Resource Explorer discovery results to a JSON cache file."""
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'inputs': inputs,
        'items': items,
        'stats': stats,
    }
    fd = os.open(cache_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(payload, f, indent=2, default=str)
    os.chmod(cache_path, 0o600)


# Error codes returned by AWS that indicate request rate limiting.
# Used to distinguish throttling from real errors in retry logic.
THROTTLE_ERROR_CODES = {
    'Throttling',
    'ThrottlingException',
    'ThrottledException',
    'TooManyRequestsException',
    'RequestLimitExceeded',
    'RateExceeded',
}


def is_throttling_error(code, message):
    """Return True if the AWS error code/message indicates throttling."""
    code = (code or '').strip()
    if code in THROTTLE_ERROR_CODES:
        return True
    msg = (message or '').lower()
    return (
        'rate exceeded' in msg
        or 'throttl' in msg
        or 'too many requests' in msg
        or 'request limit exceeded' in msg
    )


def is_throttling_exception(exc):
    """Return True if an exception (ClientError or other) represents throttling."""
    if isinstance(exc, ClientError):
        err = exc.response.get('Error', {})
        return is_throttling_error(err.get('Code'), err.get('Message'))
    return is_throttling_error('', str(exc))


def is_access_denied_exception(exc):
    """Return True if an exception indicates IAM access denial."""
    if isinstance(exc, ClientError):
        err = exc.response.get('Error', {})
        code = (err.get('Code') or '').strip()
        msg = (err.get('Message') or '').lower()
        if code in {'AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', 'UnauthorizedException'}:
            return True
        return 'not authorized' in msg or 'access denied' in msg
    msg = str(exc).lower()
    return 'not authorized' in msg or 'access denied' in msg


def retry_backoff_seconds(attempt):
    """
    Calculate exponential backoff with jitter for retry attempts.
    Doubles each attempt, capped at TAG_WRITE_RETRY_MAX_DELAY.
    Small random jitter prevents thundering herd when multiple workers retry.
    """
    base = TAG_WRITE_RETRY_BASE_DELAY * (2 ** (attempt - 1))
    delay = min(TAG_WRITE_RETRY_MAX_DELAY, base)
    jitter = random.uniform(0, TAG_WRITE_RETRY_JITTER)
    return delay + jitter


def _phase(label):
    """Print a visual phase separator banner."""
    fill = '-' * max(0, 72 - len(label) - 5)
    print(f"\n--- {label} {fill}")


arn_service = tagger_utils.arn_service


def tagging_region_for_arn(arn, default_region=GLOBAL_TAGGING_REGION):
    """
    Resolve which region to use for Tagging API calls for a given ARN.
    Global resources are routed through GLOBAL_TAGGING_REGION_OVERRIDES.
    """
    return tagger_utils.tagging_region_for_arn(
        arn,
        default_region=default_region,
        global_overrides=GLOBAL_TAGGING_REGION_OVERRIDES,
    )


def _sorted_count_dict(counts):
    """Return a count dict sorted by descending count then key."""
    return {k: v for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))}


def service_counts_from_arns(arns):
    """Build per-service counts from an iterable of ARNs."""
    counts = defaultdict(int)
    for arn in arns:
        counts[arn_service(arn)] += 1
    return _sorted_count_dict(counts)


def should_skip(arn, tags, allow_inheritable=False):
    """
    Check whether a resource should be excluded from tagging enforcement.
    Returns (True, reason_string) if the resource should be skipped,
    or (False, '') if it should be processed.

    allow_inheritable: when True, patterns in _INHERITABLE_SKIP_PATTERNS are
    not treated as exclusions — the resource passes through for tag inheritance.
    Only set when --inherit-parent-tags is active.
    """
    for pattern in SKIP_ARN_PATTERNS:
        if pattern in arn:
            if allow_inheritable and pattern in _INHERITABLE_SKIP_PATTERNS:
                continue   # Pass through for inheritance
            return True, pattern
    for tag_key in SKIP_TAG_KEYS:
        if tag_key in tags:
            if allow_inheritable and tag_key in _INHERITABLE_SKIP_TAG_KEYS:
                continue   # Pass through for inheritance
            return True, f"tag:{tag_key}"
    return False, ''


# =============================================================================
# Tag Inheritance — resolve child ARN to parent ARN
# =============================================================================

resolve_log_group_parent_arns = inheritance_resolver_ops.resolve_log_group_parent_arns
resolve_eks_nodegroup_parent_arn = inheritance_resolver_ops.resolve_eks_nodegroup_parent_arn
resolve_alb_listener_parent_arn = inheritance_resolver_ops.resolve_alb_listener_parent_arn
resolve_efs_access_point_parent_arn = inheritance_resolver_ops.resolve_efs_access_point_parent_arn
resolve_rds_snapshot_parent_arn = inheritance_resolver_ops.resolve_rds_snapshot_parent_arn
resolve_backup_recovery_point_parent_arn = inheritance_resolver_ops.resolve_backup_recovery_point_parent_arn
resolve_network_interface_parent_arns = inheritance_resolver_ops.resolve_network_interface_parent_arns
resolve_ec2_snapshot_parent_arns = inheritance_resolver_ops.resolve_ec2_snapshot_parent_arns
resolve_ec2_image_parent_arns = inheritance_resolver_ops.resolve_ec2_image_parent_arns
resolve_security_group_rule_parent_arn = inheritance_resolver_ops.resolve_security_group_rule_parent_arn
resolve_managed_tag_parent_arns = inheritance_resolver_ops.resolve_managed_tag_parent_arns


def resolve_parent_arns(child_arn, region, child_tags=None):
    """Compatibility wrapper with explicit inheritance resolver dependencies."""
    return inheritance_resolver_ops.resolve_parent_arns(
        child_arn,
        region,
        child_tags=child_tags,
        boto_config=BOTO_CONFIG,
        verbose=lambda: _VERBOSE,
        log_group_prefix_map=_LOG_GROUP_PREFIX_MAP,
    )

# =============================================================================
# Discovery helpers — Resource Explorer + Tagging API
# =============================================================================

def re_search(client, query, view_arn=None):
    """Compatibility wrapper for RE search helper."""
    return discovery_helpers_ops.re_search(
        client,
        query,
        view_arn=view_arn,
        re_query_delay=RE_QUERY_DELAY,
    )


def re_search_with_splits(client, resource_type, region, view_arn, stats):
    """Compatibility wrapper for split RE search helper."""
    return discovery_helpers_ops.re_search_with_splits(
        client,
        resource_type,
        region,
        view_arn,
        stats,
        split_tag_keys=SPLIT_TAG_KEYS,
        verbose=lambda: _VERBOSE,
        re_query_delay=RE_QUERY_DELAY,
    )


def re_auto_setup(re_region, regions):
    """Compatibility wrapper for RE auto-setup."""
    return discovery_helpers_ops.re_auto_setup(
        re_region,
        regions,
        boto_config=BOTO_CONFIG,
    )


def re_get_indexed_regions(re_region):
    """Compatibility wrapper for RE indexed-region listing."""
    return discovery_helpers_ops.re_get_indexed_regions(
        re_region,
        boto_config=BOTO_CONFIG,
    )


def re_list_supported_resource_types(re_region):
    """Compatibility wrapper for RE supported-type listing."""
    return discovery_helpers_ops.re_list_supported_resource_types(
        re_region,
        boto_config=BOTO_CONFIG,
    )


def build_re_type_coverage(re_region, configured_re_types):
    """Compatibility wrapper for RE type drift/coverage computation."""
    return discovery_helpers_ops.build_re_type_coverage(
        re_region,
        configured_re_types,
        boto_config=BOTO_CONFIG,
    )


def discover_via_resource_explorer(regions, re_types, re_region, view_arn=None):
    """Compatibility wrapper for RE discovery."""
    return discovery_helpers_ops.discover_via_resource_explorer(
        regions,
        re_types,
        re_region,
        view_arn=view_arn,
        boto_config=BOTO_CONFIG,
        re_query_delay=RE_QUERY_DELAY,
        split_tag_keys=SPLIT_TAG_KEYS,
        global_resource_types=GLOBAL_RESOURCE_TYPES,
        global_tagging_region=GLOBAL_TAGGING_REGION,
        global_tagging_region_overrides=GLOBAL_TAGGING_REGION_OVERRIDES,
        arn_service=arn_service,
        verbose=lambda: _VERBOSE,
    )


def _discover_tagging_api_region(region, tag_types):
    """Compatibility wrapper for single-region Tagging API discovery."""
    return discovery_helpers_ops._discover_tagging_api_region(
        region,
        tag_types,
        boto_config=BOTO_CONFIG,
        arn_service=arn_service,
        global_tagging_region=GLOBAL_TAGGING_REGION,
        global_tagging_region_overrides=GLOBAL_TAGGING_REGION_OVERRIDES,
        tag_api_region_delay=TAG_API_REGION_DELAY,
    )


def discover_via_tagging_api(regions, tag_types=None, max_workers=None):
    """Compatibility wrapper for parallel Tagging API discovery."""
    return discovery_helpers_ops.discover_via_tagging_api(
        regions,
        tag_types,
        max_workers=max_workers,
        boto_config=BOTO_CONFIG,
        tag_discovery_workers=TAG_DISCOVERY_WORKERS,
        verbose=lambda: _VERBOSE,
        arn_service=arn_service,
        global_tagging_region=GLOBAL_TAGGING_REGION,
        global_tagging_region_overrides=GLOBAL_TAGGING_REGION_OVERRIDES,
        tag_api_region_delay=TAG_API_REGION_DELAY,
    )


def _fetch_tags_for_region(region, arns):
    """Compatibility wrapper for single-region tag fetch."""
    return discovery_helpers_ops._fetch_tags_for_region(
        region,
        arns,
        boto_config=BOTO_CONFIG,
        chunked=chunked,
    )


def fetch_tags_for_arns(items, max_workers=None):
    """Compatibility wrapper for parallel tag fetch."""
    return discovery_helpers_ops.fetch_tags_for_arns(
        items,
        max_workers=max_workers,
        boto_config=BOTO_CONFIG,
        chunked=chunked,
        tag_fetch_workers=TAG_FETCH_WORKERS,
        verbose=lambda: _VERBOSE,
    )


def fetch_tags_for_specific_arns(arns):
    """Compatibility wrapper for targeted tag:GetResources lookups."""
    return discovery_helpers_ops.fetch_tags_for_specific_arns(
        arns,
        boto_config=BOTO_CONFIG,
        unique_nonempty=tagger_utils.unique_nonempty,
        tagging_region_for_arn=tagging_region_for_arn,
        chunked=chunked,
        tag_api_arn_lookup_batch_size=TAG_API_ARN_LOOKUP_BATCH_SIZE,
        tag_write_max_retries=TAG_WRITE_MAX_RETRIES,
        is_throttling_exception=is_throttling_exception,
        retry_backoff_seconds=retry_backoff_seconds,
        is_access_denied_exception=is_access_denied_exception,
    )


# =============================================================================
# Unified discovery — RE primary + Tagging API supplement
# =============================================================================

def get_all_resources(regions, re_types=None, tag_types=None, discovery='auto',
                      re_region=RESOURCE_EXPLORER_DEFAULT_REGION, re_view_arn=None,
                      name_filter=None, no_cache=False,
                      cache_ttl_minutes=RE_CACHE_DEFAULT_TTL_MINUTES,
                      tag_fetch_workers=TAG_FETCH_WORKERS,
                      tag_discovery_workers=TAG_DISCOVERY_WORKERS,
                      inherit_parent_tags=False):
    """Compatibility wrapper around the extracted discovery pipeline."""
    return discovery_pipeline_ops.get_all_resources(
        regions,
        re_types=re_types,
        tag_types=tag_types,
        discovery=discovery,
        re_region=re_region,
        re_view_arn=re_view_arn,
        name_filter=name_filter,
        no_cache=no_cache,
        cache_ttl_minutes=cache_ttl_minutes,
        tag_fetch_workers=tag_fetch_workers,
        tag_discovery_workers=tag_discovery_workers,
        inherit_parent_tags=inherit_parent_tags,
        phase=_phase,
        build_re_cache_key=build_re_cache_key,
        get_account_name=get_account_name,
        re_cache_path=re_cache_path,
        load_re_cache=load_re_cache,
        discover_via_resource_explorer=discover_via_resource_explorer,
        save_re_cache=save_re_cache,
        fetch_tags_for_arns=fetch_tags_for_arns,
        discover_via_tagging_api=discover_via_tagging_api,
        service_counts_from_arns=service_counts_from_arns,
        should_skip=should_skip,
        resolve_parent_arns=resolve_parent_arns,
        fetch_tags_for_specific_arns=fetch_tags_for_specific_arns,
        fetch_tags_for_specific_arns_native=fetch_tags_for_specific_arns_native,
        classify_unresolved_inheritance_reason=classify_unresolved_inheritance_reason,
    )

# =============================================================================
# Tagger — apply new tags and/or replace existing tag values
# =============================================================================

compute_tagging_decisions = tag_write_ops.compute_tagging_decisions


is_tagresources_unsupported_error = tag_write_ops.is_tagresources_unsupported_error


def _native_client(service, region):
    """Compatibility wrapper for native adapter client caching."""
    return tag_write_ops.native_client(
        service,
        region,
        boto_config=BOTO_CONFIG,
    )


def has_native_tag_adapter(arn):
    """Compatibility wrapper for adapter availability checks."""
    return tag_write_ops.has_native_tag_adapter(
        arn,
        arn_service=arn_service,
        native_tag_adapter_services=NATIVE_TAG_ADAPTER_SERVICES,
    )


def apply_native_tag_adapter(arn, tags, fallback_region=None):
    """Compatibility wrapper for service-native tag write fallback."""
    return tag_write_ops.apply_native_tag_adapter(
        arn,
        tags,
        fallback_region=fallback_region,
        arn_service=arn_service,
        tagging_region_for_arn=tagging_region_for_arn,
        native_client_fn=_native_client,
        global_tagging_region=GLOBAL_TAGGING_REGION,
    )


def tag_resources(
    resources,
    new_tags,
    replace_rules,
    force=False,
    dry_run=False,
    native_tag_adapters=False,
    decisions=None,
):
    """Compatibility wrapper for batched tag writes."""
    return tag_write_ops.tag_resources(
        resources,
        new_tags,
        replace_rules,
        force=force,
        dry_run=dry_run,
        native_tag_adapters=native_tag_adapters,
        decisions=decisions,
        compute_tagging_decisions=compute_tagging_decisions,
        phase=_phase,
        boto_config=BOTO_CONFIG,
        global_tagging_region=GLOBAL_TAGGING_REGION,
        tag_write_batch_size=TAG_WRITE_BATCH_SIZE,
        tag_write_batch_delay=TAG_WRITE_BATCH_DELAY,
        tag_write_max_retries=TAG_WRITE_MAX_RETRIES,
        native_tag_adapter_services=NATIVE_TAG_ADAPTER_SERVICES,
        retry_backoff_seconds=retry_backoff_seconds,
        is_throttling_error=is_throttling_error,
        is_throttling_exception=is_throttling_exception,
        arn_service=arn_service,
        tagging_region_for_arn=tagging_region_for_arn,
        chunked=chunked,
    )


compute_removal_decisions = tag_write_ops.compute_removal_decisions


def remove_tags_from_resources(resources, remove_keys, dry_run=False, native_tag_adapters=False):
    """Compatibility wrapper for batched tag removals."""
    return tag_write_ops.remove_tags_from_resources(
        resources,
        remove_keys,
        dry_run=dry_run,
        native_tag_adapters=native_tag_adapters,
        phase=_phase,
        boto_config=BOTO_CONFIG,
        global_tagging_region=GLOBAL_TAGGING_REGION,
        tag_write_batch_size=TAG_WRITE_BATCH_SIZE,
        tag_write_batch_delay=TAG_WRITE_BATCH_DELAY,
        tag_write_max_retries=TAG_WRITE_MAX_RETRIES,
        native_tag_adapter_services=NATIVE_TAG_ADAPTER_SERVICES,
        retry_backoff_seconds=retry_backoff_seconds,
        is_throttling_error=is_throttling_error,
        is_throttling_exception=is_throttling_exception,
        arn_service=arn_service,
        tagging_region_for_arn=tagging_region_for_arn,
        chunked=chunked,
    )

classify_unresolved_inheritance_reason = parent_tag_read_ops.classify_unresolved_inheritance_reason


def fetch_tags_for_specific_arns_native(arns):
    """Compatibility wrapper for native parent-tag fallback lookups."""
    return parent_tag_read_ops.fetch_tags_for_specific_arns_native(
        arns,
        native_client=_native_client,
        tagging_region_for_arn=tagging_region_for_arn,
        is_access_denied_exception=is_access_denied_exception,
        global_tagging_region=GLOBAL_TAGGING_REGION,
    )

write_report = report_ops.write_report
print_type_drift_check_summary = report_ops.print_type_drift_check_summary


def print_coverage_report(stats, top_n=20, re_type_coverage=None):
    """Compatibility wrapper for coverage reporting output."""
    return report_ops.print_coverage_report(
        stats,
        top_n=top_n,
        re_type_coverage=re_type_coverage,
        phase=_phase,
        native_tag_adapter_services=NATIVE_TAG_ADAPTER_SERVICES,
    )


def print_asset_decisions(
    resources,
    skipped_items,
    new_tags,
    replace_rules,
    force,
    *,
    decisions=None,
):
    """Compatibility wrapper for TAG/SKIP decision display."""
    return report_ops.print_asset_decisions(
        resources,
        skipped_items,
        new_tags,
        replace_rules,
        force,
        decisions=decisions,
        phase=_phase,
        compute_tagging_decisions=compute_tagging_decisions,
        verbose=lambda: _VERBOSE,
    )

# =============================================================================
# Argument validation — runs BEFORE discovery to fail fast on bad input
# =============================================================================

def _arg_error(msg):
    """Print an error message and exit with code 2."""
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(2)


def validate_tag_args(tag_list, replace_list):
    """Compatibility wrapper for CLI tag/replace validation."""
    return arg_validation_ops.validate_tag_args(
        tag_list,
        replace_list,
        tag_key_max_len=TAG_KEY_MAX_LEN,
        tag_value_max_len=TAG_VALUE_MAX_LEN,
        arg_error=_arg_error,
    )


# =============================================================================
# Rollback — save and restore tag state
# =============================================================================

def save_state(
    resources,
    output_file,
    account,
    invocation,
    *,
    account_id=None,
    account_alias=None,
):
    """Persist pre-write tag state for rollback."""
    return state_ops.save_state(
        resources,
        output_file,
        account,
        invocation,
        account_id=account_id,
        account_alias=account_alias,
    )


def load_state(state_file):
    """Load a saved state file and validate structure + metadata."""
    return state_ops.load_state(state_file, arg_error=_arg_error)


def restore_tags_from_state(
    state_resources,
    dry_run=False,
    clear_empty=False,
    native_tag_adapters=False,
):
    """Restore tags from a saved state snapshot."""
    return state_ops.restore_tags_from_state(
        state_resources,
        dry_run=dry_run,
        clear_empty=clear_empty,
        native_tag_adapters=native_tag_adapters,
        fetch_tags_for_specific_arns=fetch_tags_for_specific_arns,
        tag_resources=tag_resources,
        remove_tags_from_resources=remove_tags_from_resources,
    )


def run_multi_account(args):
    """Run one child tagger process per account target."""
    return multi_account_ops.run_multi_account(
        args,
        arg_error=_arg_error,
        script_path=__file__,
        assume_role_credentials_fn=lambda **kwargs: multi_account_ops.assume_role_credentials(
            boto_config=BOTO_CONFIG,
            **kwargs,
        ),
    )


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = cli_parser_ops.build_parser(
        resource_explorer_default_region=RESOURCE_EXPLORER_DEFAULT_REGION,
        re_cache_default_ttl_minutes=RE_CACHE_DEFAULT_TTL_MINUTES,
        tag_fetch_workers=TAG_FETCH_WORKERS,
        tag_discovery_workers=TAG_DISCOVERY_WORKERS,
    )
    args = parser.parse_args()

    # Set the module-level verbosity flag immediately (before any print calls)
    global _VERBOSE
    _VERBOSE = args.verbose

    if args.full:
        args.auto_setup = True
        args.all_types = True
        args.inherit_parent_tags = True

    # Validate tag args BEFORE discovery — fail fast on bad input
    tags, replace_rules = validate_tag_args(args.tag, args.replace)

    # Validate worker counts — ThreadPoolExecutor raises ValueError on 0 workers
    if args.tag_fetch_workers < 1 or args.tag_discovery_workers < 1:
        _arg_error("--tag-fetch-workers and --tag-discovery-workers must be >= 1")

    if args.assume_role_duration < 900 or args.assume_role_duration > 43200:
        _arg_error("--assume-role-duration must be between 900 and 43200 seconds")
    if args.account_run_timeout < 0:
        _arg_error("--account-run-timeout must be >= 0 seconds")

    multi_account_related_used = any([
        args.assume_role_name,
        args.assume_role_external_id,
        args.assume_role_duration != 3600,
        args.account_run_timeout != 0,
        args.assume_role_session_prefix != 'tagger',
        args.continue_on_account_error,
    ])
    if multi_account_related_used and not args.accounts_file:
        _arg_error(
            "--assume-role-*, --account-run-timeout, and --continue-on-account-error "
            "require --accounts-file"
        )

    if args.restore_clear_empty and not args.restore_state:
        _arg_error("--restore-clear-empty requires --restore-state")
    if args.allow_restore_account_mismatch and not args.restore_state:
        _arg_error("--allow-restore-account-mismatch requires --restore-state")

    # --accounts-file: orchestrate child runs and exit before single-account flow.
    if args.accounts_file:
        run_multi_account(args)
        return

    # --restore-state: short-circuit before discovery, apply state, exit
    if args.restore_state:
        state_data = load_state(args.restore_state)
        state_resources = state_data.get('resources', [])
        state_account_id = state_data.get('account_id') or 'unknown-account'
        if state_account_id == 'unknown-account':
            if not args.allow_restore_account_mismatch:
                _arg_error(
                    "State file account ID is unknown and cannot be verified against current "
                    "credentials. Re-run with --allow-restore-account-mismatch to override."
                )
            print(
                "WARN restore account verification skipped "
                "(state file account ID unknown; override enabled).",
                file=sys.stderr,
            )
        else:
            try:
                current_account_id = get_account_identity(strict=True).get('account_id', 'unknown-account')
            except Exception as e:
                _arg_error(f"Unable to verify current account before restore: {e}")

            if current_account_id != state_account_id:
                mismatch = (
                    f"State file account ID ({state_account_id}) does not match current "
                    f"caller account ID ({current_account_id})"
                )
                if not args.allow_restore_account_mismatch:
                    _arg_error(f"{mismatch}. Use --allow-restore-account-mismatch to override.")
                print(f"WARN {mismatch}; proceeding due to override flag.", file=sys.stderr)

        restore_tags_from_state(
            state_resources,
            dry_run=args.dry_run,
            clear_empty=args.restore_clear_empty,
            native_tag_adapters=args.native_tag_adapters,
        )
        sys.exit(0)

    # Ensure per-run cache isolation in long-lived processes/tests.
    inheritance_resolver_ops.clear_caches()
    tag_write_ops.clear_native_client_cache()

    single_account_run_ops.run_single_account(
        args,
        tags,
        replace_rules,
        enable_logging=enable_logging,
        re_auto_setup=re_auto_setup,
        get_all_resources=get_all_resources,
        build_re_type_coverage=build_re_type_coverage,
        print_type_drift_check_summary=print_type_drift_check_summary,
        print_coverage_report=print_coverage_report,
        write_report=write_report,
        print_asset_decisions=print_asset_decisions,
        get_account_identity=get_account_identity,
        save_state=save_state,
        fetch_tags_for_specific_arns=fetch_tags_for_specific_arns,
        compute_removal_decisions=compute_removal_decisions,
        remove_tags_from_resources=remove_tags_from_resources,
        compute_tagging_decisions=compute_tagging_decisions,
        tag_resources=tag_resources,
        regions=REGIONS,
        re_resource_types=RE_RESOURCE_TYPES,
        tag_resource_types=TAG_RESOURCE_TYPES,
        invocation_argv=sys.argv,
    )


if __name__ == '__main__':
    main()
