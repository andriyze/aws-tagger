"""
Inheritance parent-resolution logic extracted from tagger.py.

This module resolves child ARNs to parent ARNs for --inherit-parent-tags.
Refactored to explicit dependency injection (no module-level runtime wiring).
"""

import re

import boto3

from .constants import LOG_GROUP_PREFIX_MAP
from .utils import (
    arn_parts as _arn_parts,
    extract_first_id as _extract_first_id,
    is_verbose as _is_verbose,
    unique_nonempty as _unique_nonempty,
)

DEFAULT_LOG_GROUP_PREFIX_MAP = LOG_GROUP_PREFIX_MAP

_EB_ENV_ARN_CACHE = {}
_ASG_ARN_CACHE = {}
_ENI_PARENT_CACHE = {}
_DLM_POLICY_ARN_CACHE = {}
_EC2_SNAPSHOT_PARENT_CACHE = {}
_EC2_IMAGE_PARENT_CACHE = {}
_SG_RULE_PARENT_CACHE = {}

def clear_caches():
    """Reset module-level best-effort lookup caches."""
    _EB_ENV_ARN_CACHE.clear()
    _ASG_ARN_CACHE.clear()
    _ENI_PARENT_CACHE.clear()
    _DLM_POLICY_ARN_CACHE.clear()
    _EC2_SNAPSHOT_PARENT_CACHE.clear()
    _EC2_IMAGE_PARENT_CACHE.clear()
    _SG_RULE_PARENT_CACHE.clear()


def _log_nonfatal_parent_lookup_error(context, exc, *, verbose=False):
    """
    Emit a concise warning for best-effort parent resolution failures.
    Kept verbose-only to avoid noisy default output.
    """
    if not _is_verbose(verbose):
        return
    print(f"    WARN {context} — {exc}")


def _resolve_elasticbeanstalk_environment_arn(
    region,
    env_name,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve Elastic Beanstalk environment name to ARN (cached, best effort)."""
    cache_key = (region, env_name)
    if cache_key in _EB_ENV_ARN_CACHE:
        return _EB_ENV_ARN_CACHE[cache_key]
    resolved = None
    try:
        client = boto3.client('elasticbeanstalk', region_name=region, config=boto_config)
        response = client.describe_environments(
            EnvironmentNames=[env_name],
            IncludeDeleted=False,
        )
        for env in response.get('Environments', []):
            if env.get('EnvironmentName') == env_name and env.get('EnvironmentArn'):
                resolved = env['EnvironmentArn']
                break
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (elasticbeanstalk:DescribeEnvironments, {env_name})",
            e,
            verbose=verbose,
        )
    _EB_ENV_ARN_CACHE[cache_key] = resolved
    return resolved


def _resolve_asg_arn(region, group_name, *, boto_config=None, verbose=False):
    """Resolve Auto Scaling group name to ARN (cached, best effort)."""
    cache_key = (region, group_name)
    if cache_key in _ASG_ARN_CACHE:
        return _ASG_ARN_CACHE[cache_key]
    resolved = None
    try:
        client = boto3.client('autoscaling', region_name=region, config=boto_config)
        response = client.describe_auto_scaling_groups(AutoScalingGroupNames=[group_name])
        groups = response.get('AutoScalingGroups', [])
        if groups:
            resolved = groups[0].get('AutoScalingGroupARN')
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (autoscaling:DescribeAutoScalingGroups, {group_name})",
            e,
            verbose=verbose,
        )
    _ASG_ARN_CACHE[cache_key] = resolved
    return resolved


def _resolve_dlm_policy_arn(
    region,
    policy_id,
    partition='aws',
    account=None,
    *,
    boto_config=None,
    verbose=False,
):
    """
    Resolve a DLM lifecycle policy ID to policy ARN (cached, best effort).
    Falls back to ARN construction when the policy lookup is denied/unavailable.
    """
    cache_key = (region, policy_id, partition, account or '')
    if cache_key in _DLM_POLICY_ARN_CACHE:
        return _DLM_POLICY_ARN_CACHE[cache_key]

    resolved = None
    try:
        client = boto3.client('dlm', region_name=region, config=boto_config)
        response = client.get_lifecycle_policy(PolicyId=policy_id)
        policy = response.get('Policy', {}) if isinstance(response, dict) else {}
        resolved = policy.get('PolicyArn')
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (dlm:GetLifecyclePolicy, {policy_id})",
            e,
            verbose=verbose,
        )

    if not resolved and account:
        resolved = f"arn:{partition}:dlm:{region}:{account}:policy/{policy_id}"

    _DLM_POLICY_ARN_CACHE[cache_key] = resolved
    return resolved


def resolve_log_group_parent_arns(
    child_arn,
    *,
    boto_config=None,
    verbose=False,
    log_group_prefix_map=None,
):
    """
    Resolve a CloudWatch log group ARN to the ARN of its owning service resource.
    Returns parent ARN candidates in priority order.
    """
    log_group_prefix_map = list(log_group_prefix_map or DEFAULT_LOG_GROUP_PREFIX_MAP)

    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('log-group:'):
        return []
    log_group_name = resource_part[len('log-group:'):]

    partition = parts[1]
    region = parts[3]
    account = parts[4]

    if log_group_name.startswith('/aws/rds/instance/'):
        name = log_group_name[len('/aws/rds/instance/'):].split('/')[0]
        if name:
            return [f"arn:{partition}:rds:{region}:{account}:db:{name}"]

    if log_group_name.startswith('/aws/rds/cluster/'):
        name = log_group_name[len('/aws/rds/cluster/'):].split('/')[0]
        if name:
            return [f"arn:{partition}:rds:{region}:{account}:cluster:{name}"]

    if log_group_name.startswith('/aws/elasticbeanstalk/'):
        env_name = log_group_name[len('/aws/elasticbeanstalk/'):].split('/')[0]
        if env_name:
            eb_arn = _resolve_elasticbeanstalk_environment_arn(
                region,
                env_name,
                boto_config=boto_config,
                verbose=verbose,
            )
            return [eb_arn] if eb_arn else []

    for prefix, service, rtype in log_group_prefix_map:
        if not log_group_name.startswith(prefix):
            continue
        name = log_group_name[len(prefix):].split('/')[0]
        if not name:
            return []

        if service == 'states':
            return [f"arn:{partition}:{service}:{region}:{account}:stateMachine:{name}"]
        if service == 'route53':
            return [f"arn:{partition}:route53:::hostedzone/{name}"]
        if service == 'apigateway':
            return [
                f"arn:{partition}:apigateway:{region}::/restapis/{name}",
                f"arn:{partition}:apigateway:{region}::/apis/{name}",
            ]
        if service == 'ec2' and rtype == 'vpc':
            if name.startswith('vpc-'):
                return [f"arn:{partition}:ec2:{region}:{account}:vpc/{name}"]
            return []
        if service in ('firehose',):
            return [f"arn:{partition}:{service}:{region}:{account}:deliverystream/{name}"]
        if service in ('codebuild',):
            return [f"arn:{partition}:{service}:{region}:{account}:project/{name}"]
        if service in ('cloudtrail',):
            return [f"arn:{partition}:{service}:{region}:{account}:trail/{name}"]
        if service == 'eks':
            return [f"arn:{partition}:{service}:{region}:{account}:cluster/{name}"]
        if service == 'ecs':
            if prefix == '/aws/containerinsights/':
                return [
                    f"arn:{partition}:ecs:{region}:{account}:cluster/{name}",
                    f"arn:{partition}:eks:{region}:{account}:cluster/{name}",
                ]
            return [f"arn:{partition}:{service}:{region}:{account}:cluster/{name}"]
        if service == 'lambda':
            return [f"arn:{partition}:{service}:{region}:{account}:function:{name}"]
        if service == 'es':
            return [f"arn:{partition}:{service}:{region}:{account}:domain/{name}"]
        return [f"arn:{partition}:{service}:{region}:{account}:{rtype}/{name}"]

    return []


def resolve_eks_nodegroup_parent_arn(child_arn):
    """Resolve an EKS node group ARN to its EKS cluster ARN."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('nodegroup/'):
        return None
    segments = resource_part.split('/')
    if len(segments) < 2 or not segments[1]:
        return None
    partition = parts[1]
    region = parts[3]
    account = parts[4]
    return f"arn:{partition}:eks:{region}:{account}:cluster/{segments[1]}"


def resolve_alb_listener_parent_arn(child_arn):
    """Resolve ALB/NLB listener or listener-rule ARN to load balancer ARN."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]

    for child_prefix in ('listener-rule/', 'listener/'):
        if not resource_part.startswith(child_prefix):
            continue
        remainder = resource_part[len(child_prefix):]
        segments = remainder.split('/')
        if len(segments) < 3:
            return None
        lb_type = segments[0]
        lb_name = segments[1]
        lb_id = segments[2]
        partition = parts[1]
        region = parts[3]
        account = parts[4]
        return (
            f"arn:{partition}:elasticloadbalancing:{region}:{account}:"
            f"loadbalancer/{lb_type}/{lb_name}/{lb_id}"
        )

    return None


def resolve_efs_access_point_parent_arn(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve EFS access point ARN to its EFS file system ARN."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('access-point/'):
        return None
    ap_id = resource_part[len('access-point/'):]
    if not ap_id:
        return None
    try:
        client = boto3.client('efs', region_name=region, config=boto_config)
        response = client.describe_access_points(AccessPointId=ap_id)
        for ap in response.get('AccessPoints', []):
            fs_id = ap.get('FileSystemId')
            if fs_id:
                partition = parts[1]
                account = parts[4]
                return f"arn:{partition}:elasticfilesystem:{region}:{account}:file-system/{fs_id}"
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (elasticfilesystem:DescribeAccessPoints, {ap_id})",
            e,
            verbose=verbose,
        )
    return None


def resolve_rds_snapshot_parent_arn(child_arn):
    """Resolve automated RDS snapshot ARN to source DB/cluster ARN."""
    parts = _arn_parts(child_arn)
    partition = parts[1]
    region = parts[3]
    account = parts[4]
    resource_part = parts[5]

    if ':snapshot:rds:' in child_arn or resource_part.startswith('snapshot:rds:'):
        snap_id = resource_part.split(':')[-1]
        m = re.match(r'^(.+)-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$', snap_id)
        if m:
            return f"arn:{partition}:rds:{region}:{account}:db:{m.group(1)}"

    if ':cluster-snapshot:rds:' in child_arn or resource_part.startswith('cluster-snapshot:rds:'):
        snap_id = resource_part.split(':')[-1]
        m = re.match(r'^(.+)-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$', snap_id)
        if m:
            return f"arn:{partition}:rds:{region}:{account}:cluster:{m.group(1)}"

    return None


def resolve_backup_recovery_point_parent_arn(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve backup recovery point ARN to source resource ARN."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('recovery-point:'):
        return None
    remainder = resource_part[len('recovery-point:'):]
    segments = remainder.split('/', 1)
    if len(segments) < 2:
        return None
    vault_name = segments[0]
    try:
        client = boto3.client('backup', region_name=region, config=boto_config)
        response = client.describe_recovery_point(
            BackupVaultName=vault_name,
            RecoveryPointArn=child_arn,
        )
        source_arn = response.get('SourceResourceArn') or response.get('ResourceArn')
        return source_arn if source_arn else None
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (backup:DescribeRecoveryPoint, {child_arn})",
            e,
            verbose=verbose,
        )
    return None


def resolve_network_interface_parent_arns(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve ENI ARN to likely parent resources using DescribeNetworkInterfaces."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('network-interface/'):
        return []
    eni_id = resource_part[len('network-interface/'):]
    if not eni_id:
        return []
    if eni_id in _ENI_PARENT_CACHE:
        return _ENI_PARENT_CACHE[eni_id]

    partition = parts[1]
    account = parts[4]
    candidates = []
    try:
        client = boto3.client('ec2', region_name=region, config=boto_config)
        response = client.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
        interfaces = response.get('NetworkInterfaces', [])
        if interfaces:
            eni = interfaces[0]
            attachment = eni.get('Attachment') or {}
            instance_id = attachment.get('InstanceId')
            if instance_id:
                candidates.append(f"arn:{partition}:ec2:{region}:{account}:instance/{instance_id}")

            desc = eni.get('Description') or ''
            requester_id = eni.get('RequesterId') or ''
            interface_type = (eni.get('InterfaceType') or '').lower()
            search_text = f"{desc} {requester_id}".strip()

            nat_id = _extract_first_id(search_text, r'\bnat-[0-9a-f]+\b')
            if nat_id:
                candidates.append(
                    f"arn:{partition}:ec2:{region}:{account}:natgateway/{nat_id}"
                )

            lb_match = re.search(r'\b(app|net|gwy)/([^/\s]+)/([0-9a-z]+)\b', search_text)
            if lb_match:
                lb_type, lb_name, lb_id = lb_match.groups()
                candidates.append(
                    f"arn:{partition}:elasticloadbalancing:{region}:{account}:"
                    f"loadbalancer/{lb_type}/{lb_name}/{lb_id}"
                )

            if interface_type == 'vpc_endpoint':
                vpce_id = _extract_first_id(search_text, r'\bvpce-[0-9a-f]+\b')
                if vpce_id:
                    candidates.append(
                        f"arn:{partition}:ec2:{region}:{account}:vpc-endpoint/{vpce_id}"
                    )

            if interface_type == 'efs':
                fs_id = _extract_first_id(search_text, r'\bfs-[0-9a-f]+\b')
                if fs_id:
                    candidates.append(
                        f"arn:{partition}:elasticfilesystem:{region}:{account}:file-system/{fs_id}"
                    )

            if interface_type == 'ec2_instance_connect_endpoint':
                eice_id = _extract_first_id(search_text, r'\beice-[0-9a-f]+\b')
                if eice_id:
                    candidates.append(
                        f"arn:{partition}:ec2:{region}:{account}:instance-connect-endpoint/{eice_id}"
                    )
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (ec2:DescribeNetworkInterfaces, {eni_id})",
            e,
            verbose=verbose,
        )

    resolved = _unique_nonempty(candidates)
    _ENI_PARENT_CACHE[eni_id] = resolved
    return resolved


def resolve_ec2_snapshot_parent_arns(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve EC2 snapshot ARN to likely parent resources."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('snapshot/'):
        return []
    snapshot_id = resource_part[len('snapshot/'):]
    if not snapshot_id:
        return []

    cache_key = (region, snapshot_id)
    if cache_key in _EC2_SNAPSHOT_PARENT_CACHE:
        return _EC2_SNAPSHOT_PARENT_CACHE[cache_key]

    partition = parts[1]
    account = parts[4]
    candidates = []
    try:
        client = boto3.client('ec2', region_name=region, config=boto_config)
        response = client.describe_snapshots(SnapshotIds=[snapshot_id])
        snapshots = response.get('Snapshots', [])
        if snapshots:
            snapshot = snapshots[0]
            transfer_type = snapshot.get('TransferType')
            if not transfer_type:
                volume_id = snapshot.get('VolumeId') or ''
                if re.fullmatch(r'vol-[0-9a-f]+', volume_id):
                    candidates.append(
                        f"arn:{partition}:ec2:{region}:{account}:volume/{volume_id}"
                    )
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (ec2:DescribeSnapshots, {snapshot_id})",
            e,
            verbose=verbose,
        )

    resolved = _unique_nonempty(candidates)
    _EC2_SNAPSHOT_PARENT_CACHE[cache_key] = resolved
    return resolved


def resolve_ec2_image_parent_arns(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve EC2 AMI ARN to likely parent resources."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('image/'):
        return []
    image_id = resource_part[len('image/'):]
    if not image_id:
        return []

    cache_key = (region, image_id)
    if cache_key in _EC2_IMAGE_PARENT_CACHE:
        return _EC2_IMAGE_PARENT_CACHE[cache_key]

    partition = parts[1]
    account = parts[4]
    candidates = []
    try:
        client = boto3.client('ec2', region_name=region, config=boto_config)
        response = client.describe_images(ImageIds=[image_id])
        images = response.get('Images', [])
        if images:
            image = images[0]
            source_instance_id = image.get('SourceInstanceId') or ''
            if re.fullmatch(r'i-[0-9a-f]+', source_instance_id):
                candidates.append(
                    f"arn:{partition}:ec2:{region}:{account}:instance/{source_instance_id}"
                )
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (ec2:DescribeImages, {image_id})",
            e,
            verbose=verbose,
        )

    resolved = _unique_nonempty(candidates)
    _EC2_IMAGE_PARENT_CACHE[cache_key] = resolved
    return resolved


def resolve_security_group_rule_parent_arn(
    child_arn,
    region,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve EC2 security-group-rule ARN to owning security group ARN."""
    parts = _arn_parts(child_arn)
    resource_part = parts[5]
    if not resource_part.startswith('security-group-rule/'):
        return None
    rule_id = resource_part[len('security-group-rule/'):]
    if not rule_id:
        return None

    cache_key = (region, rule_id)
    if cache_key in _SG_RULE_PARENT_CACHE:
        return _SG_RULE_PARENT_CACHE[cache_key]

    resolved = None
    try:
        client = boto3.client('ec2', region_name=region, config=boto_config)
        response = client.describe_security_group_rules(SecurityGroupRuleIds=[rule_id])
        rules = response.get('SecurityGroupRules', [])
        if rules:
            group_id = rules[0].get('GroupId') or ''
            if re.fullmatch(r'sg-[0-9a-f]+', group_id):
                partition = parts[1]
                account = parts[4]
                resolved = f"arn:{partition}:ec2:{region}:{account}:security-group/{group_id}"
    except Exception as e:
        _log_nonfatal_parent_lookup_error(
            f"inheritance parent lookup (ec2:DescribeSecurityGroupRules, {rule_id})",
            e,
            verbose=verbose,
        )

    _SG_RULE_PARENT_CACHE[cache_key] = resolved
    return resolved


def resolve_managed_tag_parent_arns(
    child_arn,
    region,
    child_tags,
    *,
    boto_config=None,
    verbose=False,
):
    """Resolve parent candidates from AWS-managed linkage tags on the child."""
    if not child_tags:
        return []

    parts = _arn_parts(child_arn)
    partition = parts[1]
    account = parts[4]
    candidates = []

    backup_source = child_tags.get('aws:backup:source-resource')
    if backup_source and backup_source.startswith('arn:'):
        candidates.append(backup_source)

    launch_template_id = child_tags.get('aws:ec2launchtemplate:id')
    if launch_template_id and ':ec2:' in child_arn and ':instance/' in child_arn:
        candidates.append(
            f"arn:{partition}:ec2:{region}:{account}:launch-template/{launch_template_id}"
        )

    asg_name = child_tags.get('aws:autoscaling:groupName')
    if asg_name and ':ec2:' in child_arn and ':instance/' in child_arn:
        asg_arn = _resolve_asg_arn(
            region,
            asg_name,
            boto_config=boto_config,
            verbose=verbose,
        )
        if asg_arn:
            candidates.append(asg_arn)

    dlm_policy_id = child_tags.get('aws:dlm:lifecycle-policy-id')
    if dlm_policy_id and ':ec2:' in child_arn and (
        ':snapshot/' in child_arn or ':image/' in child_arn
    ):
        dlm_policy_arn = _resolve_dlm_policy_arn(
            region,
            dlm_policy_id,
            partition=partition,
            account=account,
            boto_config=boto_config,
            verbose=verbose,
        )
        if dlm_policy_arn:
            candidates.append(dlm_policy_arn)

    return _unique_nonempty(candidates)


def resolve_parent_arns(
    child_arn,
    region,
    child_tags=None,
    *,
    boto_config=None,
    verbose=False,
    log_group_prefix_map=None,
):
    """
    Dispatch to parent ARN resolvers based on child ARN signature and tags.
    Returns parent ARN candidates in priority order.
    """
    candidates = []

    candidates.extend(
        resolve_managed_tag_parent_arns(
            child_arn,
            region,
            child_tags,
            boto_config=boto_config,
            verbose=verbose,
        )
    )

    if ':logs:' in child_arn and ':log-group:' in child_arn:
        candidates.extend(
            resolve_log_group_parent_arns(
                child_arn,
                boto_config=boto_config,
                verbose=verbose,
                log_group_prefix_map=log_group_prefix_map,
            )
        )

    if ':eks:' in child_arn and ':nodegroup/' in child_arn:
        candidates.append(resolve_eks_nodegroup_parent_arn(child_arn))

    if ':elasticloadbalancing:' in child_arn and (
        ':listener/' in child_arn or ':listener-rule/' in child_arn
    ):
        candidates.append(resolve_alb_listener_parent_arn(child_arn))

    if ':elasticfilesystem:' in child_arn and ':access-point/' in child_arn:
        candidates.append(
            resolve_efs_access_point_parent_arn(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    if ':rds:' in child_arn and (
        ':snapshot:rds:' in child_arn or ':cluster-snapshot:rds:' in child_arn
    ):
        candidates.append(resolve_rds_snapshot_parent_arn(child_arn))

    if ':backup:' in child_arn and ':recovery-point:' in child_arn:
        candidates.append(
            resolve_backup_recovery_point_parent_arn(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    if ':ec2:' in child_arn and ':snapshot/' in child_arn:
        candidates.extend(
            resolve_ec2_snapshot_parent_arns(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    if ':ec2:' in child_arn and ':image/' in child_arn:
        candidates.extend(
            resolve_ec2_image_parent_arns(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    if ':ec2:' in child_arn and ':security-group-rule/' in child_arn:
        candidates.append(
            resolve_security_group_rule_parent_arn(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    if ':ec2:' in child_arn and ':network-interface/' in child_arn:
        candidates.extend(
            resolve_network_interface_parent_arns(
                child_arn,
                region,
                boto_config=boto_config,
                verbose=verbose,
            )
        )

    return _unique_nonempty(candidates)
