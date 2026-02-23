"""
Service-native parent tag read helpers for inheritance fallback.

Extracted from tagger.py to reduce monolith complexity while preserving behavior.
"""

from botocore.exceptions import ClientError

from .utils import (
    arn_parts as _arn_parts,
    arn_resource_id as _arn_resource_id,
    s3_bucket_from_arn as _s3_bucket_from_arn,
    unique_nonempty as _unique_nonempty,
)

def _normalize_ecs_tags(tag_items):
    """Convert ECS tag list [{'key','value'}] to {'Key': 'Value'} dict."""
    out = {}
    for item in (tag_items or []):
        key = item.get('key') if isinstance(item, dict) else None
        value = item.get('value') if isinstance(item, dict) else None
        if key is not None and value is not None:
            out[str(key)] = str(value)
    return out


def _normalize_kv_tag_list(tag_items):
    """Convert generic [{'Key','Value'}] lists to dict."""
    out = {}
    for item in (tag_items or []):
        if not isinstance(item, dict):
            continue
        key = item.get('Key')
        value = item.get('Value')
        if key is not None and value is not None:
            out[str(key)] = str(value)
    return out


def _extract_asg_name_from_arn(arn):
    """
    Extract ASG name from ARN forms like:
      autoScalingGroup:uuid:autoScalingGroupName/<name>
      autoScalingGroup/<name>
    """
    resource = _arn_parts(arn)[5]
    if 'autoScalingGroupName/' in resource:
        return resource.split('autoScalingGroupName/', 1)[1].split('/')[0]
    if resource.startswith('autoScalingGroup/'):
        return resource[len('autoScalingGroup/'):].split('/')[0]
    return None


def _resolved_status_for_tags(tags):
    """Return inheritance lookup status label for a tag dict."""
    return 'resolved_with_tags' if tags else 'resolved_no_tags'


def _fetch_parent_tags_native(
    arn,
    *,
    native_client,
    tagging_region_for_arn,
    is_access_denied_exception,
    global_tagging_region,
):
    """
    Best-effort service-native parent tag read for a single ARN.

    Returns (tags, status, error_message) where status is one of:
      resolved_with_tags, resolved_no_tags, not_found,
      unsupported_service, unsupported_arn_format,
      access_denied, error
    """
    parts = _arn_parts(arn)
    service = parts[2]
    region = tagging_region_for_arn(arn)
    resource = parts[5]

    try:
        if service == 'lambda':
            client = native_client('lambda', region)
            tags = dict(client.list_tags(Resource=arn).get('Tags') or {})
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'ec2':
            resource_id = _arn_resource_id(arn)
            if not resource_id:
                return {}, 'unsupported_arn_format', 'cannot parse EC2 resource ID'
            client = native_client('ec2', region)
            response = client.describe_tags(
                Filters=[{'Name': 'resource-id', 'Values': [resource_id]}]
            )
            tags = _normalize_kv_tag_list(response.get('Tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'rds':
            client = native_client('rds', region)
            response = client.list_tags_for_resource(ResourceName=arn)
            tags = _normalize_kv_tag_list(response.get('TagList'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'ecs':
            client = native_client('ecs', region)
            response = client.list_tags_for_resource(resourceArn=arn)
            tags = _normalize_ecs_tags(response.get('tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'eks':
            client = native_client('eks', region)
            response = client.list_tags_for_resource(resourceArn=arn)
            tags = dict(response.get('tags') or {})
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'states':
            client = native_client('stepfunctions', region)
            response = client.list_tags_for_resource(resourceArn=arn)
            tags = _normalize_kv_tag_list(response.get('tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'codebuild':
            client = native_client('codebuild', region)
            response = client.list_tags_for_resource(resourceArn=arn)
            tags = _normalize_kv_tag_list(response.get('tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'cloudtrail':
            client = native_client('cloudtrail', region)
            response = client.list_tags(ResourceIdList=[arn])
            entries = response.get('ResourceTagList') or []
            tags = {}
            if entries:
                tags = _normalize_kv_tag_list(entries[0].get('TagsList'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'firehose':
            if not resource.startswith('deliverystream/'):
                return {}, 'unsupported_arn_format', 'not a deliverystream ARN'
            stream_name = resource[len('deliverystream/'):]
            client = native_client('firehose', region)
            response = client.list_tags_for_delivery_stream(DeliveryStreamName=stream_name)
            tags = _normalize_kv_tag_list(response.get('Tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'elasticfilesystem':
            if not resource.startswith('file-system/'):
                return {}, 'unsupported_arn_format', 'not an EFS file-system ARN'
            fs_id = resource[len('file-system/'):]
            client = native_client('efs', region)
            response = client.list_tags_for_resource(ResourceId=fs_id)
            tags = _normalize_kv_tag_list(response.get('Tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'elasticloadbalancing':
            tag_list = []
            if (
                resource.startswith('loadbalancer/')
                and not resource.startswith('loadbalancer/app/')
                and not resource.startswith('loadbalancer/net/')
                and not resource.startswith('loadbalancer/gwy/')
            ):
                lb_name = resource.split('/', 1)[1]
                if not lb_name:
                    return {}, 'unsupported_arn_format', 'cannot parse classic ELB name'
                client = native_client('elb', region)
                response = client.describe_tags(LoadBalancerNames=[lb_name])
                descriptions = response.get('TagDescriptions') or []
                if descriptions:
                    tag_list = descriptions[0].get('Tags') or []
            else:
                client = native_client('elbv2', region)
                response = client.describe_tags(ResourceArns=[arn])
                descriptions = response.get('TagDescriptions') or []
                if descriptions:
                    tag_list = descriptions[0].get('Tags') or []
            tags = _normalize_kv_tag_list(tag_list)
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'autoscaling':
            group_name = _extract_asg_name_from_arn(arn)
            if not group_name:
                return {}, 'unsupported_arn_format', 'cannot parse ASG name'
            client = native_client('autoscaling', region)
            response = client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[group_name],
            )
            groups = response.get('AutoScalingGroups') or []
            if not groups:
                return {}, 'not_found', ''
            tags = _normalize_kv_tag_list(groups[0].get('Tags'))
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'dlm':
            if not resource.startswith('policy/'):
                return {}, 'unsupported_arn_format', 'not a DLM policy ARN'
            policy_id = resource[len('policy/'):]
            client = native_client('dlm', region)
            response = client.get_lifecycle_policy(PolicyId=policy_id)
            policy = response.get('Policy') if isinstance(response, dict) else {}
            tags = dict((policy or {}).get('Tags') or {})
            return tags, _resolved_status_for_tags(tags), ''

        if service == 'route53':
            if not resource.startswith('hostedzone/'):
                return {}, 'unsupported_arn_format', 'not a Route53 hosted zone ARN'
            zone_id = resource[len('hostedzone/'):]
            client = native_client('route53', global_tagging_region)
            response = client.list_tags_for_resource(
                ResourceType='hostedzone',
                ResourceId=zone_id,
            )
            tag_set = (response.get('ResourceTagSet') or {}).get('Tags') or []
            tags = _normalize_kv_tag_list(tag_set)
            return tags, _resolved_status_for_tags(tags), ''

        if service == 's3':
            bucket = _s3_bucket_from_arn(arn)
            if not bucket:
                return {}, 'unsupported_arn_format', 'not an S3 bucket ARN'
            client = native_client('s3', global_tagging_region)
            try:
                response = client.get_bucket_tagging(Bucket=bucket)
                tags = _normalize_kv_tag_list(response.get('TagSet'))
                return tags, _resolved_status_for_tags(tags), ''
            except ClientError as e:
                code = ((e.response or {}).get('Error') or {}).get('Code', '')
                if code in {'NoSuchTagSet', 'NoSuchTagSetError'}:
                    return {}, 'resolved_no_tags', ''
                raise

        return {}, 'unsupported_service', ''

    except Exception as e:
        if isinstance(e, ClientError):
            err = e.response.get('Error', {})
            code = (err.get('Code') or '').strip()
            msg = err.get('Message') or str(e)
            msg_l = msg.lower()
            if code in {'ResourceNotFoundException', 'NotFoundException'} or 'not found' in msg_l:
                return {}, 'not_found', msg
            if is_access_denied_exception(e):
                return {}, 'access_denied', msg
            return {}, 'error', msg

        if is_access_denied_exception(e):
            return {}, 'access_denied', str(e)
        return {}, 'error', str(e)


def fetch_tags_for_specific_arns_native(
    arns,
    *,
    native_client,
    tagging_region_for_arn,
    is_access_denied_exception,
    global_tagging_region,
):
    """
    Fetch tags for specific ARNs using service-native read APIs.
    Used only as fallback when tag:GetResources misses parent ARNs.

    Returns (tags_by_arn, status_by_arn, errors).
    """
    tags_by_arn = {}
    status_by_arn = {}
    errors = []

    for arn in _unique_nonempty(arns):
        tags, status, err = _fetch_parent_tags_native(
            arn,
            native_client=native_client,
            tagging_region_for_arn=tagging_region_for_arn,
            is_access_denied_exception=is_access_denied_exception,
            global_tagging_region=global_tagging_region,
        )
        status_by_arn[arn] = status
        if status in {'resolved_with_tags', 'resolved_no_tags'}:
            tags_by_arn[arn] = tags
        elif status in {'access_denied', 'error'} and err:
            errors.append(f"native parent lookup ({arn}) — {err}")

    return tags_by_arn, status_by_arn, errors


def classify_unresolved_inheritance_reason(parent_arns, tagapi_status_by_arn, native_status_by_arn):
    """
    Classify why parent-tag inheritance remained unresolved for a child resource.
    """
    statuses = []
    for arn in parent_arns:
        if arn in tagapi_status_by_arn:
            statuses.append(tagapi_status_by_arn[arn])
        if arn in native_status_by_arn:
            statuses.append(native_status_by_arn[arn])

    if any(s == 'access_denied' for s in statuses):
        return 'lookup_access_denied'
    if any(s == 'error' for s in statuses):
        return 'lookup_error'
    if any(s in {'unsupported_service', 'unsupported_arn_format'} for s in statuses):
        return 'unsupported_parent_type'
    if any(s == 'not_found' for s in statuses):
        return 'parent_not_visible'
    return 'parent_unresolved'
