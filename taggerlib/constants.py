"""
Shared constants for tagger modules.
"""

DEFAULT_GLOBAL_TAGGING_REGION = 'us-east-1'
GLOBAL_TAGGING_REGION_OVERRIDES = {
    'globalaccelerator': 'us-west-2',
}

DEFAULT_RESOURCE_EXPLORER_DEFAULT_REGION = 'us-east-1'
DEFAULT_RE_CACHE_DEFAULT_TTL_MINUTES = 0
DEFAULT_RE_CACHE_DIR = '.tagger_cache'

DEFAULT_RE_QUERY_DELAY = 0.2
DEFAULT_TAG_API_REGION_DELAY = 0.1
DEFAULT_TAG_FETCH_WORKERS = 15
DEFAULT_TAG_DISCOVERY_WORKERS = 15

DEFAULT_TAG_API_ARN_LOOKUP_BATCH_SIZE = 100
DEFAULT_TAG_WRITE_BATCH_SIZE = 10
DEFAULT_TAG_WRITE_BATCH_DELAY = 0.5
DEFAULT_TAG_WRITE_MAX_RETRIES = 6

NATIVE_TAG_ADAPTER_SERVICES = {
    'ec2',
    's3',
    'lambda',
    'logs',
    'rds',
    'elasticloadbalancing',
    'eks',
    'dynamodb',
}

LOG_GROUP_PREFIX_MAP = [
    ('/aws/lambda/',                'lambda',    'function'),
    ('/aws/codebuild/',             'codebuild', 'project'),
    ('/aws/eks/',                   'eks',       'cluster'),
    ('/aws/rds/instance/',          'rds',       'db'),
    ('/aws/rds/cluster/',           'rds',       'cluster'),
    ('/aws/ecs/',                   'ecs',       'cluster'),
    ('/aws/containerinsights/',     'ecs',       'cluster'),
    ('/aws/states/',                'states',    'stateMachine'),
    ('/aws/kinesisfirehose/',       'firehose',  'deliverystream'),
    ('/aws/cloudtrail/',            'cloudtrail', 'trail'),
    ('/aws/opensearch/',            'es',        'domain'),
    ('/aws/apigateway/',            'apigateway', 'restapis'),
    ('/aws/route53/',               'route53',   'hostedzone'),
    ('/aws/vendedlogs/traffic-log/','ec2',       'vpc'),
]
