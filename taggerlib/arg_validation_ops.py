"""
Argument validation helpers for tagger CLI.
"""


def _validate_tag_key_value(key, value, flag, *, tag_key_max_len, tag_value_max_len, arg_error):
    """Validate a single tag key+value pair against AWS constraints."""
    if key.lower().startswith('aws:') or key.lower() == 'aws':
        # key == 'aws' catches: --tag aws:foo:bar -> split gives key='aws', value='foo:bar'
        # which is almost certainly a mistake (user probably meant key='aws:foo', value='bar')
        arg_error(
            f"{flag}: tag key '{key}' uses the AWS reserved namespace — "
            f"keys starting with 'aws:' (or the bare key 'aws') cannot be set by users. "
            f"If you wrote '--tag aws:foo:bar', the key parsed as '{key}' and value as '{value}'. "
            f"AWS reserved keys like 'aws:foo' cannot be set regardless."
        )
    if len(key) > tag_key_max_len:
        arg_error(
            f"{flag}: tag key '{key}' is {len(key)} characters — "
            f"AWS maximum is {tag_key_max_len}"
        )
    if len(value) > tag_value_max_len:
        arg_error(
            f"{flag}: tag value for key '{key}' is {len(value)} characters — "
            f"AWS maximum is {tag_value_max_len}"
        )


def validate_tag_args(
    tag_list,
    replace_list,
    *,
    tag_key_max_len,
    tag_value_max_len,
    arg_error,
):
    """
    Parse and validate --tag and --replace arguments before discovery begins.
    Returns (tags_dict, replace_rules_list) on success.
    Exits with code 2 on any validation error via arg_error().
    """
    tags = {}
    replace_rules = []

    if tag_list:
        for raw in tag_list:
            if ':' not in raw:
                arg_error(f"--tag '{raw}': must be in key:value format")
            key, value = raw.split(':', 1)
            _validate_tag_key_value(
                key,
                value,
                '--tag',
                tag_key_max_len=tag_key_max_len,
                tag_value_max_len=tag_value_max_len,
                arg_error=arg_error,
            )
            tags[key] = value

    if replace_list:
        for raw in replace_list:
            if ':' not in raw or '=' not in raw:
                arg_error(f"--replace '{raw}': must be in key:old=new format")
            key, rest = raw.split(':', 1)
            if '=' not in rest:
                arg_error(f"--replace '{raw}': must be in key:old=new format")
            old, new = rest.split('=', 1)
            if not key or old == '' or new == '':
                arg_error(f"--replace '{raw}': key, old, and new must all be non-empty")
            _validate_tag_key_value(
                key,
                new,
                '--replace (new value)',
                tag_key_max_len=tag_key_max_len,
                tag_value_max_len=tag_value_max_len,
                arg_error=arg_error,
            )
            replace_rules.append((key, old, new))

    return tags, replace_rules
