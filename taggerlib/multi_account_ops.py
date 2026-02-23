"""
Multi-account orchestration helpers for tagger.
"""

import os
import re
import subprocess
import sys
import time

import boto3


MULTI_ACCOUNT_ARGS_WITH_VALUE = {
    '--accounts-file',
    '--assume-role-name',
    '--assume-role-session-prefix',
    '--assume-role-external-id',
    '--assume-role-duration',
    '--account-run-timeout',
}

MULTI_ACCOUNT_BOOLEAN_ARGS = {
    '--continue-on-account-error',
}


def parse_role_arn_account_id(role_arn):
    """Extract 12-digit account ID from an IAM role ARN, or None."""
    match = re.match(r'^arn:[^:]+:iam::(\d{12}):role/.+$', role_arn or '')
    return match.group(1) if match else None


def build_role_arn(account_id, role_name):
    """Build a standard IAM role ARN for an account ID + role name."""
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def path_with_suffix(path, suffix):
    """Return path with -<suffix> inserted before the extension."""
    base, ext = os.path.splitext(path)
    if ext:
        return f"{base}-{suffix}{ext}"
    return f"{path}-{suffix}"


def sanitize_sts_session_name(name):
    """Sanitize session name to the character set accepted by STS."""
    safe = re.sub(r'[^A-Za-z0-9+=,.@-]+', '-', name or '').strip('-')
    if not safe:
        safe = 'tagger-session'
    return safe[:64]


def strip_multi_account_args(argv):
    """
    Remove multi-account orchestration flags from child argv so each child run
    executes in regular single-account mode.
    """
    out = []
    skip_next = False
    for idx, token in enumerate(argv):
        if skip_next:
            skip_next = False
            continue
        if token in MULTI_ACCOUNT_BOOLEAN_ARGS:
            continue
        if token in MULTI_ACCOUNT_ARGS_WITH_VALUE:
            skip_next = True
            continue
        if token.startswith('--'):
            key = token.split('=', 1)[0]
            if key in MULTI_ACCOUNT_ARGS_WITH_VALUE:
                # --flag=value form
                continue
        out.append(token)
    return out


def upsert_cli_value(argv, flag, value):
    """
    Return argv with --flag value inserted or updated.

    Supports:
      --flag value
      --flag=value
    """
    updated = list(argv)
    i = 0
    while i < len(updated):
        token = updated[i]
        if token == flag:
            if i + 1 < len(updated):
                updated[i + 1] = value
            else:
                updated.append(value)
            return updated
        if token.startswith(flag + '='):
            updated[i] = f"{flag}={value}"
            return updated
        i += 1

    updated.extend([flag, value])
    return updated


def load_account_targets(accounts_file, default_role_name, arg_error):
    """
    Parse an accounts file into unique targets:
      - 12-digit account ID (requires default_role_name)
      - full role ARN
      - "<account_id> <role_name_or_arn>"
      - same with comma separator
    """
    targets = []
    seen_role_arns = set()

    try:
        with open(accounts_file, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        arg_error(f"Accounts file not found: {accounts_file}")
    except Exception as e:
        arg_error(f"Cannot read accounts file {accounts_file}: {e}")

    for lineno, raw in enumerate(lines, start=1):
        line = raw.split('#', 1)[0].strip()
        if not line:
            continue

        account_id = None
        role_arn = None
        parts = re.split(r'[\s,]+', line)

        if len(parts) == 1:
            token = parts[0]
            if token.startswith('arn:'):
                role_arn = token
                account_id = parse_role_arn_account_id(role_arn)
                if not account_id:
                    arg_error(
                        f"{accounts_file}:{lineno}: invalid role ARN "
                        f"(expected IAM role ARN): {token}"
                    )
            elif re.fullmatch(r'\d{12}', token):
                account_id = token
                if not default_role_name:
                    arg_error(
                        f"{accounts_file}:{lineno}: account ID '{account_id}' requires "
                        f"--assume-role-name (or provide a full role ARN in the file)"
                    )
                role_arn = build_role_arn(account_id, default_role_name)
            else:
                arg_error(
                    f"{accounts_file}:{lineno}: unrecognized target '{token}' "
                    f"(expected 12-digit account ID or role ARN)"
                )

        elif len(parts) == 2 and re.fullmatch(r'\d{12}', parts[0]):
            account_id = parts[0]
            second = parts[1]
            if second.startswith('arn:'):
                role_arn = second
                arn_account = parse_role_arn_account_id(role_arn)
                if arn_account and arn_account != account_id:
                    arg_error(
                        f"{accounts_file}:{lineno}: account ID '{account_id}' does not match "
                        f"role ARN account '{arn_account}'"
                    )
            else:
                role_arn = build_role_arn(account_id, second)
        else:
            arg_error(
                f"{accounts_file}:{lineno}: unsupported format '{line}'"
            )

        if role_arn in seen_role_arns:
            continue
        seen_role_arns.add(role_arn)

        targets.append({
            'account_id': account_id,
            'role_arn': role_arn,
        })

    if not targets:
        arg_error(f"No valid account targets found in {accounts_file}")

    return targets


def assume_role_credentials(role_arn, session_name, external_id=None, duration_seconds=3600, *, boto_config):
    """Assume a role and return temporary credentials dict."""
    sts = boto3.client('sts', config=boto_config)
    params = {
        'RoleArn': role_arn,
        'RoleSessionName': session_name,
        'DurationSeconds': duration_seconds,
    }
    if external_id:
        params['ExternalId'] = external_id
    response = sts.assume_role(**params)
    return response['Credentials']


def run_multi_account(
    args,
    *,
    arg_error,
    script_path,
    assume_role_credentials_fn,
):
    """
    Orchestrate one subprocess run per account target using STS AssumeRole.
    Child runs keep all existing behavior unchanged.
    """
    if args.restore_state:
        arg_error(
            "--restore-state is account-specific and not supported with --accounts-file. "
            "Run restore per account."
        )

    targets = load_account_targets(args.accounts_file, default_role_name=args.assume_role_name, arg_error=arg_error)
    child_base_args = strip_multi_account_args(sys.argv[1:])
    script_path = os.path.abspath(script_path)

    print(f"Multi-account mode: {len(targets)} targets from {args.accounts_file}")
    print(f"Assume role session prefix: {args.assume_role_session_prefix}")
    print()

    failures = []
    child_timeout = args.account_run_timeout or None

    for idx, target in enumerate(targets, start=1):
        account_id = target['account_id']
        role_arn = target['role_arn']
        label = account_id or role_arn
        print(f"[{idx}/{len(targets)}] account {label}")
        print(f"  role: {role_arn}")

        session_name = sanitize_sts_session_name(
            f"{args.assume_role_session_prefix}-{account_id}-{int(time.time())}"
        )

        try:
            creds = assume_role_credentials_fn(
                role_arn=role_arn,
                session_name=session_name,
                external_id=args.assume_role_external_id,
                duration_seconds=args.assume_role_duration,
            )
        except Exception as e:
            print(f"  ERROR assume role failed: {e}")
            failures.append((label, f"assume-role failed: {e}"))
            if not args.continue_on_account_error:
                break
            print()
            continue

        child_args = list(child_base_args)
        suffix = account_id or 'unknown-account'

        # Avoid file collisions between account runs.
        if args.debug:
            output_path = path_with_suffix(args.output, suffix)
            child_args = upsert_cli_value(child_args, '--output', output_path)
        if args.save_state:
            state_path = path_with_suffix(args.save_state, suffix)
            child_args = upsert_cli_value(child_args, '--save-state', state_path)

        env = os.environ.copy()
        env.update({
            'AWS_ACCESS_KEY_ID': creds['AccessKeyId'],
            'AWS_SECRET_ACCESS_KEY': creds['SecretAccessKey'],
            'AWS_SESSION_TOKEN': creds['SessionToken'],
            'TAGGER_MULTI_ACCOUNT_CHILD': '1',
            'TAGGER_TARGET_ACCOUNT_ID': suffix,
        })
        env.pop('AWS_PROFILE', None)

        cmd = [sys.executable, script_path, *child_args]
        print(f"  command: {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, env=env, timeout=child_timeout)
        except subprocess.TimeoutExpired:
            failures.append((label, f"child timed out after {args.account_run_timeout}s"))
            print(f"  ERROR child run timed out after {args.account_run_timeout}s")
            if not args.continue_on_account_error:
                break
            print()
            continue

        if result.returncode != 0:
            failures.append((label, f"child exit code {result.returncode}"))
            print(f"  ERROR child run failed with exit code {result.returncode}")
            if not args.continue_on_account_error:
                break
        else:
            print(f"  OK account {label}")
        print()

    if failures:
        print("Multi-account run completed with failures:")
        for label, reason in failures:
            print(f"  {label}: {reason}")
        sys.exit(1)

    print(f"Multi-account run completed successfully for {len(targets)} accounts.")
    sys.exit(0)
