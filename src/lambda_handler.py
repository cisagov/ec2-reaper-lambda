"""AWS Lambda handler to terminate EC2 instances that have existed too long."""

# Standard Python Libraries
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import logging
import os
from typing import Any, NamedTuple

# Third-Party Libraries
import boto3
from botocore.exceptions import BotoCoreError, ClientError

default_log_level = "INFO"
logger = logging.getLogger()
logger.setLevel(default_log_level)

DEFAULT_MAX_AGE_HOURS = 24
# SNS rejects subjects longer than this.
SNS_SUBJECT_MAX_LENGTH = 100

PROTECTED_SKIP_REASON = (
    "Termination protection (disableApiTermination) is enabled for this "
    "instance, so it was not terminated.  Disable termination protection and "
    "terminate the instance manually, or remove it from this account."
)


class EventValidation(NamedTuple):
    """Named tuple to hold event validation information."""

    errors: list[str]
    event: dict[str, Any]
    valid: bool


@dataclass
class InstanceRecord:
    """Details about an EC2 instance that is old enough to be reaped."""

    age_hours: float
    ami_id: str | None
    arn: str
    availability_zone: str | None
    created_time: datetime
    # True when created_time is the root EBS volume's attach time; False when
    # it is the launch time because the instance has no EBS root volume.
    created_from_root_volume: bool
    iam_instance_profile: str | None
    instance_id: str
    instance_type: str | None
    key_name: str | None
    launch_time: datetime | None
    name: str | None
    private_ip_address: str | None
    public_ip_address: str | None
    region: str
    security_groups: list[str]
    state: str
    state_transition_reason: str | None
    subnet_id: str | None
    tags: dict[str, str]
    vpc_id: str | None
    note: str | None = None


@dataclass
class ReapResults:
    """The outcome of a reaping run."""

    errors: list[str] = field(default_factory=list)
    skipped: list[InstanceRecord] = field(default_factory=list)
    terminated: list[InstanceRecord] = field(default_factory=list)


def utcnow() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


def failed_task(result: dict[str, Any], error_msg: str) -> None:
    """Update a given result because of a failure during processing."""
    result["success"] = False
    result["error_message"] = error_msg


def task_default(event):
    """Provide a result if no valid task was provided."""
    result = {}
    error_msg = 'Provided task "%s" is not supported.'
    task = event.get("task")
    logging.error(error_msg, task)
    failed_task(result, error_msg % task)
    return result


def validate_event_data(event: dict[str, Any]) -> EventValidation:
    """Validate event data and fill in default values.

    Return: a tuple containing the validated event, a boolean result
    (True if valid, False if invalid), and a list of error message
    strings.
    """
    errors: list[str] = []
    validated: dict[str, Any] = dict(event)

    sns_topic_arn = event.get("sns_topic_arn")
    if not sns_topic_arn:
        errors.append('Missing required key "sns_topic_arn" in event.')
    elif not isinstance(sns_topic_arn, str) or not sns_topic_arn.startswith("arn:"):
        errors.append('"sns_topic_arn" must be an SNS topic ARN.')

    max_age_hours = event.get("max_age_hours", DEFAULT_MAX_AGE_HOURS)
    # bool is a subclass of int, so it must be rejected explicitly.
    if isinstance(max_age_hours, bool):
        errors.append('"max_age_hours" must be a strictly positive integer.')
    else:
        try:
            validated["max_age_hours"] = int(max_age_hours)
            if validated["max_age_hours"] <= 0:
                errors.append('"max_age_hours" must be a strictly positive integer.')
        except (TypeError, ValueError):
            errors.append('"max_age_hours" must be an integer.')

    # Require real booleans so that a string such as "false" cannot silently
    # disable a dry run.
    for key in ("dry_run", "terminate_stopped"):
        value = event.get(key, False)
        if not isinstance(value, bool):
            errors.append(f'"{key}" must be a boolean (true or false).')
        else:
            validated[key] = value

    return EventValidation(errors, validated, not errors)


def get_enabled_regions(session: boto3.session.Session) -> list[str]:
    """Return the names of all regions that are enabled for this account."""
    ec2 = session.client("ec2")
    # Without AllRegions, only regions enabled for the account are returned.
    regions = ec2.describe_regions()["Regions"]
    return sorted(region["RegionName"] for region in regions)


def get_created_time(instance: dict[str, Any]) -> tuple[datetime, bool]:
    """Return when an instance was created and whether a root volume was used.

    LaunchTime resets every time an instance is started, but the root EBS
    volume stays attached across stops and starts.
    """
    root_device_name = instance.get("RootDeviceName")
    for mapping in instance.get("BlockDeviceMappings", []):
        if mapping.get("DeviceName") == root_device_name:
            attach_time = mapping.get("Ebs", {}).get("AttachTime")
            if attach_time is not None:
                return attach_time, True
    return instance["LaunchTime"], False


def build_instance_record(
    instance: dict[str, Any],
    owner_id: str,
    partition: str,
    region: str,
    now: datetime,
) -> InstanceRecord:
    """Collect the details of an instance that are used in the summary."""
    created_time, from_root_volume = get_created_time(instance)
    tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}
    return InstanceRecord(
        age_hours=(now - created_time).total_seconds() / 3600,
        ami_id=instance.get("ImageId"),
        arn=f"arn:{partition}:ec2:{region}:{owner_id}:instance/{instance['InstanceId']}",
        availability_zone=instance.get("Placement", {}).get("AvailabilityZone"),
        created_from_root_volume=from_root_volume,
        created_time=created_time,
        iam_instance_profile=instance.get("IamInstanceProfile", {}).get("Arn"),
        instance_id=instance["InstanceId"],
        instance_type=instance.get("InstanceType"),
        key_name=instance.get("KeyName"),
        launch_time=instance.get("LaunchTime"),
        name=tags.get("Name"),
        private_ip_address=instance.get("PrivateIpAddress"),
        public_ip_address=instance.get("PublicIpAddress"),
        region=region,
        security_groups=[
            f"{group['GroupId']} ({group['GroupName']})"
            for group in instance.get("SecurityGroups", [])
        ],
        state=instance["State"]["Name"],
        state_transition_reason=instance.get("StateTransitionReason") or None,
        subnet_id=instance.get("SubnetId"),
        tags=tags,
        vpc_id=instance.get("VpcId"),
    )


def is_termination_protected(ec2, instance_id: str) -> bool:
    """Return whether termination protection is enabled for an instance."""
    response = ec2.describe_instance_attribute(
        Attribute="disableApiTermination", InstanceId=instance_id
    )
    return bool(response["DisableApiTermination"]["Value"])


def terminate_instance(ec2, record: InstanceRecord, dry_run: bool) -> None:
    """Terminate an instance, or verify that it could be terminated.

    Raises ClientError if the instance could not be terminated.
    """
    if dry_run:
        try:
            ec2.terminate_instances(DryRun=True, InstanceIds=[record.instance_id])
        except ClientError as err:
            # AWS reports a dry run that would have succeeded as an error.
            if err.response.get("Error", {}).get("Code") != "DryRunOperation":
                raise
        return
    ec2.terminate_instances(InstanceIds=[record.instance_id])


def reap_region(
    session: boto3.session.Session,
    region: str,
    states: list[str],
    cutoff: datetime,
    now: datetime,
    dry_run: bool,
    results: ReapResults,
) -> None:
    """Terminate instances in a region that were created before the cutoff."""
    ec2 = session.client("ec2", region_name=region)
    partition = session.get_partition_for_region(region)

    candidates: list[InstanceRecord] = []
    try:
        paginator = ec2.get_paginator("describe_instances")
        for page in paginator.paginate(
            Filters=[{"Name": "instance-state-name", "Values": states}]
        ):
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    record = build_instance_record(
                        instance, reservation["OwnerId"], partition, region, now
                    )
                    if record.created_time <= cutoff:
                        candidates.append(record)
                    else:
                        logging.debug(
                            "Instance %s is too young to reap.", record.instance_id
                        )
    except (BotoCoreError, ClientError) as err:
        error_msg = f"{region}: Unable to list EC2 instances: {err}"
        logging.error(error_msg)
        results.errors.append(error_msg)
        return

    for record in candidates:
        try:
            if is_termination_protected(ec2, record.instance_id):
                logging.warning(
                    "Skipping instance %s because termination protection is enabled.",
                    record.instance_id,
                )
                record.note = PROTECTED_SKIP_REASON
                results.skipped.append(record)
                continue

            logging.info(
                "%s instance %s (created %s).",
                "Dry run: would terminate" if dry_run else "Terminating",
                record.instance_id,
                record.created_time.isoformat(),
            )
            terminate_instance(ec2, record, dry_run)
            results.terminated.append(record)
        except (BotoCoreError, ClientError) as err:
            error_msg = f"{record.arn}: Unable to terminate instance: {err}"
            logging.error(error_msg)
            results.errors.append(error_msg)


def format_time(value: datetime | None) -> str:
    """Format a datetime for the summary."""
    if value is None:
        return "(unknown)"
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_instance(record: InstanceRecord, state_label: str) -> str:
    """Format the details of a single instance for the summary."""
    created_label = (
        "Root volume attach time"
        if record.created_from_root_volume
        else "Launch time (no EBS root volume)"
    )
    none = "(none)"
    lines = [
        ("Instance ARN", record.arn),
        ("Name tag", record.name or none),
        ("Region", record.region),
        ("Availability zone", record.availability_zone or none),
        ("VPC ID", record.vpc_id or none),
        ("Subnet ID", record.subnet_id or none),
        (
            created_label,
            f"{format_time(record.created_time)} ({record.age_hours:.1f} hours ago)",
        ),
        ("Launch time (last start)", format_time(record.launch_time)),
        (state_label, record.state),
        ("State transition reason", record.state_transition_reason or none),
        ("Instance type", record.instance_type or none),
        ("AMI ID", record.ami_id or none),
        ("Key pair", record.key_name or none),
        ("Public IP address", record.public_ip_address or none),
        ("Private IP address", record.private_ip_address or none),
        ("IAM instance profile", record.iam_instance_profile or none),
        ("Security groups", ", ".join(record.security_groups) or none),
        (
            "Tags",
            ", ".join(f"{k}={v}" for k, v in sorted(record.tags.items())) or none,
        ),
    ]
    if record.note:
        lines.insert(0, ("Reason skipped", record.note))
    width = max(len(label) for label, _ in lines) + 1
    return "\n".join(f"  {label + ':':<{width}} {value}" for label, value in lines)


def build_summary(
    results: ReapResults,
    account_id: str,
    now: datetime,
    max_age_hours: int,
    states: list[str],
    dry_run: bool,
) -> tuple[str, str]:
    """Build the subject and body of the summary message."""
    prefix = "[DRY RUN] " if dry_run else ""
    terminated_verb = "would terminate" if dry_run else "terminated"
    subject = (
        f"{prefix}EC2 Reaper {account_id}: {terminated_verb} "
        f"{len(results.terminated)}, skipped {len(results.skipped)}, "
        f"errors {len(results.errors)}"
    )[:SNS_SUBJECT_MAX_LENGTH]

    sections = [
        f"EC2 Reaper summary for AWS account {account_id}",
        f"Run time: {format_time(now)}",
        f"Maximum instance age: {max_age_hours} hours, measured from the root "
        "volume attach time (launch time if there is no EBS root volume)",
        f"Instance states considered: {', '.join(states)}",
    ]
    if dry_run:
        sections.append(
            "DRY RUN: no instances were terminated.  The instances below are the "
            "ones that would have been terminated."
        )

    if results.terminated:
        heading = (
            "Instances that would be terminated" if dry_run else "Terminated instances"
        )
        sections.append(f"===== {heading} ({len(results.terminated)}) =====")
        sections.extend(
            format_instance(record, "State before termination")
            for record in results.terminated
        )

    if results.skipped:
        sections.append(
            "===== Instances older than the maximum age that were NOT "
            f"terminated ({len(results.skipped)}) ====="
        )
        sections.extend(
            format_instance(record, "Current state") for record in results.skipped
        )

    if results.errors:
        sections.append(f"===== Errors ({len(results.errors)}) =====")
        sections.extend(f"  - {error}" for error in results.errors)

    return subject, "\n\n".join(sections) + "\n"


def task_reap(event):
    """Terminate EC2 instances in all enabled regions that are too old.

    :param event: The event dict that contains the parameters sent when
    the function is invoked.
    :return: The result of the action.
    """
    result: dict[str, Any] = {"message": None, "success": True}

    event_validation: EventValidation = validate_event_data(event)
    if not event_validation.valid:
        for e in event_validation.errors:
            logging.error(e)
        failed_task(result, " ".join(event_validation.errors))
        return result
    validated_event = event_validation.event

    dry_run: bool = validated_event["dry_run"]
    max_age_hours: int = validated_event["max_age_hours"]
    sns_topic_arn: str = validated_event["sns_topic_arn"]
    states = ["running"]
    if validated_event["terminate_stopped"]:
        states.extend(["stopping", "stopped"])

    now = utcnow()
    cutoff = now - timedelta(hours=max_age_hours)
    session = boto3.session.Session()
    results = ReapResults()
    account_id = "(unknown)"

    try:
        account_id = session.client("sts").get_caller_identity()["Account"]
        regions = get_enabled_regions(session)
    except (BotoCoreError, ClientError) as err:
        error_msg = f"Unable to determine the account ID or enabled regions: {err}"
        logging.error(error_msg)
        results.errors.append(error_msg)
        regions = []

    for region in regions:
        logging.debug("Examining region %s", region)
        reap_region(session, region, states, cutoff, now, dry_run, results)

    result["dry_run"] = dry_run
    result["errors"] = results.errors
    result["skipped"] = [record.arn for record in results.skipped]
    result["terminated"] = [record.arn for record in results.terminated]

    if results.terminated or results.skipped or results.errors:
        subject, body = build_summary(
            results, account_id, now, max_age_hours, states, dry_run
        )
        try:
            session.client("sns").publish(
                Message=body, Subject=subject, TopicArn=sns_topic_arn
            )
        except (BotoCoreError, ClientError) as err:
            error_msg = f"Unable to publish summary to {sns_topic_arn}: {err}"
            logging.error(error_msg)
            results.errors.append(error_msg)
    else:
        logging.info("No instances needed to be reaped.")

    if results.errors:
        failed_task(result, " ".join(results.errors))
    else:
        result["message"] = (
            f"{'Dry run: would terminate' if dry_run else 'Terminated'} "
            f"{len(results.terminated)} instance(s) and skipped "
            f"{len(results.skipped)} protected instance(s)."
        )
        logging.info(result["message"])
    return result


def handler(event, context) -> dict[str, str | None]:
    """Process the event and generate a response.

    The event should have a task member that is one of the supported tasks.

    :param event: The event dict that contains the parameters sent when the function
                  is invoked.
    :param context: The context in which the function is called.
    :return: The result of the action.
    """
    old_log_level = None
    response: dict[str, str | None] = {"timestamp": str(datetime.now(timezone.utc))}

    # Update the logging level if necessary
    new_log_level = os.environ.get("log_level", default_log_level).upper()
    if not isinstance(logging.getLevelName(new_log_level), int):
        logging.warning(
            "Invalid logging level %s. Using %s instead.",
            new_log_level,
            default_log_level,
        )
        new_log_level = default_log_level
    if logging.getLogger().getEffectiveLevel() != logging.getLevelName(new_log_level):
        old_log_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(new_log_level)

    task_name = f"task_{event.get('task')}"
    task = globals().get(task_name, task_default)

    result: dict[str, Any]
    if not callable(task):
        logging.error("Provided task is not a callable.")
        logging.error(task)
        result = task_default(event)
    else:
        result = task(event)
    response.update(result)

    if old_log_level is not None:
        logging.getLogger().setLevel(old_log_level)

    return response
