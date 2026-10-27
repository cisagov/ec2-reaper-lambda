"""Tests for the EC2 reaper Lambda handler."""

# Standard Python Libraries
from datetime import datetime, timedelta, timezone

# Third-Party Libraries
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID
from moto.sns.models import sns_backends
import pytest

# cisagov Libraries
import lambda_handler

REGION = "us-east-1"


@pytest.fixture
def aws(monkeypatch):
    """Provide a mocked AWS environment with fake credentials."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with mock_aws():
        yield


@pytest.fixture
def topic_arn(aws):
    """Create the SNS topic that receives summaries."""
    return boto3.client("sns", region_name=REGION).create_topic(Name="reaper")[
        "TopicArn"
    ]


def sent_messages(topic_arn, region=REGION):
    """Return the (subject, message) pairs published to the topic."""
    topic = sns_backends[DEFAULT_ACCOUNT_ID][region].topics[topic_arn]
    return [
        (subject, message) for _, message, subject, _, _ in topic.sent_notifications
    ]


def advance_clock(monkeypatch, hours):
    """Make the handler believe that the given number of hours have passed."""
    now = datetime.now(timezone.utc) + timedelta(hours=hours)
    monkeypatch.setattr(lambda_handler, "utcnow", lambda: now)


def launch(region=REGION, **kwargs):
    """Launch an instance and return its ID."""
    ec2 = boto3.client("ec2", region_name=region)
    ami = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
    return ec2.run_instances(
        ImageId=ami, InstanceType="t3.micro", MaxCount=1, MinCount=1, **kwargs
    )["Instances"][0]["InstanceId"]


def state_of(instance_id, region=REGION):
    """Return the state name of an instance."""
    ec2 = boto3.client("ec2", region_name=region)
    instance = ec2.describe_instances(InstanceIds=[instance_id])["Reservations"][0][
        "Instances"
    ][0]
    return instance["State"]["Name"]


def deny_describe_instances(monkeypatch, region, reason):
    """Make DescribeInstances fail in a region as it would if access were denied."""
    original_client = boto3.session.Session.client
    message = (
        "You are not authorized to perform this operation. User: "
        "arn:aws:sts::123456789012:assumed-role/EC2ReaperLambda/ec2-reaper is not "
        f"authorized to perform: ec2:DescribeInstances {reason}"
    )

    def deny(**kwargs):
        raise ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": message}},
            "DescribeInstances",
        )

    def client(self, service_name, *args, **kwargs):
        new_client = original_client(self, service_name, *args, **kwargs)
        if service_name == "ec2" and new_client.meta.region_name == region:
            new_client.meta.events.register("before-call.ec2.DescribeInstances", deny)
        return new_client

    monkeypatch.setattr(boto3.session.Session, "client", client)


SCP_DENIAL = (
    "with an explicit deny in a service control policy: "
    "arn:aws:organizations::123456789012:policy/o-example/"
    "service_control_policy/p-example"
)


def reap(topic_arn, **kwargs):
    """Invoke the handler with the reap task."""
    return lambda_handler.handler(
        {"task": "reap", "sns_topic_arn": topic_arn, **kwargs}, None
    )


@pytest.mark.parametrize(
    "event,message",
    [
        ({"sns_topic_arn": None}, 'Missing required key "sns_topic_arn"'),
        ({"sns_topic_arn": "not-an-arn"}, "must be an SNS topic ARN"),
        ({"sns_topic_arn": "arn:"}, "must be an SNS topic ARN"),
        (
            {"sns_topic_arn": "arn:aws:sqs:us-east-1:123456789012:queue"},
            "must be an SNS topic ARN",
        ),
        (
            {"sns_topic_arn": "arn:aws:sns:us-east-1:1234:topic"},
            "must be an SNS topic ARN",
        ),
        (
            {"sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:"},
            "must be an SNS topic ARN",
        ),
        (
            {"sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:reaper\n"},
            "must be an SNS topic ARN",
        ),
        (
            {"sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:reaper.fifo"},
            "must not be a FIFO topic",
        ),
        ({"max_age_hours": 0}, "strictly positive"),
        ({"max_age_hours": "abc"}, "must be an integer"),
        ({"max_age_hours": 24.9}, "must be an integer"),
        ({"max_age_hours": "24.9"}, "must be an integer"),
        ({"max_age_hours": True}, "strictly positive"),
        ({"dry_run": "false"}, '"dry_run" must be a boolean'),
        ({"terminate_stopped": 1}, '"terminate_stopped" must be a boolean'),
    ],
)
def test_invalid_event(event, message):
    """Verify that invalid events are rejected before AWS is touched."""
    full_event = {
        "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:t",
        "task": "reap",
        **event,
    }
    with pytest.raises(lambda_handler.InvalidEventError) as excinfo:
        lambda_handler.handler(full_event, None)
    assert message in str(excinfo.value)


def test_defaults():
    """Verify the default values filled in by validation."""
    validation = lambda_handler.validate_event_data(
        {"sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:t"}
    )
    assert validation.valid
    assert validation.event["dry_run"] is False
    assert validation.event["max_age_hours"] == 24
    assert validation.event["terminate_stopped"] is False


@pytest.mark.parametrize("value", [48, 48.0, "48"])
def test_whole_number_max_age(value):
    """Verify that whole-number forms of max_age_hours are accepted."""
    validation = lambda_handler.validate_event_data(
        {
            "max_age_hours": value,
            "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:t",
        }
    )
    assert validation.valid
    assert validation.event["max_age_hours"] == 48


def test_unsupported_task():
    """Verify that an unknown task fails."""
    with pytest.raises(lambda_handler.InvalidEventError, match="not supported"):
        lambda_handler.handler({"task": "nope"}, None)


def test_nothing_to_reap(monkeypatch, topic_arn):
    """Verify that young instances survive and no email is sent."""
    instance_id = launch()
    advance_clock(monkeypatch, 1)
    result = reap(topic_arn)
    assert result["success"] is True
    assert result["terminated"] == []
    assert state_of(instance_id) == "running"
    assert sent_messages(topic_arn) == []


def test_terminates_old_running_instance(monkeypatch, topic_arn):
    """Verify that an old instance is terminated and summarized."""
    instance_id = launch(KeyName="packer-key")
    other_region_id = launch(region="us-west-2")
    advance_clock(monkeypatch, 25)
    result = reap(topic_arn)

    assert result["success"] is True
    assert state_of(instance_id) in ("shutting-down", "terminated")
    assert state_of(other_region_id, "us-west-2") in ("shutting-down", "terminated")
    assert len(result["terminated"]) == 2

    [(subject, body)] = sent_messages(topic_arn)
    assert "terminated 2" in subject
    assert len(subject) <= lambda_handler.SNS_SUBJECT_MAX_LENGTH
    assert f"arn:aws:ec2:{REGION}:{DEFAULT_ACCOUNT_ID}:instance/{instance_id}" in body
    for label in (
        "VPC ID:",
        "Subnet ID:",
        "Root volume attach time:",
        "Launch time (last start):",
        "State before termination: running",
        "Public IP address:",
        "Private IP address:",
        "Instance type:",
        "Key pair:",
    ):
        assert label in body
    assert "packer-key" in body
    assert "t3.micro" in body


def test_respects_max_age(monkeypatch, topic_arn):
    """Verify that max_age_hours changes the cutoff."""
    instance_id = launch()
    advance_clock(monkeypatch, 25)
    result = reap(topic_arn, max_age_hours=48)
    assert result["terminated"] == []
    assert state_of(instance_id) == "running"


def test_stopped_instances(monkeypatch, topic_arn):
    """Verify that stopped instances are only reaped when requested."""
    instance_id = launch()
    boto3.client("ec2", region_name=REGION).stop_instances(InstanceIds=[instance_id])
    advance_clock(monkeypatch, 25)

    result = reap(topic_arn)
    assert result["terminated"] == []
    assert state_of(instance_id) == "stopped"

    result = reap(topic_arn, terminate_stopped=True)
    assert len(result["terminated"]) == 1
    assert state_of(instance_id) in ("shutting-down", "terminated")
    [(_, body)] = sent_messages(topic_arn)
    assert "State before termination: stopped" in body


def test_skips_protected_instance(monkeypatch, topic_arn):
    """Verify that protected instances are skipped and explained."""
    instance_id = launch()
    # moto ignores DisableApiTermination in run_instances.
    boto3.client("ec2", region_name=REGION).modify_instance_attribute(
        DisableApiTermination={"Value": True}, InstanceId=instance_id
    )
    advance_clock(monkeypatch, 25)
    result = reap(topic_arn)

    assert result["success"] is True
    assert result["terminated"] == []
    assert len(result["skipped"]) == 1
    assert state_of(instance_id) == "running"
    [(subject, body)] = sent_messages(topic_arn)
    assert "skipped 1" in subject
    assert "NOT terminated" in body
    assert "Reason skipped:" in body
    assert "Termination protection (disableApiTermination) is enabled" in body


def test_dry_run(monkeypatch, topic_arn):
    """Verify that a dry run terminates nothing but still reports."""
    instance_id = launch()
    advance_clock(monkeypatch, 25)
    result = reap(topic_arn, dry_run=True)

    assert result["success"] is True
    assert result["dry_run"] is True
    assert len(result["terminated"]) == 1
    assert state_of(instance_id) == "running"
    [(subject, body)] = sent_messages(topic_arn)
    assert subject.startswith("[DRY RUN]")
    assert "would terminate 1" in subject
    assert "DRY RUN: no instances were terminated" in body


def test_termination_error_is_reported(monkeypatch, topic_arn):
    """Verify that a failed termination is emailed and fails the run."""
    launch()
    advance_clock(monkeypatch, 25)

    def deny(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "UnauthorizedOperation", "Message": "denied"}},
            "TerminateInstances",
        )

    monkeypatch.setattr(lambda_handler, "terminate_instance", deny)
    with pytest.raises(lambda_handler.ReapError, match="UnauthorizedOperation"):
        reap(topic_arn)

    [(subject, body)] = sent_messages(topic_arn)
    assert "errors 1" in subject
    assert "Unable to terminate instance" in body


def test_skips_region_denied_by_scp(monkeypatch, topic_arn):
    """Verify that a region denied by an SCP is skipped and reported."""
    instance_id = launch()
    other_region_id = launch(region="us-west-2")
    advance_clock(monkeypatch, 25)
    deny_describe_instances(monkeypatch, "us-west-2", SCP_DENIAL)
    result = reap(topic_arn)

    assert result["success"] is True
    assert result["errors"] == []
    assert result["scp_denied_regions"] == ["us-west-2"]
    assert "service control policy denies access to them: us-west-2." in (
        result["message"]
    )
    assert state_of(instance_id) in ("shutting-down", "terminated")
    # state_of() can't be used in the denied region, but only instances that
    # were listed can have been terminated.
    [terminated_arn] = result["terminated"]
    assert terminated_arn.endswith(f"/{instance_id}")
    assert other_region_id not in terminated_arn
    [(subject, body)] = sent_messages(topic_arn)
    assert "errors 0" in subject
    assert (
        "Regions skipped because a service control policy denies access to "
        "them: us-west-2"
    ) in body


def test_scp_denied_region_alone_sends_no_summary(monkeypatch, topic_arn):
    """Verify that SCP-denied regions alone do not trigger a summary."""
    advance_clock(monkeypatch, 25)
    deny_describe_instances(monkeypatch, "us-west-2", SCP_DENIAL)
    result = reap(topic_arn)

    assert result["success"] is True
    assert result["scp_denied_regions"] == ["us-west-2"]
    assert sent_messages(topic_arn) == []


def test_iam_denial_is_an_error(monkeypatch, topic_arn):
    """Verify that a region denied by the Lambda's own policy is an error."""
    advance_clock(monkeypatch, 25)
    deny_describe_instances(
        monkeypatch,
        "us-west-2",
        "because no identity-based policy allows the ec2:DescribeInstances action",
    )
    with pytest.raises(lambda_handler.ReapError, match="us-west-2: Unable to list"):
        reap(topic_arn)

    [(subject, body)] = sent_messages(topic_arn)
    assert "errors 1" in subject
    assert "Regions skipped" not in body


def test_publish_failure(monkeypatch, aws):
    """Verify that a failure to publish the summary fails the run."""
    launch()
    advance_clock(monkeypatch, 25)
    with pytest.raises(lambda_handler.ReapError, match="Unable to publish summary"):
        reap(f"arn:aws:sns:{REGION}:{DEFAULT_ACCOUNT_ID}:missing")


def test_oversized_summary(monkeypatch, topic_arn, caplog):
    """Verify that an oversized summary is truncated but logged in full."""
    max_bytes = 2000
    for _ in range(3):
        launch()
    advance_clock(monkeypatch, 25)
    monkeypatch.setattr(lambda_handler, "SNS_MESSAGE_MAX_BYTES", max_bytes)
    result = reap(topic_arn)

    assert result["success"] is True
    assert len(result["terminated"]) == 3
    [(_, body)] = sent_messages(topic_arn)
    assert len(body.encode()) <= max_bytes
    assert body.count("Instance ARN:") < 3
    assert "more item(s) omitted" in body
    # Each instance is logged in full, in its own log record.
    logged = [r for r in caplog.records if "Instance ARN:" in r.getMessage()]
    assert len(logged) == 3


def test_summary_body_omits_trailing_items():
    """Verify that items are omitted from the end of an oversized summary."""
    # Multibyte characters ensure that the limit is applied to bytes.
    items = [f"item {i} " + "é" * 100 for i in range(10)]
    summary = lambda_handler.Summary(
        subject="subject",
        header=["header"],
        groups=[("===== A =====", items[:5]), ("===== B =====", items[5:])],
    )
    full_body = lambda_handler.summary_body(summary)
    assert all(item in full_body for item in items)
    assert lambda_handler.summary_body(summary, len(full_body.encode())) == full_body

    max_bytes = 1000
    body = lambda_handler.summary_body(summary, max_bytes)
    shown = [item for item in items if item in body]
    assert len(body.encode()) <= max_bytes
    assert 0 < len(shown) < len(items)
    assert shown == items[: len(shown)]
    assert "===== B =====" in body
    assert f"{len(items) - len(shown)} more item(s) omitted" in body


def test_publishes_in_topic_region(monkeypatch, aws):
    """Verify that the summary is published in the topic's region."""
    topic_region = "us-west-2"
    topic_arn = boto3.client("sns", region_name=topic_region).create_topic(
        Name="reaper"
    )["TopicArn"]
    launch()
    advance_clock(monkeypatch, 25)
    result = reap(topic_arn)
    assert result["success"] is True
    assert len(sent_messages(topic_arn, topic_region)) == 1


def test_created_time_prefers_root_volume():
    """Verify that a restart does not reset the instance's age."""
    attach_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    launch_time = datetime(2026, 1, 5, tzinfo=timezone.utc)
    instance = {
        "BlockDeviceMappings": [
            {"DeviceName": "/dev/sdb", "Ebs": {"AttachTime": launch_time}},
            {"DeviceName": "/dev/xvda", "Ebs": {"AttachTime": attach_time}},
        ],
        "LaunchTime": launch_time,
        "RootDeviceName": "/dev/xvda",
    }
    assert lambda_handler.get_created_time(instance) == (attach_time, True)


def test_created_time_without_ebs_root():
    """Verify the fallback for instances without an EBS root volume."""
    launch_time = datetime(2026, 1, 5, tzinfo=timezone.utc)
    instance = {"BlockDeviceMappings": [], "LaunchTime": launch_time}
    assert lambda_handler.get_created_time(instance) == (launch_time, False)


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:sns:us-east-1:123456789012:ec2-reaper",
        "arn:aws-us-gov:sns:us-gov-west-1:123456789012:ec2_reaper",
    ],
)
def test_valid_sns_topic_arn(arn):
    """Verify that well-formed SNS topic ARNs are accepted."""
    assert lambda_handler.validate_event_data({"sns_topic_arn": arn}).valid
