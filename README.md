# ec2-reaper-lambda #

[![GitHub Build Status](https://github.com/cisagov/ec2-reaper-lambda/workflows/build/badge.svg)](https://github.com/cisagov/ec2-reaper-lambda/actions)
[![License](https://img.shields.io/github/license/cisagov/ec2-reaper-lambda)](https://spdx.org/licenses/)
[![CodeQL](https://github.com/cisagov/ec2-reaper-lambda/workflows/CodeQL/badge.svg)](https://github.com/cisagov/ec2-reaper-lambda/actions/workflows/codeql-analysis.yml)

This repository contains the code for an AWS Lambda function that
terminates EC2 instances, in every enabled region of the account where
it is deployed, that have existed longer than a maximum age.  When any
instances are terminated, skipped, or an error occurs, a plain-text
summary is published to an SNS topic.

An instance's age is measured from the time its root EBS volume was
attached, since that time does not change when an instance is stopped
and started.  If an instance has no EBS root volume, its launch time is
used instead.  Instances with termination protection enabled are never
terminated; they are listed in every summary until they are dealt with.

The summary includes the following details for each instance:

- Instance ARN, `Name` tag, region, and availability zone
- VPC ID and subnet ID
- Root volume attach time (with age) and most recent launch time
- Instance state prior to termination and the state transition reason
- Instance type, AMI ID, and the key pair assigned at launch
- Public IP address (if any) and private IP address
- IAM instance profile, security groups, and tags

If a summary is larger than the 256 KiB SNS message size limit, items
at the end of it are omitted and a note says how many were left out.
The full summary is always written to the Lambda's CloudWatch logs.

If the event is not valid, the invocation fails with an
`InvalidEventError` before any instances are examined.  If any errors
occur during a run, the invocation fails with a `ReapError` after the
summary is published.  Either way, Lambda error metrics and alarms
register the failure.  Lambda retries a failed asynchronous invocation,
such as one from an EventBridge schedule, twice by default, and each
retried run publishes another summary.  Set the function's maximum
retry attempts to zero to avoid this.

## Building the base Lambda image ##

The base Lambda image can be built with the following command:

```console
docker compose build
```

This base image is used both to build a deployment package and to run the
Lambda locally.

## Building a deployment package ##

You can build a deployment zip file to use when creating a new AWS Lambda
function with the following command:

```console
docker compose up build_deployment_package
```

This will output the deployment zip file in the root directory.

## Running the Lambda locally ##

The configuration in this repository allows you run the Lambda locally for
testing as long as you do not need explicit permissions for other AWS
services. This can be done with the following command:

```console
docker compose up --detach run_lambda_locally
```

You can then invoke the Lambda using the following:

```console
 curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

The `{}` in the command is the invocation event payload to send to the Lambda
and would be the value given as the `event` argument to the handler.

Once you are finished you can stop the detached container with the following command:

```console
docker compose down
```

## How to update Python dependencies ##

The Lambda's Python dependencies are maintained using a [Pipenv](https://github.com/pypa/pipenv)
configuration. Changes to requirements should be made to the `Pipfile` located at
`build/Pipfile`. More information about the `Pipfile` format can be found in the
[`pipenv` documentation](https://pipenv.pypa.io/en/latest/pipfile.html#example-pipfile).
The accompanying `Pipfile.lock` file contains the specific dependency versions
that will be installed. This file is updated automatically like so:

```console
cd build
pipenv lock
```

## Lambda inputs ##

| Name | Description | Type | Default | Required |
| ---- | ----------- | ---- | ------- | :------: |
| dry\_run | Whether to report the instances that would be terminated without terminating them.  A dry run still publishes a summary and uses the `DryRun` option of the EC2 `TerminateInstances` API to verify that each termination would be permitted. | `bool` | `false` | no |
| max\_age\_hours | A strictly positive integer denoting the number of hours after which an instance is terminated. | `number` | `24` | no |
| sns\_topic\_arn | The ARN of the SNS topic to which summaries are published. | `string` | n/a | yes |
| task | The task to perform.  Must be `reap`. | `string` | n/a | yes |
| terminate\_stopped | Whether to also terminate stopped (and stopping) instances.  By default only running instances are terminated. | `bool` | `false` | no |

## Example Lambda input ##

The following is an example of the JSON input event that is expected by the
Lambda:

```json
{
    "dry_run": false,
    "max_age_hours": 24,
    "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:ec2-reaper",
    "task": "reap",
    "terminate_stopped": true
}
```

## Required permissions ##

The Lambda's execution role requires the following permissions:

- `ec2:DescribeInstanceAttribute`
- `ec2:DescribeInstances`
- `ec2:DescribeRegions`
- `ec2:TerminateInstances`
- `sns:Publish` on the SNS topic

The `ec2:TerminateInstances` permission can be scoped (for example, by
resource tag) to limit which instances may be terminated.  Any instance
that the Lambda is not permitted to terminate is reported as an error
in the summary.

## Running the tests ##

The tests use [moto](https://github.com/getmoto/moto) to mock AWS, so no
AWS credentials are needed:

```console
pytest
```

## Contributing ##

We welcome contributions!  Please see [`CONTRIBUTING.md`](CONTRIBUTING.md) for
details.

## License ##

This project is in the worldwide [public domain](LICENSE).

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain
dedication](https://creativecommons.org/publicdomain/zero/1.0/).

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.
