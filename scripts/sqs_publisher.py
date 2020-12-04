#!/usr/bin/env python

import hashlib

import boto3

def setup_session(profile_name, region):
    """
    Sets up boto3 for instantiating other resources. Assumes that you have named credentials saved in ~/.aws/credentials.
    Note that all resources should exist in the same region.
    Args:
        profile_name: The profile name to be used from ~/.aws/credentials.
        region: The AWS region your resources exist in.


    Returns:
        A boto3 Session.
    """
    session = boto3.session.Session(
        region_name=region,
        profile_name=profile_name
    )
    
    return session

def setup_s3(session, bucket_name):
    """
    Instantiates an S3 resource.

    Args:
        session: A boto3 Session.
        bucket_name: Your S3 bucket's name.

    Returns:
        An S3 bucket.
    """
    s3_resource = session.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)

    return bucket

def setup_sqs(session):
    """
    Instantiates a SQS resource.

    Args:
        session: A boto3 Session.

    Returns:
        An SQS client.
    """
    sqs_client = session.client("sqs")

    return sqs_client

def send(files, queue_url, sqs_client, n=10, send_all=False):
    """
    This sends n, or optionally all, filenames into an SQS queue. This presupposes that you have set up a standard SQS queue.

    Args:
        files: A list of filenames to send.
        queue_url: The URL of your SQS queue.
        sqs_client: The SQS client instantiated by setup_sqs().
        [n]: The number of filenames to send. Defaults to 10.
        [send_all]: If True, ignores n and sends all filenames. Defaults to False.

    Raises:
        KeyError: Ignores exception; indicates either that there were successful entries, or failures.

    Returns:
        A response message indicating the number of files sent, and a list of all failed files (if any).
    """
    cnt = 0
    success = []
    failed = []
    for f in files:
        if cnt >= n and not send_all:
            break
        response = sqs_client.send_message_batch(
            QueueUrl=queue_url,
            # The ID is the filename hashed, which can be used later for deleting the message.
            Entries=[
                {
                    "Id": hashlib.md5(f.key.encode("utf-8")).hexdigest(),
                    "MessageBody": f.key
                }
            ]
        )
        cnt = cnt + 1
        try:
            success.append((f.key, response["Successful"][0]["Id"]))
        except KeyError:
            pass
        try:
            failed.append((f.key, response["Failed"][0]["Id"]))
        except KeyError:
            pass
    ret = "Sent {} messages, {} failed: {}".format(
        len(success), len(failed), failed)
    return ret

def main():
    queue_url = "$QUEUE_URL"
    region = "$REGION"
    profile_name = "$PROFILE_NAME"
    session = setup_session(profile_name, region)
    bucket = setup_s3(session)
    sqs_client = setup_sqs(session)
    files = list(bucket.objects.all())
    resp = send(files, queue_url, sqs_client)
    print(resp)
