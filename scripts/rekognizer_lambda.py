#!/usr/bin/env python

from decimal import Decimal
import json
import logging
import sys
import traceback

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)
region = "$REGION"
bucket_name = "$BUCKET_NAME"
queue_url = "$QUEUE_URL"

reg_config = Config(region_name=region)
s3_resource = boto3.resource("s3", config=reg_config)
sqs_client = boto3.client("sqs", config=reg_config)
bucket = s3_resource.Bucket(bucket_name)
rekognition = boto3.client("rekognition", config=reg_config)

def detect_gender(photo, bucket):
    """
    Passes a photo into Rekognition from S3.

    Args:
        photo: A filename to retrieve from the bucket.
        bucket: The bucket's name.

    Returns:
        A tuple containing the filename and the assumed gender, with confidence.

    Raises:
        IndexError: If the photo is not human, no details will be returned.
                    An easily identifiable marker is made to filter out later.
    """
    response = rekognition.detect_faces(
        Image={
            'S3Object':
            {
                'Bucket': bucket, 'Name': photo
            }
        },
        Attributes=["ALL"]
    )
    try:
        ret = response["FaceDetails"][0]["Gender"]
    except IndexError:
        ret = {"Value": "Unknown", "Confidence": 100}
    logger.info("Response: ".format(response))
    return photo, ret

def write_table(rek_ret):
    """
    Writes results returned from detect_gender() to a DynamoDB table.
    Presupposes that you have created the table.

    Args:
        rek_ret: The return value from detect_gender()

    Returns:
        Nothing.
    """
    table = boto3.resource("dynamodb").Table("ghtorrent-faces")
    table.put_item(Item={
        "PK": rek_ret[0],
        "Gender": rek_ret[1]["Value"],
        "Confidence": Decimal(rek_ret[1]["Confidence"])
    })

def delete_msg(receipt_handle):
    """
    Deletes a message from the SQS queue given a receipt handle.

    Args:
        receipt_handle: A receipt handle.

    Returns:
        Nothing.
    """
    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

def recv():
    """
    Retrieves a message from an SQS queue.
    Presupposes that you've created the queue.

    Args:
        None.

    Returns:
        The body of a message, containing a filename.

    Raises:
        KeyError: If a message can't be retrieved, usually due to IAM errors.
        Exception: Catch-all, raises exception.

    """
    recvd = []
    export = []
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=30,
                WaitTimeSeconds=15
            )
            logger.info("Received {}".format(response))
            for x in response["Messages"]:
                if x["MD5OfBody"] not in recvd:
                    recvd.append(x["MD5OfBody"])
                    export.append(x["Body"])
                    delete_msg(x["ReceiptHandle"])
        except KeyError:
            exception_type, exception_value, exception_traceback = sys.exc_info()
            traceback_string = traceback.format_exception(
                exception_type, exception_value, exception_traceback)
            err_msg = json.dumps({
                "errorType": exception_type.__name__,
                "errorMessage": str(exception_value),
                "stackTrace": traceback_string
            })
            logger.error(err_msg)
            break
        except Exception as exp:
            exception_type, exception_value, exception_traceback = sys.exc_info()
            traceback_string = traceback.format_exception(
            exception_type, exception_value, exception_traceback)
            err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
            })
            logger.error(err_msg)

    return export

def lambda_handler(event, context):
    """
    The Lambda handler. Runs other functions and logs results for troubleshooting.

    Args:
        All handled by AWS Lambda.

    Returns:
        Nothing.
    """
    file_chunks = recv()
    try:
        for f in file_chunks:
            logger.info("Working on file {}".format(f))
            gender_resp = detect_gender(f, bucket_name)
            write_table(gender_resp)
    except Exception as exp:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(
            exception_type, exception_value, exception_traceback)
        err_msg = json.dumps({
            "errorType": exception_type.__name__,
            "errorMessage": str(exception_value),
            "stackTrace": traceback_string
        })
        logger.error(err_msg)
