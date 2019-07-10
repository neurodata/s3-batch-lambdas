"""Lambda to create a new PNG file for a given TIFF file inside the same S3 Bucket
It does not delete or modify the original TIFF file
"""

from io import BytesIO
from os import path

import boto3
from PIL import Image


def lambda_handler(event, context):
    s3Client = boto3.client("s3")

    # Parse job parameters
    invocationId = event["invocationId"]
    invocationSchemaVersion = event["invocationSchemaVersion"]

    # Process the task
    task = event["tasks"][0]
    taskId = task["taskId"]

    s3Key = task["s3Key"]

    s3BucketArn = task["s3BucketArn"]
    s3Bucket = s3BucketArn.split(":")[-1]

    upload_path = "/tmp/{}.png".format(s3Key)

    rootname, extension = path.splitext(s3Key)
    extension = extension.lower()
    if extension in [".tif", ".tiff"]:
        obj = s3Client.Object(bucket_name=s3Bucket, key=s3Key)
        obj_body = obj.get()["Body"].read()
        img = Image.open(BytesIO(obj_body))
        img.save(upload_path, format="PNG")

        s3Client.upload_file(upload_path, s3Bucket, "{}.png".format(rootname))

        results = [
            {"taskId": taskId, "resultCode": "Succeeded", "resultString": "Succeeded"}
        ]
    else:
        results = [{"taskId": taskId, "resultCode": "PermanentFailure"}]

    return {
        "invocationSchemaVersion": invocationSchemaVersion,
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": invocationId,
        "results": results,
    }
