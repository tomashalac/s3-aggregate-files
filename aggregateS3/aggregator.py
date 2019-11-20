import boto3
from random import randint
import os
from aggregateS3 import main
from datetime import datetime
import hashlib

s3 = boto3.client("s3")

files = []

def download_all_files_in_bucket():
    response = s3.list_objects(Bucket=main.BUCKET_DOWNLOAD, MaxKeys=main.MAX_KEYS)
    for file in response['Contents']:
        file_key = file["Key"]
        filename = "/tmp/" + str(randint(0, 10000000))

        if file_key[-1] == "/":
            print("Skiping folder: " + file_key)
            continue

        s3.download_file(Bucket=main.BUCKET_DOWNLOAD, Key=file_key, Filename=filename)
        files.append([filename, file_key])


def aggregate_files():
    aggregate = []
    size = 0

    for file, file_key in files:
        file_size = os.path.getsize(file)

        if size + file_size > main.MAX_SIZE_BYTES:
            do_aggregate(aggregate, size)
            size = 0
            aggregate = []

        aggregate.append([file, file_key])
        size += file_size

    if size > 0:
        if size < main.MIN_SIZE_BYTES:
            print("The upload was canceled because the file weighs less than the minimum, file size: " + human_readable_size(size))
            return False
        return do_aggregate(aggregate, size)
    return True

def do_aggregate(list_files, total_size):

    key = datetime.now().strftime(main.OUTPUT_FILE) + ".txt"
    tmp_filename = str(randint(0, 10000000)) + ".txt"

    with open('/tmp/' + tmp_filename, 'wb') as outfile:
        for filename, file_key in list_files:
            with open(filename, "rb") as infile:
                outfile.write(infile.read())

    # Basic check before update
    if os.path.getsize("/tmp/" + tmp_filename) != total_size:
        print("The size are not equal, total: " + str(total_size) + " file: " + str(os.path.getsize("/tmp/" + tmp_filename)))
        return False

    # Upload to AWS S3
    print("New file size: " + human_readable_size(os.path.getsize('/tmp/' + tmp_filename)))
    s3.upload_file(Filename='/tmp/' + tmp_filename, Bucket=main.BUCKET_UPLOAD,
                   Key=key)

    if main.DELETE_OLD_FILE:
        #We want to validate that the new file was uploaded before deleting the old ones.
        if is_valid_upload(total_size, key, tmp_filename):
            print("Deleting old files....")
            return delete_files(list_files)
        else:
            print("Error in check of the new file, aborting deleting.")
            return False
    return True

def is_valid_upload(total_size, key, tmp_filename):
    head = s3.head_object(Bucket=main.BUCKET_UPLOAD, Key=key)

    if not head:
        print("The file was not found in S3: " + key)
        return False

    md5_value = md5("/tmp/" + tmp_filename)
    etag = head["ETag"].replace('"', "")

    # We validate in X different ways that the file is uploaded correctly, they are redundant.
    if md5_value != etag:
        print("MD5 hash fail, on file: " + key + " S3 hash: " + etag + "  python md5: " + md5_value)
        return False

    if total_size != head["ContentLength"]:
        print("The size are not equal, S3: " + str(head["ContentLength"]) + " local: " + str(total_size))
        return False

    return True


def delete_files(list_files):
    if not main.DELETE_OLD_FILE:
        return False

    for filename, file_key in list_files:
        print("Deleting key: " + file_key)
        s3.delete_object(Bucket=main.BUCKET_DOWNLOAD, Key=file_key)

    return True

def human_readable_size(size, decimal_places=3):
    for unit in ['BYTES','KB','MB','GB','TB']:
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


def md5(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
