from random import randint
import os
from aggregateS3 import main, parallel_download, config
from datetime import datetime
import hashlib

s3 = None

def aggregate_files(list_keys, suffix):
    global s3
    s3 = main.get_boto3()
    aggregate = []
    size = 0

    for file_key in list_keys:
        filename = config.CONFIG.local_folder_to_download + file_key.replace('/', '_')
        file_size = os.path.getsize(filename)

        if size + file_size > config.CONFIG.max_size_bytes:
            do_aggregate(aggregate, size, suffix)
            size = 0
            aggregate = []

        aggregate.append([filename, file_key])
        size += file_size

    if size > 0:
        if size < config.CONFIG.min_size_bytes:
            print("The upload was canceled because the file weighs less than the minimum, file size: " + human_readable_size(size) +
                  ", the minimum is: " + human_readable_size(config.CONFIG.min_size_bytes))
            return False
        return do_aggregate(aggregate, size, suffix)
    return True

def do_aggregate(list_files, total_size, suffix):

    key = datetime.now().strftime(config.CONFIG.output_file) + "." + suffix
    tmp_filename = str(randint(0, 10000000)) + ".txt"

    with open(config.CONFIG.local_folder_to_download + tmp_filename, 'wb') as outfile:
        for filename, file_key in list_files:
            with open(filename, "rb") as infile:
                outfile.write(infile.read())

    # Basic check before update
    if os.path.getsize(config.CONFIG.local_folder_to_download + tmp_filename) != total_size:
        print("The size are not equal, total: " + str(total_size) + " file: " + str(os.path.getsize(config.CONFIG.local_folder_to_download + tmp_filename)))
        return False

    # Upload to AWS S3
    print("New file size: " + human_readable_size(os.path.getsize(config.CONFIG.local_folder_to_download + tmp_filename)))
    s3.upload_file(Filename=config.CONFIG.local_folder_to_download + tmp_filename, Bucket=config.CONFIG.bucket_upload,
                   Key=key)

    if config.CONFIG.delete_old_file:
        #We want to validate that the new file was uploaded before deleting the old ones.
        if is_valid_upload(total_size, key, tmp_filename):
            print("Deleting old files....")
            return delete_files(list_files)
        else:
            print("Error in check of the new file, aborting deleting.")
            return False
    return True

def is_valid_upload(total_size, key, tmp_filename):
    head = s3.head_object(Bucket=config.CONFIG.bucket_upload, Key=key)

    if not head:
        print("The file was not found in S3: " + key)
        return False

    md5_value = md5(config.CONFIG.local_folder_to_download + tmp_filename)
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
    if not config.CONFIG.delete_old_file:
        return False

    print("Start deleting... in S3")
    for filename, file_key in list_files:
        s3.delete_object(Bucket=config.CONFIG.bucket_download, Key=file_key)

    print("End deleting... " + str(len(list_files)) + " deleted files")
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
