from multiprocessing import Process
from aggregateS3 import main, config
from pathlib import Path
import os
import queue
import threading
import time


def private_download(keys_queue):
    s3 = main.get_boto3()

    while not keys_queue.empty():
        try:
            file_key = keys_queue.get()

            filename = config.CONFIG.local_folder_to_download + file_key.replace('/', '_')
            if os.path.exists(filename):
                raise Exception("File exists: " + filename)
            s3.download_file(Bucket=config.CONFIG.bucket_download, Key=file_key, Filename=filename)

            keys_queue.task_done()
        except Exception as e:
            print(e)


def list_all(keys_queue, data):
    s3 = main.get_boto3()

    key_list, continuation_token, suffix = private_list_files_in_bukcet(s3, suffix=None, max_keys=config.CONFIG.max_keys)
    actual_count = len(key_list)
    for key in key_list:
        keys_queue.put(key)

    while actual_count < config.CONFIG.max_keys and continuation_token:
        new_key_list, continuation_token, _ = private_list_files_in_bukcet(s3, suffix, continuation_token, config.CONFIG.max_keys - actual_count)
        actual_count += len(new_key_list)
        for key in new_key_list:
            keys_queue.put(key)
        key_list = key_list + new_key_list


    print("Total files to download: " + str(actual_count))
    data.append(suffix)
    data.append(key_list)


def download_all_files():
    os.mkdir(config.CONFIG.local_folder_to_download)
    if len(os.listdir(config.CONFIG.local_folder_to_download)) > 0:
        raise Exception("Folder " + config.CONFIG.local_folder_to_download + " is not empty.")

    keys_queue = queue.Queue()
    data = []

    list_all_thread = threading.Thread(target=list_all, args=(keys_queue, data))
    list_all_thread.start()

    timeout = time.time() + 3
    while keys_queue.empty() and timeout > time.time():
        time.sleep(0.1)

    if keys_queue.empty():
        raise Exception("Error listing")

    private_start_parallel_download(keys_queue)

    # end of the thread
    list_all_thread.join()
    suffix, key_list = data
    print("All " + str(len(key_list)) + " files downloaded.")

    return suffix, key_list


def private_list_files_in_bukcet(s3, suffix, continuation_token=[], max_keys=1000):
    vars = {"Bucket" : config.CONFIG.bucket_download, "MaxKeys" : max_keys, "Prefix" : config.CONFIG.bucket_download_prefix}
    if continuation_token:
        vars["ContinuationToken"] = continuation_token
    response = s3.list_objects_v2(**vars)

    print("Listing "+str(len(response['Contents']))+" files from AWS S3")
    keys_list = []
    for file in response['Contents']:
        file_key = file["Key"]
        file_suffix = Path(file_key).suffix[1:]

        if file_key[-1] == "/":
            print("Skiping folder: " + file_key)
            continue

        if file_suffix.lower() not in main.EXTENSIONS_ALLOWED:
            print("Skiping file: " + file_key + " suffix is not valid.")
            continue

        if not suffix:
            suffix = file_suffix

        if suffix != file_suffix:
            print("Skiping file: " + file_key + " suffix it is different than the initial: " + suffix)
            continue

        keys_list.append(file_key)

    continuation_token = None
    if "NextContinuationToken" in response:
        continuation_token = response["NextContinuationToken"]
    return keys_list, continuation_token, suffix


def private_start_parallel_download(keys_queue):
    # create a list to keep all processes
    threads = []
    print("Starting download...")

    for _ in range(10):
        thread = threading.Thread(target=private_download, args=(keys_queue,))
        threads.append(thread)

    # start all thread
    for thread in threads:
        thread.start()

    # make sure that all thread have finished
    for thread in threads:
        thread.join()

    return True
