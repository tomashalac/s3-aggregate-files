from multiprocessing import Process
from aggregateS3 import main, config
from pathlib import Path

def private_download(list_keys, config_obj):
    config.CONFIG = config_obj
    s3 = main.get_boto3()

    for file_key in list_keys:
        filename = config.CONFIG.local_folder_to_download + file_key.replace('/', '_')
        s3.download_file(Bucket=config.CONFIG.bucket_download, Key=file_key, Filename=filename)

def download_all_files():
    actual_count = 0
    s3 = main.get_boto3()

    key_list, continuation_token, suffix = private_list_files_in_bukcet(s3, suffix=None, max_keys=config.CONFIG.max_keys)
    actual_count += len(key_list)
    private_start_parallel_download(key_list)

    while actual_count < config.CONFIG.max_keys and continuation_token:
        new_key_list, continuation_token, _ = private_list_files_in_bukcet(s3, suffix, continuation_token, config.CONFIG.max_keys - actual_count)
        private_start_parallel_download(new_key_list)
        actual_count += len(new_key_list)
        key_list = key_list + new_key_list

    return key_list, suffix


def private_list_files_in_bukcet(s3, suffix, continuation_token="", max_keys=1000):
    # AWS limit
    if max_keys > 1000:
        max_keys = 1000

    if continuation_token:
        response = s3.list_objects_v2(Bucket=config.CONFIG.bucket_download, MaxKeys=max_keys, Prefix=config.CONFIG.bucket_download_prefix, ContinuationToken=continuation_token)
    else:
        response = s3.list_objects_v2(Bucket=config.CONFIG.bucket_download, MaxKeys=max_keys, Prefix=config.CONFIG.bucket_download_prefix)

    print("Downloading "+str(len(response['Contents']))+" files from AWS S3")
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
    return keys_list, response["NextContinuationToken"], suffix


def private_start_parallel_download(keys_list):
    # create a list to keep all processes
    processes = []

    total_files = len(keys_list)
    keys_list = list(private_chunks(keys_list, int(total_files/20)))

    for id in range(20):
        # create the process, pass instance and connection
        process = Process(target=private_download, args=(keys_list[id], config.CONFIG))
        processes.append(process)

    # start all processes
    for process in processes:
        process.start()

    # make sure that all processes have finished
    for process in processes:
        process.join()

    print("All " + str(total_files) + " files downloaded.")
    return True


def private_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
