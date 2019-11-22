import os
import json

CONFIG = None


class Config:

    def __init__(self, event):

        if os.path.exists("config.json"):
            with open("config.json") as config:
                self.config_file = json.load(config)

        self.json = event
        self.bucket_download = self.get_key("bucket_download", None, has_default=False)
        self.bucket_upload = self.get_key("bucket_upload", None, show_warning=False, has_default=True if self.config_file else False)

        # optional
        self.max_size_bytes = int(self.get_key("max_size_bytes", 0, show_warning=False))
        self.min_size_bytes = int(self.get_key("min_size_bytes", 0, show_warning=False))

        if self.max_size_bytes == 0:
            self.max_size_bytes = int(self.get_key("max_size_megas", 128)) * 1024 * 1024  # Max 128MB

        if self.min_size_bytes == 0:
            self.min_size_bytes = int(self.get_key("min_size_megas", 100)) * 1024 * 1024  # Min 100MB

        self.output_file = self.get_key("output_file", "%d/%m/%Y %H:%M:%S")
        self.delete_old_file = bool(self.get_key("delete_old_file", False))
        self.max_keys = int(self.get_key("max_keys", 1000))
        self.bucket_download_prefix = self.get_key("bucket_download_prefix", "")
        self.only_download = bool(self.get_key("only_download", False, show_warning=False))
        self.local_folder_to_download = self.get_key("local_folder_to_download", "/tmp/", show_warning=False)
        self.aws_access_key_id = self.get_key("aws_access_key_id", None, show_warning=False)
        self.aws_secret_access_key = self.get_key("aws_secret_access_key", None, show_warning=False)

    def get_key(self, key, default, show_warning=True, has_default=True):
        if key in self.json:
            return self.json[key]
        if key in self.config_file:
            return self.config_file[key]

        if has_default:
            if show_warning:
                print("Config name " + key + " not found, using default value of: " + str(default))
            return default
        else:
            raise Exception("Key not found in config: " + key)
