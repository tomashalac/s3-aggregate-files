
CONFIG = None

class Config:

    def __init__(self, json):
        self.json = json
        self.bucket_download = json["bucket_download"]
        self.bucket_upload = json["bucket_upload"]

        # optional
        self.max_size_bytes = int(self.get_key("max_size_bytes", 0, show_warning=False))
        self.min_size_bytes = int(self.get_key("min_size_bytes", 0, show_warning=False))

        if self.max_size_bytes == 0:
            self.max_size_bytes = int(self.get_key("max_size_megas", 128)) * 1024 * 1024 # Max 128MB

        if self.min_size_bytes == 0:
            self.min_size_bytes = int(self.get_key("min_size_megas", 100)) * 1024 * 1024 # Min 100MB

        self.output_file = self.get_key("output_file", "%d/%m/%Y %H:%M:%S")
        self.delete_old_file = bool(self.get_key("delete_old_file", False))
        self.max_keys = int(self.get_key("max_keys", 1000))
        self.bucket_download_prefix = self.get_key("bucket_download_prefix", "")

    def get_key(self, key, default, show_warning = True):
        if show_warning and key not in self.json:
            print("Config name " + key + " not found, using default value of: " + str(default))
        return self.json[key] if key in self.json else default