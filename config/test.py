import os

os.environ["HECK_STORE_ENGINE"] = "posix"
os.environ["HECK_STORE_BASE"] = "/home/data"
os.environ["HECK_STORE_HOST"] = "localhost"
os.environ["HECK_STORE_PORT"] = "50070"
os.environ["HECK_STORE_USER"] = "hdfs"
os.environ["HECK_DB"] = "postgresql://heck:pass@localhost/heck_test"

