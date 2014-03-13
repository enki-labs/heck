import os

os.environ["HECK_STORE_ENGINE"] = "posix"
os.environ["HECK_STORE_BASE"] = "/home/data"
os.environ["HECK_STORE_HOST"] = "localhost"
os.environ["HECK_STORE_PORT"] = "50070"
os.environ["HECK_STORE_USER"] = "hdfs"
os.environ["HECK_DB"] = "postgresql://heck:pass@localhost/heck"
os.environ["HECK_LOG"] = "INFO"
os.environ["HECK_REDIS"] = "redis://:jdk9dD0knn8md922XkdWQ980D9dkSD90S809dk@localhost:6379/1"
