#!/usr/bin/env python

import hashlib
import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import os
import psutil
import argparse
import sys
import humanize

logging.basicConfig(stream=sys.stdout, level=logging.INFO)


def sha256sum(filename):
    h = hashlib.sha256()
    b = bytearray(128 * 1024)
    mv = memoryview(b)
    with open(filename, "rb", buffering=0) as f:
        while n := f.readinto(mv):
            h.update(mv[:n])
    return h.hexdigest()


ROOT_S3_CAS = Path("cas")


def main():
    parser = argparse.ArgumentParser(
        prog="ProgramName",
        description="What the program does",
        epilog="Text at the bottom of help",
    )
    parser.add_argument("bucket")
    parser.add_argument("dirs", nargs="+")
    parser.add_argument("--battery", action="store_true")

    args = parser.parse_args()
    dirs = [Path(dir) for dir in args.dirs]

    if not args.battery and not psutil.sensors_battery().power_plugged:
        print("Not doing anything; we're on battery!")
        return 1

    s3_client = boto3.client("s3")

    for dir in dirs:
        for f in dir.glob("**/*"):
            if f.is_dir():
                continue
            try:
                csum = sha256sum(f)
                size_on_disk = f.stat().st_size
                path_in_s3 = str(ROOT_S3_CAS / csum[0] / csum[1] / csum)
                try:
                    obj = s3_client.get_object(Bucket=args.bucket, Key=path_in_s3)
                    if "ContentLength" in obj and obj["ContentLength"] == size_on_disk:
                        logging.info(
                            f"Skipping {f.absolute()} ({humanize.naturalsize(size_on_disk)}) because it's already in S3 as {path_in_s3}"
                        )
                        continue
                except s3_client.exceptions.NoSuchKey:
                    pass
                # TODO: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/object/checksum_sha256.html
                logging.info(
                    f"uploading {f} ({humanize.naturalsize(size_on_disk)}) to {path_in_s3}"
                )

                response = s3_client.upload_file(f.absolute(), args.bucket, path_in_s3)
                print("success!")
            except Exception as e:
                print(f"error uploading {f.absolute()}")
                print(e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
