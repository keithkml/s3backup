#!/usr/bin/env python

from dataclasses import dataclass
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
import csv

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


@dataclass
class FileToProcess:
    path: Path
    size: int


def main():
    parser = argparse.ArgumentParser(
        prog="ProgramName",
        description="What the program does",
        epilog="Text at the bottom of help",
    )
    parser.add_argument("bucket")
    parser.add_argument("dirs", nargs="+")
    parser.add_argument("--battery", action="store_true")
    parser.add_argument("--output-csv")

    args = parser.parse_args()
    dirs = [Path(dir) for dir in args.dirs]

    if not args.battery and not psutil.sensors_battery().power_plugged:
        logging.warn("Not doing anything; we're on battery!")
        return 1

    s3_client = boto3.client("s3")

    logging.info("Scanning directories...")
    files_to_process = sorted(
        (
            FileToProcess(path=f.absolute(), size=f.stat().st_size)
            for dir in dirs
            for f in dir.glob("**/*")
            if not (f.is_dir() or f.is_symlink())
        ),
        key=lambda f: str(f.path),
    )

    total_bytes = sum(f.size for f in files_to_process)

    logging.info(
        f"Will process {len(files_to_process):,} files, a total of {humanize.naturalsize(total_bytes)}"
    )

    output_csv_path = Path(args.output_csv)
    logging.info(f"Writing to {output_csv_path.absolute()}")

    with open(output_csv_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=("path", "sha256"))
        writer.writeheader()

        # for printing progress
        percent_int = 0
        processed_bytes = 0

        for index, file_to_process in enumerate(files_to_process):
            current_percent_int = int(index * 100.0 / len(files_to_process))
            if percent_int != current_percent_int:
                percent_int = current_percent_int
                logging.info(
                    f"Processed {index:,} of {len(files_to_process):,} files ({current_percent_int}%)"
                )
                logging.info(
                    f"Processed {humanize.naturalsize(processed_bytes)} of {humanize.naturalsize(total_bytes)} ({round(processed_bytes * 100.0 / total_bytes)}%)"
                )

            processed_bytes += file_to_process.size

            try:
                f = file_to_process.path
                csum = sha256sum(f)

                # store in manifest
                writer.writerow({"path": f.absolute(), "sha256": csum})
                csvfile.flush()

                size_on_disk = file_to_process.size
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
                logging.info("success!")
            except Exception as e:
                logging.warn(f"error uploading {f.absolute()}")
                logging.warn(e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
