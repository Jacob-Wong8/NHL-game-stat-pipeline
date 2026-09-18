import argparse
from pathlib import Path

import boto3


def build_s3_key(file_path: Path, prefix: str) -> str:
    filename = file_path.name
    clean_prefix = prefix.strip("/")
    return f"{clean_prefix}/{filename}" if clean_prefix else filename


def upload_file_to_s3(file_path: Path, bucket: str, prefix: str = "expected/hockey_reference") -> str:
    """Upload one generated batch file and return its S3 URI."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    key = build_s3_key(file_path, prefix)
    boto3.client("s3").upload_file(
        str(file_path),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )
    return f"s3://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload one expected-stats file to S3.")
    parser.add_argument("file", type=Path, help="Generated JSONL or JSON file to upload")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument(
        "--prefix",
        default="expected/hockey_reference",
        help="S3 key prefix; the input filename is appended",
    )
    args = parser.parse_args()

    s3_uri = upload_file_to_s3(args.file, args.bucket, args.prefix)
    print(f"Uploaded -> {s3_uri}")


if __name__ == "__main__":
    main()