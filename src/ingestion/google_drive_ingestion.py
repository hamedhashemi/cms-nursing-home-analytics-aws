import argparse
import io
import json
import sys

import boto3

from botocore.exceptions import ClientError

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--SECRET_NAME", required=True)
    parser.add_argument("--BUCKET_NAME", required=True)
    parser.add_argument("--PBJ_FOLDER_ID", required=True)
    parser.add_argument("--SUPPORTING_FOLDER_ID", required=True)

    args, unknown = parser.parse_known_args()

    return args


# ============================================================
# AWS CLIENTS
# ============================================================

s3 = boto3.client("s3")
secretsmanager = boto3.client("secretsmanager")


STATE_KEY = "control/ingestion_state.json"


# ============================================================
# GOOGLE AUTHENTICATION
# ============================================================

def get_google_credentials(secret_name):
    print("Loading Google OAuth credentials from Secrets Manager...")

    response = secretsmanager.get_secret_value(
        SecretId=secret_name
    )

    secret = json.loads(response["SecretString"])

    token_info = json.loads(secret["token_json"])

    scopes = [
        "https://www.googleapis.com/auth/drive.readonly"
    ]

    credentials = Credentials.from_authorized_user_info(
        token_info,
        scopes=scopes
    )

    if credentials.expired and credentials.refresh_token:
        print("Google access token expired. Refreshing...")
        credentials.refresh(Request())
        print("Google access token refreshed successfully.")

    return credentials


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state(bucket_name):
    try:
        response = s3.get_object(
            Bucket=bucket_name,
            Key=STATE_KEY
        )

        state = json.loads(
            response["Body"].read().decode("utf-8")
        )

        print(
            f"Existing ingestion state loaded: "
            f"{len(state.get('files', {}))} files"
        )

        return state

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code in ["NoSuchKey", "404"]:
            print("No previous ingestion state found.")
            print("Starting first ingestion.")

            return {
                "files": {}
            }

        raise


def save_state(bucket_name, state):
    s3.put_object(
        Bucket=bucket_name,
        Key=STATE_KEY,
        Body=json.dumps(
            state,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json"
    )

    print(
        f"Ingestion state saved to "
        f"s3://{bucket_name}/{STATE_KEY}"
    )


# ============================================================
# GOOGLE DRIVE LISTING
# ============================================================

def list_drive_files(service, folder_id):
    files = []

    page_token = None

    query = (
        f"'{folder_id}' in parents "
        "and trashed = false"
    )

    while True:
        response = service.files().list(
            q=query,
            spaces="drive",
            fields=(
                "nextPageToken,"
                "files("
                "id,"
                "name,"
                "mimeType,"
                "modifiedTime,"
                "size"
                ")"
            ),
            pageToken=page_token,
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        files.extend(
            response.get("files", [])
        )

        page_token = response.get(
            "nextPageToken"
        )

        if not page_token:
            break

    return files


# ============================================================
# FILE DOWNLOAD
# ============================================================

def download_drive_file(service, file):
    file_id = file["id"]
    mime_type = file.get("mimeType", "")

    # Ignore folders
    if mime_type == "application/vnd.google-apps.folder":
        return None

    # Google-native documents cannot use get_media directly.
    # This project expects CSV/PDF/etc. source files.
    if mime_type.startswith(
        "application/vnd.google-apps."
    ):
        print(
            f"SKIP unsupported Google-native file: "
            f"{file['name']}"
        )
        return None

    request = service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True
    )

    buffer = io.BytesIO()

    downloader = MediaIoBaseDownload(
        buffer,
        request
    )

    done = False

    while not done:
        status, done = downloader.next_chunk()

    buffer.seek(0)

    return buffer


# ============================================================
# INGEST ONE SOURCE FOLDER
# ============================================================

def ingest_folder(
    service,
    folder_id,
    source_name,
    s3_prefix,
    bucket_name,
    state
):

    print()
    print("=" * 70)
    print(f"SOURCE: {source_name}")
    print(f"S3 PREFIX: {s3_prefix}")
    print("=" * 70)

    drive_files = list_drive_files(
        service,
        folder_id
    )

    print(
        f"Found {len(drive_files)} files."
    )

    stats = {
        "new": 0,
        "modified": 0,
        "skipped": 0,
        "failed": 0
    }

    for file in drive_files:

        file_id = file["id"]
        file_name = file["name"]

        mime_type = file.get(
            "mimeType",
            ""
        )

        modified_time = file.get(
            "modifiedTime"
        )

        # Ignore folders
        if mime_type == "application/vnd.google-apps.folder":
            print(
                f"SKIP FOLDER: {file_name}"
            )
            continue

        previous = state["files"].get(
            file_id
        )

        if previous is None:

            status = "NEW"

        elif (
            previous.get("modifiedTime")
            != modified_time
        ):

            status = "MODIFIED"

        else:

            print(
                f"SKIP: {file_name}"
            )

            stats["skipped"] += 1

            continue

        try:

            print(
                f"{status}: {file_name}"
            )

            file_buffer = download_drive_file(
                service,
                file
            )

            if file_buffer is None:
                stats["skipped"] += 1
                continue

            s3_key = (
                f"{s3_prefix}/"
                f"{file_name}"
            )

            s3.upload_fileobj(
                file_buffer,
                bucket_name,
                s3_key
            )

            print(
                f"Uploaded → "
                f"s3://{bucket_name}/{s3_key}"
            )

            # Update state ONLY after successful S3 upload
            state["files"][file_id] = {
                "name": file_name,
                "modifiedTime": modified_time,
                "source": source_name,
                "s3_key": s3_key
            }

            if status == "NEW":
                stats["new"] += 1
            else:
                stats["modified"] += 1

        except Exception as e:

            stats["failed"] += 1

            print(
                f"FAILED: {file_name}"
            )

            print(
                f"ERROR: {str(e)}"
            )

    return stats


# ============================================================
# SUMMARY
# ============================================================

def print_summary(
    pbj_stats,
    supporting_stats
):

    total = {
        "new": (
            pbj_stats["new"]
            + supporting_stats["new"]
        ),
        "modified": (
            pbj_stats["modified"]
            + supporting_stats["modified"]
        ),
        "skipped": (
            pbj_stats["skipped"]
            + supporting_stats["skipped"]
        ),
        "failed": (
            pbj_stats["failed"]
            + supporting_stats["failed"]
        )
    }

    print()
    print("=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)

    print()
    print("PBJ")
    print(f"NEW       : {pbj_stats['new']}")
    print(f"MODIFIED  : {pbj_stats['modified']}")
    print(f"SKIPPED   : {pbj_stats['skipped']}")
    print(f"FAILED    : {pbj_stats['failed']}")

    print()
    print("SUPPORTING")
    print(f"NEW       : {supporting_stats['new']}")
    print(f"MODIFIED  : {supporting_stats['modified']}")
    print(f"SKIPPED   : {supporting_stats['skipped']}")
    print(f"FAILED    : {supporting_stats['failed']}")

    print()
    print("TOTAL")
    print(f"NEW       : {total['new']}")
    print(f"MODIFIED  : {total['modified']}")
    print(f"SKIPPED   : {total['skipped']}")
    print(f"FAILED    : {total['failed']}")

    print("=" * 70)

    return total


# ============================================================
# MAIN
# ============================================================

def main():

    args = parse_args()

    print("=" * 70)
    print("Healthcare Metrics - Google Drive Ingestion")
    print("=" * 70)

    credentials = get_google_credentials(
        args.SECRET_NAME
    )

    service = build(
        "drive",
        "v3",
        credentials=credentials,
        cache_discovery=False
    )

    print(
        "Google Drive authentication successful."
    )

    state = load_state(
        args.BUCKET_NAME
    )

    # --------------------------------------------------------
    # PBJ MAIN DATA
    # --------------------------------------------------------

    pbj_stats = ingest_folder(
        service=service,
        folder_id=args.PBJ_FOLDER_ID,
        source_name="PBJ",
        s3_prefix="raw/pbj",
        bucket_name=args.BUCKET_NAME,
        state=state
    )

    # Save after PBJ
    save_state(
        args.BUCKET_NAME,
        state
    )

    # --------------------------------------------------------
    # SUPPORTING DATA
    # --------------------------------------------------------

    supporting_stats = ingest_folder(
        service=service,
        folder_id=args.SUPPORTING_FOLDER_ID,
        source_name="SUPPORTING",
        s3_prefix="raw/supporting",
        bucket_name=args.BUCKET_NAME,
        state=state
    )

    # Save final state
    save_state(
        args.BUCKET_NAME,
        state
    )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    total = print_summary(
        pbj_stats,
        supporting_stats
    )

    if total["failed"] > 0:
        raise RuntimeError(
            f"Ingestion completed with "
            f"{total['failed']} failed files."
        )

    print()
    print(
        "Google Drive ingestion completed successfully."
    )


if __name__ == "__main__":
    main()