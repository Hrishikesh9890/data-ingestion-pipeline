import os
import time
import yaml
import getpass
from io import BytesIO
from datetime import datetime
import pytz
import pandas as pd

from azure.storage.blob import BlobServiceClient
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
from tenacity import retry, wait_fixed, stop_after_attempt, RetryError

# ==================== Global Configurations ====================

# ADLS Configuration

connection_string = ""
container_name = ""
adls_folder = ""

# SharePoint Configuration
sharepoint_url = ""
username = ""
password = ""
sharepoint_folder_path = ""

# Local Paths
timestamp_file = "C:/Users/hrishikesh.mohitkar/Downloads/projects/QA_sys/notebook/config.yaml"
log_dir = "C:/Users/hrishikesh.mohitkar/Downloads/projects/QA_sys/notebook/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Allowed file types for ingestion
ALLOWED_EXTENSIONS = [".txt", ".doc", ".docx", ".pdf", ".ppt", ".pptx", ".xls", ".xlsx"]

# ==================== Global Log Lists ====================
activity_log_entries = []  # Activity logs
error_log_entries = []     # Error logs

# ==================== Helper Logging Functions ====================
def append_error_log(file_name, error_message):
    """Append an error log entry."""
    error_log_entries.append({
        "File Name": file_name,
        "Error Message": error_message,
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

def append_activity_log(file_name, file_size, last_modified, modified_by, upload_timestamp, reference_timestamp):
    """Append an activity log entry."""
    activity_log_entries.append({
        "File Name": file_name,
        "File Size": file_size,
        "Last Modified Timestamp": last_modified,
        "Modified By": modified_by,
        "Upload Timestamp": upload_timestamp,
        "Reference Timestamp": reference_timestamp
    })

# ==================== Configuration Functions ====================
def load_config(file_path):
    """
    Loads the configuration file.
    Returns a tuple: (reference_timestamp, allowed_extensions, first_run)
    If the file doesn't exist, defaults to: (None, ALLOWED_EXTENSIONS, "yes")
    """
    try:
        if not os.path.exists(file_path):
            return None, ALLOWED_EXTENSIONS, "yes"
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        reference_timestamp = (
            time.mktime(time.strptime(data["reference_run_time"], "%Y-%m-%d %H:%M:%S"))
            if "reference_run_time" in data else None
        )
        allowed_extensions = data.get("allowed_extensions", ALLOWED_EXTENSIONS)
        first_run = data.get("first_run", "yes").strip().lower()
        return reference_timestamp, [ext.lower() for ext in allowed_extensions], first_run
    except Exception as e:
        append_error_log("config.yaml", str(e))
        print(f"Error loading configuration: {e}")
        raise

def save_config(file_path, timestamp, allowed_extensions, first_run):
    """
    Saves the configuration file with a human-readable timestamp.
    """
    try:
        readable_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        with open(file_path, "w") as file:
            yaml.dump({
                "reference_run_time": readable_timestamp,
                "allowed_extensions": allowed_extensions,
                "first_run": first_run
            }, file)
        print(f"Configuration saved with reference timestamp: {readable_timestamp}")
    except Exception as e:
        append_error_log("config.yaml", str(e))
        print(f"Error saving configuration: {e}")

# ==================== Connection Functions with Retry ====================
def before_sleep_log(retry_state):
    print(f"[Retry] Attempt {retry_state.attempt_number} failed. Retrying in {retry_state.next_action.sleep} seconds...")

@retry(wait=wait_fixed(10), stop=stop_after_attempt(3), reraise=True, before_sleep=before_sleep_log)
def get_sharepoint_context():
    """
    Returns a SharePoint ClientContext.
    Retries the connection if there are transient errors.
    """
    print("Attempting to connect to SharePoint...")
    context = ClientContext(sharepoint_url).with_credentials(UserCredential(username, password))
    folder = context.web.get_folder_by_server_relative_url(sharepoint_folder_path)
    context.load(folder)
    context.execute_query()
    return context

@retry(wait=wait_fixed(10), stop=stop_after_attempt(3), reraise=True, before_sleep=before_sleep_log)
def get_adls_client():
    """
    Returns an ADLS BlobServiceClient.
    Retries the connection if there are transient errors.
    Also verifies that the container exists by retrieving its properties.
    """
    print("Attempting to connect to ADLS...")
    client = BlobServiceClient.from_connection_string(connection_string)
    # Force a connectivity check by retrieving the container properties.
    container_client = client.get_container_client(container_name)
    container_client.get_container_properties()
    return client

# ==================== Duplicate & Timestamp Check ====================
def should_upload_file(blob_service_client, file_name, new_file_ts):
    """
    Checks if a file with the given file_name already exists in ADLS.
    If it exists, compare its stored 'sp_last_modified' metadata (or the blob's last_modified)
    with new_file_ts (a datetime object in UTC).
    
    Returns True if:
      - The file does not exist, or
      - The new file's timestamp is later than the stored timestamp.
    
    Returns False otherwise.
    """
    blob_path = f"{adls_folder}/{file_name}"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    try:
        properties = blob_client.get_blob_properties()
        if properties.metadata and "sp_last_modified" in properties.metadata:
            stored_ts_str = properties.metadata["sp_last_modified"]
            stored_ts = datetime.strptime(stored_ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
        else:
            stored_ts = properties.last_modified  # typically already in UTC
        if new_file_ts > stored_ts:
            print(f"New file {file_name} is newer than stored file (SP: {new_file_ts} > Stored: {stored_ts}).")
            return True
        else:
            print(f"Existing file {file_name} is newer or equal (Stored: {stored_ts} >= SP: {new_file_ts}). Skipping upload.")
            return False
    except Exception as e:
        # If the blob doesn't exist, an exception is thrown; in that case, we upload.
        print(f"File {file_name} does not exist in target storage. Proceeding with upload.")
        return True

# ==================== ADLS Upload Function with Metadata ====================
def upload_to_adls(file_name, file_content, blob_service_client, last_modified, reference_timestamp):
    """
    Uploads a file to ADLS with metadata and logs the activity.
    'last_modified' is expected as an ISO string (e.g., "2025-01-01T12:34:56Z").
    """
    try:
        file_size = len(file_content)
        modified_by = getpass.getuser()
        upload_timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        blob_path = f"{adls_folder}/{file_name}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # Set metadata: store the SharePoint file's last-modified timestamp in ISO format.
        metadata = {"sp_last_modified": last_modified}
        
        blob_client.upload_blob(file_content, overwrite=True, metadata=metadata)
        print(f"Uploaded: {file_name} ({file_size} bytes) by {modified_by} at {upload_timestamp}")
        append_activity_log(file_name, file_size, last_modified, modified_by, upload_timestamp, reference_timestamp)
    except Exception as e:
        append_error_log(file_name, f"Error during ADLS upload: {e}")
        print(f"Error uploading file {file_name}: {e}")
        raise

# ==================== File Upload with Integrity Check & Retry ====================
@retry(wait=wait_fixed(5), stop=stop_after_attempt(3), reraise=True)
def upload_and_verify_file(file_name, file_content, blob_service_client, last_modified, reference_timestamp):
    """
    Uploads a file to ADLS with metadata, then verifies its integrity by checking that
    the size of the uploaded blob matches the original file size.
    If the verification fails, an exception is raised to trigger a retry.
    """
    # Perform the upload.
    upload_to_adls(file_name, file_content, blob_service_client, last_modified, reference_timestamp)
    
    # Verify file integrity by comparing sizes.
    blob_path = f"{adls_folder}/{file_name}"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    properties = blob_client.get_blob_properties()
    uploaded_size = properties.size  # Size in bytes.
    original_size = len(file_content)
    
    if uploaded_size != original_size:
        raise Exception(f"File integrity check failed for {file_name}: expected {original_size} bytes, got {uploaded_size} bytes")
    else:
        print(f"File integrity verified for {file_name}")

# ==================== Main Monitoring & Upload Function ====================
def monitor_and_upload():
    try:
        # Establish ADLS connection with retry.
        try:
            blob_service_client = get_adls_client()
            print("Successfully connected to ADLS!")
        except RetryError as e:
            append_error_log("ADLS Connection", f"Failed to connect to ADLS after retries: {e}")
            print("Failed to connect to ADLS after multiple attempts.")
            return

        
        try:
            context = get_sharepoint_context()
            print("Successfully connected to SharePoint!")
        except RetryError as e:
            append_error_log("SharePoint Connection", f"Failed to connect to SharePoint after retries: {e}")
            print("Failed to connect to SharePoint after multiple attempts.")
            return

        
        reference_timestamp, allowed_extensions, first_run = load_config(timestamp_file)
        if reference_timestamp is not None:
            reference_datetime_utc = datetime.utcfromtimestamp(reference_timestamp).replace(tzinfo=pytz.utc)
        else:
            reference_datetime_utc = datetime.min.replace(tzinfo=pytz.utc)

        # Retrieve files from SharePoint.
        folder = context.web.get_folder_by_server_relative_url(sharepoint_folder_path)
        files = folder.files
        context.load(files)
        context.execute_query()

        uploaded_files = False
        max_processed_modified_time = reference_datetime_utc

        if first_run == "yes":
            print("First run detected: Uploading all files from SharePoint.")
        else:
            print(f"Subsequent run: Checking for files modified after {reference_datetime_utc} (UTC).")

        # Process each file.
        for file in files:
            try:
                file_name = file.properties["Name"]

                # Validate allowed file type.
                if not any(file_name.lower().endswith(ext) for ext in allowed_extensions):
                    append_error_log(file_name, "Unsupported file type. Skipping file.")
                    print(f"Unsupported file type: {file_name}")
                    continue

                file_url = file.properties["ServerRelativeUrl"]
                file_last_modified = file.properties["TimeLastModified"]

                # Convert SharePoint's TimeLastModified to a UTC datetime object.
                if isinstance(file_last_modified, str):
                    file_last_modified = datetime.strptime(file_last_modified, "%Y-%m-%dT%H:%M:%SZ")
                if file_last_modified.tzinfo is None:
                    file_last_modified_utc = pytz.utc.localize(file_last_modified)
                else:
                    file_last_modified_utc = file_last_modified.astimezone(pytz.utc)

                # For subsequent runs, skip files not modified after the saved reference timestamp.
                if first_run == "no" and file_last_modified_utc <= reference_datetime_utc:
                    print(f"Skipping {file_name} (modified at {file_last_modified_utc} UTC)")
                    continue

                # Convert the timestamp to an ISO string for metadata.
                iso_last_modified = file_last_modified_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

                # Check if the file should be uploaded (duplicate check & timestamp validation).
                if not should_upload_file(blob_service_client, file_name, file_last_modified_utc):
                    continue

                # Download file from SharePoint.
                try:
                    file_data = File.open_binary(context, file_url)
                except Exception as e:
                    append_error_log(file_name, f"Error downloading file: {e}")
                    print(f"Error downloading file {file_name}: {e}")
                    continue

                # Upload file to ADLS with integrity verification and retry logic.
                try:
                    upload_and_verify_file(
                        file_name,
                        file_data.content,
                        blob_service_client,
                        iso_last_modified,
                        reference_datetime_utc.strftime("%Y-%m-%d %H:%M:%S")
                    )
                except Exception as e:
                    append_error_log(file_name, f"Failed to upload after retries: {e}")
                    print(f"Failed to upload {file_name} after retries: {e}")
                    continue

                uploaded_files = True

                # Update maximum processed modified time if applicable.
                if file_last_modified_utc > max_processed_modified_time:
                    max_processed_modified_time = file_last_modified_utc

            except Exception as general_e:
                file_identifier = file.properties.get("Name", "Unknown")
                append_error_log(file_identifier, f"General error during processing: {general_e}")
                print(f"Error processing file {file_identifier}: {general_e}")
                continue

        # Update configuration with the new reference timestamp if files were processed.
        if uploaded_files or first_run == "yes":
            new_reference_timestamp = max_processed_modified_time.timestamp()
            save_config(timestamp_file, new_reference_timestamp, allowed_extensions, "no")
            print(f"Reference timestamp updated to {max_processed_modified_time} UTC.")
        else:
            print("No new or modified files found.")

    except Exception as e:
        append_error_log("monitor_and_upload", str(e))
        print(f"Unexpected error in monitor_and_upload: {e}")
    finally:
        # Write log data to separate Excel files.
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        activity_log_file = os.path.join(log_dir, f"activity_log_{run_timestamp}.xlsx")
        error_log_file = os.path.join(log_dir, f"error_log_{run_timestamp}.xlsx")
        try:
            if activity_log_entries:
                df_activity = pd.DataFrame(activity_log_entries)
                df_activity.to_excel(activity_log_file, index=False)
                print(f"Activity log saved to {activity_log_file}")
            else:
                print("No activity logs to save.")
        except Exception as e:
            print(f"Error saving activity log: {e}")
        try:
            if error_log_entries:
                df_error = pd.DataFrame(error_log_entries)
                df_error.to_excel(error_log_file, index=False)
                print(f"Error log saved to {error_log_file}")
            else:
                print("No error logs to save.")
        except Exception as e:
            print(f"Error saving error log: {e}")

# ==================== Main Execution ====================
if __name__ == "__main__":
    monitor_and_upload()
