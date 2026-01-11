from hdfs import InsecureClient
import os


def upload_parquet_to_hdfs(local_file_path, hdfs_path, hdfs_url='http://localhost:9870'):
    """
    Upload a Parquet file to HDFS

    Args:
        local_file_path: Path to local Parquet file
        hdfs_path: Destination path in HDFS (e.g., '/user/data/file.parquet')
        hdfs_url: HDFS NameNode URL (default: http://localhost:9870)
    """

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    client = InsecureClient(hdfs_url, user='hadoop')

    try:
        client.upload(hdfs_path, local_file_path, overwrite=True)

        status = client.status(hdfs_path)
        print(f"✓ Uploaded to HDFS:{hdfs_path} ({status['length']} bytes)")

        return True

    except Exception as e:
        print(f"✗ Error uploading file: {str(e)}")
        print("Should be setup \\etc\\hosts before")
        return False


def list_hdfs_directory(hdfs_path, hdfs_url='http://localhost:9870'):
    """List contents of an HDFS directory"""
    client = InsecureClient(hdfs_url, user='hadoop')

    try:
        files = client.list(hdfs_path, status=True)
        print(f"\nContents of {hdfs_path}:")
        for filename, status in files:
            print(f"  {filename} ({status['length']} bytes)")
    except Exception as e:
        print(f"Error listing directory: {str(e)}")


if __name__ == "__main__":
    LOCAL_FILE = "opdi_clean.parquet"
    HDFS_DESTINATION = "/user/data/opdi_clean.parquet"
    HDFS_URL = "http://localhost:9870"

    success = upload_parquet_to_hdfs(LOCAL_FILE, HDFS_DESTINATION, HDFS_URL)

    if success:
        hdfs_dir = os.path.dirname(HDFS_DESTINATION)
        list_hdfs_directory(hdfs_dir, HDFS_URL)