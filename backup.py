import json
import yaml
import time
import os
import argparse
import requests
import oss2  # Alibaba Cloud OSS SDK
import wizard
import urllib3  # Added import for urllib3

def read_config():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml')
    with open(config_path, 'r') as config_file:
        return yaml.full_load(config_file)

class Atlassian:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.auth = (config['USER_EMAIL'], config['API_TOKEN'])
        self.session.headers.update({'Content-Type': 'application/json', 'Accept': 'application/json'})
        self.payload = {"cbAttachments": self.config['INCLUDE_ATTACHMENTS'], "exportToCloud": "frue"}
        self.start_confluence_backup = 'https://{}/wiki/rest/obm/1.0/runbackup'.format(self.config['HOST_URL'])
        self.start_jira_backup = 'https://{}/rest/backup/1/export/runbackup'.format(self.config['HOST_URL'])
        self.backup_status = {}
        self.wait = 10

    def create_confluence_backup(self):
        # Start Confluence backup process
        backup = self.session.post(self.start_confluence_backup, data=json.dumps(self.payload))
        if backup.status_code != 200:
            raise Exception(backup, backup.text)
        else:
            print('-> Confluence backup process successfully started')
            confluence_backup_status = 'https://{}/wiki/rest/obm/1.0/getprogress'.format(self.config['HOST_URL'])
            time.sleep(self.wait)

            retry_count = 0
            max_retries = 5
            while 'fileName' not in self.backup_status.keys():
                try:
                    response = self.session.get(confluence_backup_status)
                    response.raise_for_status()
                    self.backup_status = json.loads(response.text)
                    print('Current status: {progress}; {description}'.format(
                        progress=self.backup_status['alternativePercentage'],
                        description=self.backup_status['currentStatus']))
                except requests.exceptions.ConnectionError as e:
                    if retry_count < max_retries:
                        retry_count += 1
                        print(f"ConnectionError: {e}. Retrying {retry_count}/{max_retries}...")
                        time.sleep(self.wait)
                        continue
                    else:
                        print("Max retries reached. Exiting.")
                        raise
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
                    raise
                time.sleep(self.wait)
                
            return 'https://{url}/wiki/download/{file_name}'.format(
                url=self.config['HOST_URL'], file_name=self.backup_status['fileName'])

    def create_jira_backup(self):
        # Start Jira backup process
        backup = self.session.post(self.start_jira_backup, data=json.dumps(self.payload))
        if backup.status_code != 200:
            raise Exception(backup, backup.text)
        else:
            task_id = json.loads(backup.text)['taskId']
            print('-> Jira backup process successfully started: taskId={}'.format(task_id))
            jira_backup_status = 'https://{jira_host}/rest/backup/1/export/getProgress?taskId={task_id}'.format(
                jira_host=self.config['HOST_URL'], task_id=task_id)
            time.sleep(self.wait)

            retry_count = 0
            max_retries = 5
            while 'result' not in self.backup_status.keys():
                try:
                    response = self.session.get(jira_backup_status)
                    response.raise_for_status()
                    self.backup_status = json.loads(response.text)
                    print('Current status: {status} {progress}; {description}'.format(
                        status=self.backup_status['status'],
                        progress=self.backup_status['progress'],
                        description=self.backup_status['description']))
                except requests.exceptions.ConnectionError as e:
                    if retry_count < max_retries:
                        retry_count += 1
                        print(f"ConnectionError: {e}. Retrying {retry_count}/{max_retries}...")
                        time.sleep(self.wait)
                        continue
                    else:
                        print("Max retries reached. Exiting.")
                        raise
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred: {e}")
                    raise
                time.sleep(self.wait)

            return '{prefix}/{result_id}'.format(
                prefix='https://' + self.config['HOST_URL'] + '/plugins/servlet', result_id=self.backup_status['result'])

    def connect_to_oss(self):
        auth = oss2.Auth(self.config['UPLOAD_TO_OSS']['ACCESS_KEY_ID'], self.config['UPLOAD_TO_OSS']['ACCESS_KEY_SECRET'])
        bucket = oss2.Bucket(auth, self.config['UPLOAD_TO_OSS']['ENDPOINT'], self.config['UPLOAD_TO_OSS']['OSS_BUCKET'])
        return bucket

    def multipart_upload_to_oss(self, local_filename, remote_filename):
        bucket = self.connect_to_oss()

        # Create the full remote path using OSS_DIR from config
        remote_path = os.path.join(self.config['UPLOAD_TO_OSS']['OSS_DIR'].rstrip('/'), remote_filename)

        # Initialize multipart upload
        upload_id = bucket.init_multipart_upload(remote_path).upload_id

        parts = []
        part_number = 1
        part_size = 50 * 1024 * 1024  # Set part size to 50 MB

        total_size = os.path.getsize(local_filename)

        # Open the file and start uploading by parts
        with open(local_filename, 'rb') as file_to_upload:
            while True:
                chunk = file_to_upload.read(part_size)
                if not chunk:
                    break

                # Upload each part
                result = bucket.upload_part(
                    remote_path, 
                    upload_id, 
                    part_number, 
                    chunk
                )

                parts.append(oss2.models.PartInfo(part_number, result.etag))
                part_number += 1

                print(f"-> Uploaded part {part_number} of {remote_filename}")

        # Complete the multipart upload
        result = bucket.complete_multipart_upload(
            remote_path, 
            upload_id, 
            parts
        )

        if result.status == 200:
            print(f"-> File {remote_filename} successfully uploaded to OSS at {remote_path}.")

        # Delete the local file after successful upload
        os.remove(local_filename)
        print(f"-> Local file {local_filename} deleted.")

    def download_file(self, url, local_filename, retries=5):
        attempt = 0
        chunk_size = 50 * 1024 * 1024  # Set chunk size to 50 MB

        # Check if the file already exists and get its size
        if os.path.exists(local_filename):
            downloaded_size = os.path.getsize(local_filename)
        else:
            downloaded_size = 0

        while attempt < retries:
            try:
                headers = {"Range": f"bytes={downloaded_size}-"}  # Resume download from the last byte
                with self.session.get(url, stream=True, headers=headers, timeout=(60, 3600)) as r:  # Increased timeout to 60 sec connection, 3600 sec read
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0)) + downloaded_size
                    print(f"Resuming download from byte {downloaded_size} of {total_size} bytes")

                    # Continue downloading from where we left off
                    with open(local_filename, 'ab') as f:  # Open file in append mode
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                print(f"Downloaded {downloaded_size} of {total_size} bytes", end="\r")

                    print(f"-> File {local_filename} downloaded successfully.")
                    break  # Exit the loop after successful download

            except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ProtocolError, requests.exceptions.ConnectionError, urllib3.exceptions.ReadTimeoutError) as e:
                attempt += 1
                print(f"Error downloading file: {e}. Retrying {attempt}/{retries}...")
                if attempt >= retries:
                    print("Max retries reached. Download failed.")
                    raise  # Stop execution if max retries exceeded
            except Exception as e:
                print(f"Unexpected error: {e}")
                raise

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', action='store_true', dest='wizard', help='activate config wizard')
    parser.add_argument('-c', action='store_true', dest='confluence', help='activate confluence backup')
    parser.add_argument('-j', action='store_true', dest='jira', help='activate jira backup')
    args = parser.parse_args()

    if args.wizard:
        wizard.create_config()
    
    config = read_config()

    if config['HOST_URL'] == 'something.atlassian.net':
        raise ValueError('You forgot to edit config.yaml or to run the backup script with "-w" flag')

    print('-> Starting backup; include attachments: {}'.format(config['INCLUDE_ATTACHMENTS']))
    atlass = Atlassian(config)

    # Get the current date for naming the backup files
    current_date = time.strftime('%d%m%Y')

    # If no flags are provided, run both Confluence and Jira backups
    if not args.confluence and not args.jira:
        print("-> Running both Confluence and Jira backups as no specific option is provided.")
        confluence_backup_url = atlass.create_confluence_backup()
        print('-> Confluence Backup URL: {}'.format(confluence_backup_url))

        jira_backup_url = atlass.create_jira_backup()
        print('-> Jira Backup URL: {}'.format(jira_backup_url))

        # Download and upload both backups with human-readable names
        for backup_url, name in [
            (confluence_backup_url, f'confluence_export_{current_date}.zip'), 
            (jira_backup_url, f'jira_export_{current_date}.zip')
        ]:
            if config['DOWNLOAD_LOCALLY'] == 'true':
                atlass.download_file(backup_url, name)
                if config['UPLOAD_TO_OSS']['OSS_BUCKET'] != '':
                    atlass.multipart_upload_to_oss(name, name)
            else:
                print(f"-> Skipping local download for {name} backup.")
    
    # Run only Confluence or Jira backup based on provided flag
    if args.confluence:
        backup_url = atlass.create_confluence_backup()
        print('-> Confluence Backup URL: {}'.format(backup_url))
        file_name = f'confluence_export_{current_date}.zip'

        if config['DOWNLOAD_LOCALLY'] == 'true':
            atlass.download_file(backup_url, file_name)
            if config['UPLOAD_TO_OSS']['OSS_BUCKET'] != '':
                atlass.multipart_upload_to_oss(file_name, file_name)

    if args.jira:
        backup_url = atlass.create_jira_backup()
        print('-> Jira Backup URL: {}'.format(backup_url))
        file_name = f'jira_export_{current_date}.zip'

        if config['DOWNLOAD_LOCALLY'] == 'true':
            atlass.download_file(backup_url, file_name)
            if config['UPLOAD_TO_OSS']['OSS_BUCKET'] != '':
                atlass.multipart_upload_to_oss(file_name, file_name)
