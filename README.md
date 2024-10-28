## Automated Backup Script for Jira and Confluence with Alibaba Cloud OSS Support

Original Repository

The original script was taken from the repository [jira-backup-py](https://github.com/datreeio/jira-backup-py) by [datreeio](https://github.com/datreeio). The original code was designed to automate the creation of Confluence and Jira backups with the ability to upload them to Amazon S3.

Changes Made

	1.	Alibaba Cloud OSS Support: Replaced Amazon S3 with support for uploading backups to Alibaba Cloud OSS.
	2.	Improved File Upload Resilience: Added error handling for issues such as connection drops and timeouts, along with a feature to resume downloads from where the failure occurred.
	3.	Increased Timeouts: For improved reliability when handling large files (over 100 GB), connection and data read timeouts were increased.
	4.	Chunk Size Optimization: Set an optimal chunk size for data transfer, balancing upload speed and reliability.

Author of Changes

[liquorice-head](https://github.com/liquorice-head) made the above changes and updates to meet the requirements for working with large backups and using OSS instead of S3.

License

The project is distributed under the MIT License, as specified in the original repository.
