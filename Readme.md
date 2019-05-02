# S3 BackUp File Transfer Utility 
This utility uses single and multipart uploads using boto3 library to transfer files to S3 bucket. 

## Get Your Environment Ready
Environment Required = __python3__ -- I tested it on python3.7.

1. Make sure you have python3 and pip installed.
2.  run `pip install -r requirements.txt` to install packages. (it will install boto3) 

# How to Run script. 
1. Run in *normal mode* with command `python3 s3FileTransfer.py --bucketname test-s3-bucket --backupfolder /path/to/backup/folder/`
    * Example: if I have 3 files present in Backup Folder. My path would look like. 
    
        * /home/user/Documents/BackUpFolder/backupFile1.dat
        * /home/user/Documents/BackUpFolder/backupFile2.dat
        * /home/user/Documents/BackUpFolder/backupFile3.dat
    
        I will run my script by this command. 
        
        `python3 s3FileTransfer.py --bucketname test-s3-bucket --backupfolder /home/user/Documents/BackUpFolder`
        
        It will create a bucket in S3 in eu-west-region and all the files `backupFile*.dat` would be put in the bucket named `test-s3-bucket` on s3. if a bucket with
        this name already exists, the existing bucket will be used and files would be transfered to that. 
        
        *A stateFile would be generated in the current directory which will show the status of each individual upload.* 


2. Run in *crashrun mode*. Pardon I could not come up with a better name. But if for some reason, all or some files were not transfered in normal mode, it would be reflected in stateFile. 
   To run this utility in crashrun, add a flag `--crashrun 1`. Upon running, it would read the stateFile and transfer only those files that were failed in the first run. 
   StateFile is updated after Each run.
    
    * Example: if *backupFile3.dat* failed to be uploaded on S3 in normal mode, StateFile would look like this
   
       ```[
        {
        "filepath": "/home/user/Documents/BackUpFolder/backupFile1.dat",
        "uploadedSuccess": 1,
        "filename": "backupFile1.datt"
        },
        {
        "filepath": "/home/user/Documents/BackUpFolder/backupFile2.dat",
        "uploadedSuccess": 1,
        "filename": "backupFile2.datt"
        },
        {
        "filepath": "/home/user/Documents/BackUpFolder/backupFile3.dat",
        "uploadedSuccess": 0,
        "filename": "backupFile1.datt"
        },
        ]```
    
    As you can see, `uploadedSuccess = 0` for `backupFile3.data`. Upon running in crashrun mode, it will read the stateFile and will transfer only backupFile3.dat. At the end of it, 
    stateFile would be updated to reflect the changes, if any. 
    
    Command to be used in case of crashrun mode. 
    `python3 s3FileTransfer.py --bucketname test-s3-bucket --backupfolder /home/user/Documents/BackUpFolder --crashrun 1`

# About Single and Multipart uploads.
* If a file Size is less than 1GB, it will calculate the md5 hash and upload the file in single part. This md5 is verified against the one calculated by aws to verify  the data integrity. 
* If a file Size is greater than 1GB, it will use multipart api call of boto3 to upload in chunks. it is faster and concurrent so saves time. 
    
    
    
    
    
