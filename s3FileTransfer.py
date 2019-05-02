import json
import boto3
import os
import argparse
import hashlib
import sys
import base64
import uuid
import pprint
import logging
import logging as Logger
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
s3Log = logging.getLogger("s3transfer-logger")
client = boto3.client('s3')

class ArgumentsParser:
    """
        Class responsible for parsing and storing command line
        arguments for the s3 backup file transfers..
    """
    def ReadArgs(self):
        parser = argparse.ArgumentParser(description='Give Path of Folder Containing Backup Files and name of S3 bucket')
        parser.add_argument("--bucketname", help="name of the s3 bucket to be created; "
                                                  "if a bucket exists with this name, it will be used", required=True)
        parser.add_argument("--backupfolder", help="path of the folder containing all the backup files to be pushed", required=True)
        parser.add_argument("--crashrun", help="give 1 if you want to do crash run; default is 0", type=int,default=0)

        args = parser.parse_args()
        return args.bucketname, args.backupfolder,args.crashrun

class S3Transfer:
    """
        Class responsible for transering files from folder
        using single and multipars depending upon the size.
    """
    def __init__(self, backUpFolder, bucketname):
        self.backupFolder = backUpFolder
        self.fileTobeUploaded=[] # with each file
        self.bucketName = bucketname

    def ParseStateFile(self):
        """
            This function is only called when flag --crashrun is set 1 from command line arguments.
            We then check the latest stateFile as that will give us information that which file is needed
            to be uploaded again. Files which were failed to be uploaded, have a key 'uploadedSuccess' set to 0 in state File.
        """
        s3Log.info ("In crash run - updating File Paths from stateFile! ")
        try:
            with open('stateFile.json') as statefile:
                self.fileTobeUploaded=json.load(statefile)
        except json.decoder.JSONDecodeError as e:
            s3Log.error("FATAL ERROR: Unable to parse stateFile; wont be able to list down backup file paths.  ")
            sys.exit(1)

    def PopulateFilePaths(self):
        """
            This function is called in normal setting. Means, we are running the script for the first time to transfer the backup Files.
            it will populate the array with the information of filepaths and set the flag uploadedSuccess:0 which means, this file has yet
            to be uploaded.
        """
        if os.path.isdir(self.backupFolder) == True:
            s3Log.info("BackUp Folder = {}".format(self.backupFolder))
            backUpFilestoTransfer = (os.listdir(self.backupFolder))
            for eachfilename in backUpFilestoTransfer:
                path = os.path.join(self.backupFolder, eachfilename)
                filedictionary={
                    "filename": eachfilename,
                    "filepath": path,
                    "uploadedSuccess": 0
                }
                self.fileTobeUploaded.append(filedictionary)

        s3Log.info("{} files are to be uploaded. ".format(len(self.fileTobeUploaded) ))
        pprint.pprint(self.fileTobeUploaded)

    def CreateS3Bucket(self):
        """
            A bucket with name from command line arguments is created in eu-west-region. if a bucket already
            exists with that name, that same bucket will be used to transfer the backup files from backup folder.
        """
        bucketFound = False
        region = "eu-west-1"
        try:  # Check if bucket exists
            client.head_bucket(Bucket=self.bucketName)
            bucketFound = True
            s3Log.info ("Bucket \'{}\' Exists! ".format(self.bucketName))
        except ClientError as e:  # Bucket Does not exist
            if e.response["Error"]["Message"] == "Not Found":
                s3Log.info("Bucket \'{}\' does not exist!".format(self.bucketName))

        if bucketFound == 0: #since bucket does not exist, we ought to create it
            s3Log.info("Creating Bucket \'{}\' in region={}".format(self.bucketName, region))
            try:
                bucket_response = client.create_bucket(Bucket=self.bucketName,
                                                   CreateBucketConfiguration={
                                                       'LocationConstraint': region})
                bucketFound = True
            except ClientError as e:
                s3Log.error("FATAL ERROR: Unable to create bucket \'{}\'  {}".format(self.bucketName, e))
                sys.exit(1)


        return bucketFound

    def CalculateMd5OfEachFile(self, filedic):
        """
            This function is used to calculate Md5 using single part upload and is matched with the one calcualted by aws from s3 files.
        """
        #for eachfiledic in self.fileTobeUploaded:
        fileobj = open(filedic["filepath"], 'rb')
        buf = fileobj.read()
        hash = hashlib.md5()
        hash.update(buf)

        digest = hashlib.md5(buf).digest()
        md5enc = base64.b64encode(digest)
        md5tostr = md5enc.decode('utf-8')
        filedic["md5"] = md5tostr
        fileobj.close()

        #pprint.pprint(self.fileTobeUploaded)

    def saveStateOfThisRun(self):
        """
            This function will save the state of each run in a json format. This
            File will have information of upload status for each file.
        """
        with open('stateFile.json', 'w') as statefile:
            json.dump(self.fileTobeUploaded, statefile, indent=4)

    def multiPartUpload(self, eachfiledic ):
        """
            Multi-part upload is used in case of file size is greater than 1GB as it
            offer concurrency with threads; thus, saving our time.
        """
        config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                        multipart_chunksize=1024*25, use_threads=True)
        toReturn = False
        try:
            client.upload_file(eachfiledic["filepath"], self.bucketName, eachfiledic["filename"] ,
                Config = config
            )
            s3Log.info ("{} got uploaded on s3 bucket = {}\n".format(eachfiledic["filepath"], self.bucketName))
            toReturn = True
        except (ClientError, boto3.exceptions.S3UploadFailedError) as e:
            s3Log.error ("FAILED TO UPLOAD file:{}\n".format(eachfiledic["filename"]) )
            if "Error" in e.response:
                s3Log.error(e.response["Error"])

        return toReturn


    def singlePartUpload(self, eachfiledic):
        """
            single part upload is used when file size is less than 1GB.
        """
        self.CalculateMd5OfEachFile(eachfiledic)
        fileobj = open(eachfiledic["filepath"], 'rb')
        toReturn = False
        try:
            response = client.put_object(Body=fileobj, Bucket=self.bucketName,
                    Key=eachfiledic["filename"], ContentMD5=eachfiledic["md5"])
            s3Log.info ("{} got uploaded on s3 bucket = {}\n".format(eachfiledic["filepath"], self.bucketName))
            toReturn = True
        except (ClientError, boto3.exceptions.S3UploadFailedError) as e:
            s3Log.error ("FAILED TO UPLOAD file:{}\n".format(eachfiledic["filename"]) )
            if "Error" in e.response:
                s3Log.error(e.response["Error"])

        fileobj.close()
        return toReturn


    def uploadFilestoS3(self):
        """
            Main function to upload files to S3 using single or multipart
            and keep track of status of each file. In the end it stores
            the state of each file upload in stateFile.
        """
        allfilesuploadedcount = 0
        for eachfiledic in self.fileTobeUploaded:
            if eachfiledic["uploadedSuccess"] == 0:     #Means this file never got uploaded.
                if os.path.getsize(eachfiledic["filepath"]) < 1000000000: #<1GB
                    s3Log.info ("FileSize < 1GB for :{}, so using single part upload.".format(eachfiledic["filepath"]) )
                    if self.singlePartUpload(eachfiledic) == True:
                        eachfiledic["uploadedSuccess"] = 1
                        allfilesuploadedcount = allfilesuploadedcount + 1
                else:
                    s3Log.info ("FileSize > 1GB for :{}, so using Multi Part upload. \n".format(eachfiledic["filepath"]) )
                    if self.multiPartUpload(eachfiledic) == True:
                        eachfiledic["uploadedSuccess"] = 1
                        allfilesuploadedcount = allfilesuploadedcount + 1


            elif eachfiledic["uploadedSuccess"] == 1: #Means it got uploaded in the last run.
                allfilesuploadedcount = allfilesuploadedcount + 1

        self.saveStateOfThisRun()
        if len(self.fileTobeUploaded) == allfilesuploadedcount: #Means we uploaded all files in the queue
            return True
        else:
            return False


if __name__ == "__main__":
    args = ArgumentsParser()
    bucketName, backupFolderPath, crashrun = args.ReadArgs()
    s3transobj = S3Transfer(backupFolderPath, bucketName)
    if crashrun!=0:
        # Means we won't be sending all the files from Backup Folder Path; instead, we will read
        # the stateFile and push only files that were left last time.
        s3transobj.ParseStateFile()
    elif crashrun==0:
        #default is 0, means script is being run for the first time and we should transfer all files.
        s3transobj.PopulateFilePaths()
    s3transobj.CreateS3Bucket()

    if s3transobj.uploadFilestoS3() == False:
        s3Log.error ("Some Files might not have been uploaded. Check your stateFile. you might need to re-run with flag crashrun")
    s3transobj.saveStateOfThisRun()
    s3Log.info ("A stateFile has been generated in the current directory!")
