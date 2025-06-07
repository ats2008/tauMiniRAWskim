import os, json
import argparse
import datetime,uuid 
import util as utl

now = datetime.datetime.now() # current date and time
TIMESTAMP=now.strftime("%m/%B/%Y, %H:%M:%S")

parser = argparse.ArgumentParser()
parser.add_argument('-i',"--DBFileName", help="Database File",default='MissingFilesDB.mu0.json')
parser.add_argument('-k',"--key", help="Missing file key",default='x')
parser.add_argument('-r',"--reason", help="Reason for the file miss",default='Unknown')
args = parser.parse_args()

MISSINGFILES_DB_FNAME=args.DBFileName
fky=args.key
reason=args.reason

utl.updateMissingFilesDB(MISSINGFILES_DB_FNAME,fky,reason,timestamp=TIMESTAMP)

