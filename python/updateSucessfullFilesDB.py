import os, json
import argparse
import datetime,uuid 
import util as utl

now = datetime.datetime.now() # current date and time
TIMESTAMP=now.strftime("%m/%B/%Y, %H:%M:%S")

parser = argparse.ArgumentParser()
parser.add_argument('-i',"--DBFileName", help="Database File",default='MissingFilesDB.mu0.json')
parser.add_argument('-f',"--FileDBName", help="File Database File name",default='fileDB.mu0.json')
parser.add_argument('-d',"--dataset", help="dataset",default='mu0')
parser.add_argument('-k',"--key", help="Missing file key",default='x')
parser.add_argument('-r',"--reason", help="Reason for the file miss",default='Unknown')
args = parser.parse_args()

fileDBFileName=args.FileDBName
dataset=args.dataset
print(f"Opening file-database {fileDBFileName} ")
try:
    with open(fileDBFileName) as f:
        fileDataStore=json.load(f)
except:
    print("Json format issue / file empty")
    fileDataStore={}
fileDB={}
if 'fileDB' in fileDataStore:
    fileDB=fileDataStore['fileDB']
metaData={}
if 'METADATA' in fileDataStore:
    metaData=fileDataStore['METADATA']

if 'timestamp' in metaData:
    print("   > The fileDB was last re-processed on ",metaData['timestamp'])


DB_FNAME=args.DBFileName
dset=args.dataset
fky=args.key
reason=args.reason

runs=None
lumis=None
if fky in fileDB[dataset]:
    print(" populating run lumi from db !")
    runs=fileDB[dataset][fky]['runs']
    lumis=fileDB[dataset][fky]['lumis']
    print("   > ",runs,lumis)
else:
    print("Missing run/lumi info for ",fky)

utl.updateSucessfullFilesDB(DB_FNAME,fky,reason,runs=runs,lumis=lumis,timestamp=TIMESTAMP)

