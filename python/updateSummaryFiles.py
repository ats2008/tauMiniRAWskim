import json
import argparse,os
import datetime,uuid 
import util as utl
import argparse

now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%d/%B/%Y, %H:%M:%S")
pwd=os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--configFile", help="Config File",default='muon0_config.json')
parser.add_argument('-b',"--base", help="Base folder to look at",default=None)
parser.add_argument('-e',"--exportMissingFiles", help="Export the missing filelist ",default=False,action='store_true')
args = parser.parse_args()


with open(args.configFile) as f:
    run_config=json.load(f)
    tag=run_config['tag']
    unique_tag=tag+"_"+unique_tag
    dataset=run_config['dataset']
    filedb_filename=run_config['filelist_db']
    fail_filedb_filename=run_config['failedFile_db']
    sucessfull_filedb_filename=run_config['sucessFile_db']
    base=run_config['destination']

filesDBName=filedb_filename
if args.base is not None:
    base=args.base

print(f"Files under {base} is being looked at !")

flist=[]
totalsize=0.0
for (root,dirs,files) in os.walk(base, topdown=True):
    fls=[root+'/'+i  for i in files if i.endswith('.root')]
    flist+=fls
    totalsize+=sum([os.path.getsize(fp) for fp in fls])
totalsize/=2**30
print("Got : ",len(flist)," Files ")
print("Total size : ",f"{totalsize:.2f} GB" )
print("estimated total number of events :  ",f"{int(totalsize*44/43)} k Events" )

duplicateFiles=[]
ftags={}
fo=open("rm_duplicate.sh","w")
for fl in flist:
    ky=fl.split("/")[-1].split("_")[0]
    if ky in ftags:
        duplicateFiles.append((ky,fl))
        print("Dumplicate  found ! ",ftags[ky].split("/")[-1],"\n\t",fl)
        fo.write("rm "+fl+"\n")
        continue
    ftags[ky]=fl
fo.close()
print(f"Number of duplicate files found : {len(duplicateFiles)}")
#for df in duplicateFiles:
#    print(ftag[df[0]].split("/")[-1],"\n\t > ",df[1].split("/")[-1])
print("opening file db : ",filesDBName)
with open(filesDBName) as f:
    data=json.load(f)

fileDB=data['fileDB']
print(f"Opened files db with { sum([len(fileDB[ds]) for ds in fileDB ]) } files")
zombieFiles=[]
dbFiles=[]
for fky in ftags:
    got=False
    for ds in fileDB:
        if fky in fileDB[ds]:
            got=True
    if not got:
        zombieFiles.append(fky)
    else:
        dbFiles.append(fky)        

if len(zombieFiles) > 0:
    print(f"Zombie root files (# {len(zombieFiles)}) found in {base}")

#for ky in zombieFiles:
#    print(ky,ftags[ky])

print(f"Total number of unique files        : {len(ftags):>4}")
print(f"Total number of files matched in DB : {len(dbFiles):>4}")

print("Opening sucess db file : ",sucessfull_filedb_filename)
with open(sucessfull_filedb_filename) as f:
    sucessFile_db=json.load(f)
    print("   Loaded sucess db with : ",len(sucessFile_db['FilesDB']))

print("Opening fail db file : ",fail_filedb_filename)
with open(fail_filedb_filename) as f:
    failFile_db=json.load(f)
    print("   Loaded fail db with : ",len(failFile_db['missingFilesDB']))
    
failedFiles=[]
for ds in fileDB:
    for fky in fileDB[ds]:
        if fky not in ftags:
            failedFiles.append(fileDB[ds][fky]['lfs'])


missing_files_from_sucessDB=[]
for fl in dbFiles:
    if fl not in sucessFile_db['FilesDB']:
        missing_files_from_sucessDB.append(fl)

sucessfull_files_in_missingDB=[]
for fl in dbFiles:
    if fl in failFile_db['missingFilesDB']:
        sucessfull_files_in_missingDB.append(fl)

print("Number of sucessfull files ",len(dbFiles))
print("Number of sucessfull files  not  updated in sucessDB : ",len(missing_files_from_sucessDB))
print("Number of files in missingFilesDB but is actually sucessfull : ",len(sucessfull_files_in_missingDB))
print("Number of failed files  in the full db : ",len(failedFiles))
if args.exportMissingFiles:
    ofname=f"missingFiles.fls"
    print("Exporting files to ",ofname)
    with open(ofname,'w') as f:
        f.write("\n".join(failedFiles))
        
exit()
