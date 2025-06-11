import json
import argparse,os
import datetime,uuid 
import util as utl
import argparse

now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%m/%B/%Y, %H:%M:%S")
pwd=os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--configFile", help="Config File",default='muon0_config.json')
parser.add_argument('-b',"--base", help="Base folder to look at",default=None)
args = parser.parse_args()


with open(args.configFile) as f:
    run_config=json.load(f)
    tag=run_config['tag']
    unique_tag=tag+"_"+unique_tag
    dataset=run_config['dataset']
    filedb_filename=run_config['filelist_db']
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

fileDB=data['fileDB'][dataset]
print(f"Opened files db with {len(fileDB)} files")
zombieFiles=[]
dbFiles=[]
for fky in ftags:
    if fky not in fileDB:
        zombieFiles.append(fky)
    else:
        dbFiles.append(fky)        
if len(zombieFiles) > 0:
    print(f"Zombie root files (# {len(zombieFiles)}) found in {base}")

#for ky in zombieFiles:
#    print(ky,ftags[ky])

print(f"Total number of unique files        : {len(ftags):>4}")
print(f"Total number of files matched in DB : {len(dbFiles):>4}")

runLumiDB={}
for fky in dbFiles:
    for run,lumis in zip(fileDB[fky]['runs'],fileDB[fky]['lumis']):
        if run not in runLumiDB:
            runLumiDB[run]=[]
        runLumiDB[run]+=lumis

runLumiDBToExport={}
for run in runLumiDB:
    runLumiDB[run].sort()
    lmList=[]
    if len(runLumiDB[run]) < 1:
        print("empty run ! ",run)
    a=runLumiDB[run][0]
    b=runLumiDB[run][0]
    for i in range(1,len(runLumiDB[run])):
        if runLumiDB[run][i]==(b+1):
            b=runLumiDB[run][i]
        else:
            lmList.append([a,b])
            a=runLumiDB[run][i]
            b=runLumiDB[run][i]
    lmList.append([a,b])
    runLumiDBToExport[f"{run}"]=lmList
ofname=f"exportedRunLumi_{tag}.json"
print("Exporting run-lumi to ",ofname)
with open(ofname,'w') as f:
    json.dump(runLumiDBToExport,f,indent=4)

brilCMD="""
source /cvmfs/cms-bril.cern.ch/cms-lumi-pog/brilws-docker/brilws-env
# please add normatag if needed  : --normtag /cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json
brilcalc lumi  -u /fb -i exportedRunLumi_@@TAG.json
"""
cmd=brilCMD.replace("@@TAG",tag)
print(cmd)

