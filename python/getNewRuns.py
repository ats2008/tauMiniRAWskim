import json
import argparse,os
import datetime,uuid 

now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%d/%B/%Y, %H:%M:%S")

runFileIn='allruns.mu0'
runDBFileName='runDB.mu0.json'
dataset='Muon0'

print(f"Opening file with run list {runFileIn} ")
with open(runFileIn) as f:
    allRuns=[int(l[:-1]) for l in f.readlines()]

print(f"Opening run-database {runDBFileName} ")
try:
    with open(runDBFileName) as f:
        runDataStore=json.load(f)
except:
    print("Json format issue / file empty")
    runDataStore={}

runDB={}
if 'runDB' in runDataStore:
    runDB=runDataStore['runDB']
metaData={}
if 'METADATA' in runDataStore:
    metaData=runDataStore['METADATA']

if 'timestamp' in metaData:
    print("   > The runDB was last re-processed on ",metaData['timestamp'])

runsToProces=[]
if dataset not in runDB:
    runDB[dataset]=[]

#for i in range(min(5,len(runDB[dataset]))):
#    runsToProces.append(runDB[dataset][-1-i])

for r in allRuns:
    if r not in runDB[dataset]:
        runsToProces.append(r)
        runDB[dataset].append(r)
if len(runsToProces) < 1 :
    print("No New Runs to process ! exiting ")
    exit(0)

cmd=f'cp {runDBFileName} bkp/{unique_tag}.{runDBFileName}'
print("Backing up runDB : ",cmd)
os.system(cmd)
if False:
    with open(runDBFileName,'w') as f:
        metaData['timestamp']=timestamp
        metaData['prev_tag']=unique_tag
        runDataStore['METADATA']=metaData
        runDataStore['runDB']=runDB
        json.dump(runDataStore,f,indent=4)

print("  Runs to process : ",",".join([str(i) for  i  in runsToProces]))
