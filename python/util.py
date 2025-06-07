import os, json
import datetime,uuid

def getAllAvalableMiniRawMaps(filesToProces,fileDB):
    cmd_prts_tpl='dasgoclient -query="parent file=@@FILE " --json > _parents.json'
    cmd_siteQ_tpl='dasgoclient -query="site file=@@FILE " --json > _site.json'
    fileMaps={}
    allMissingFiles=[]
    for fky in filesToProces:
        fl=fileDB[fky]['lfs']
        if 'parents' not in fileDB[fky]:
            print(f"  > Getting parents for {fl}")
            cmd=cmd_prts_tpl.replace("@@FILE",fl)
            print(cmd)
            os.system(cmd)
            with open("_parents.json") as f:
                pdata=json.load(f)
            pfiles=[]
            pfiles_isOnDisk=[]
            for item in pdata:
                pfiles.append(item['parent'][0]['name'])
            fileDB[fky]['parents']=pfiles
    
        pfiles=fileDB[fky]['parents']
        allParentsOnDisk=True
        for pfl in pfiles:
            print(f"Checking site for parent {pfl}")
            cmd=cmd_siteQ_tpl.replace("@@FILE",pfl)
            #print(cmd)   
            os.system(cmd)
            isOnDisk=False
            with open("_site.json") as f:
                sdata=json.load(f)
                for i in range(len(sdata)):
                    for pfn in sdata[i]['site'][0]['pfns']:
                        if sdata[i]['site'][0]['pfns'][pfn]['type']=='DISK':
                            rse=sdata[i]['site'][0]['pfns'][pfn]['rse']
                            print(f"   > found file in : {rse}")
                            isOnDisk=True
                            break
                    if isOnDisk:
                        break
            if not isOnDisk:
                allParentsOnDisk=False
                break
        if allParentsOnDisk:
            print(f"All the parents for the MIiniAOD {fky} is on disk")
            fileMaps[fky]={}
            fileMaps[fky]['miniAOD']=fl
            fileMaps[fky]['parents']=pfiles
        else:
            allMissingFiles.append(fky)
            print(f"All parents not on disk ! will jave to skip this MiniAOD file ({fky})")

    return fileMaps

 
def updateMissingFilesDB(MISSINGFILES_DB_FNAME,fky,reason,timestamp=None):
    missingRecord={}
    missingRecord['time']=timestamp
    missingRecord['reason']=reason
    
    missingFilesDB={}
    metaData={}
    print(f"Opening missing-file-database {MISSINGFILES_DB_FNAME} ")
    try:
        with open(MISSINGFILES_DB_FNAME) as f:
            data=json.load(f)
    except:
        print("Json format issue / file empty")
        data={}
    missingFilesDB={}
    if 'missingFilesDB' in data:
        missingFilesDB=data['missingFilesDB']
    metaData={}
    if 'METADATA' in data:
        metaData=data['METADATA']
    if 'timestamp' in metaData:
        print("   > The fileDB was last re-processed on ",metaData['timestamp'])
    
    
    if fky in missingFilesDB:
        print(f"{fky} was already missing ! will update the entry")
    missingFilesDB[fky]=missingRecord
    now = datetime.datetime.now() # current date and time
    uq=uuid.uuid4().hex[:6].upper()
    unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
    cmd=f'cp {MISSINGFILES_DB_FNAME} bkp/{unique_tag}.{MISSINGFILES_DB_FNAME.split("/")[-1]}'
    cmd=f'zip bkp/{unique_tag}.{MISSINGFILES_DB_FNAME.split("/")[-1]}.zip {MISSINGFILES_DB_FNAME} '
    print("Backing up missing file DB : ",cmd)
    os.system(cmd)
    with open(MISSINGFILES_DB_FNAME,'w') as f:
        metaData['timestamp']=timestamp
        metaData['prev_tag']=unique_tag
        data['METADATA']=metaData
        data['missingFilesDB']=missingFilesDB
        json.dump(data,f,indent=4)

def updateSucessfullFilesDB(DB_FNAME,fky,reason,runs=None,lumis=None,timestamp=None):
    Record={}
    Record['time']=timestamp
    Record['reason']=reason
    if runs is not None:
        Record['runs']=runs
    if lumis is not None:
        Record['lumis']=lumis
    FilesDB={}
    metaData={}
    print(f"Opening sucessfull-file-database {DB_FNAME} ")
    try:
        with open(DB_FNAME) as f:
            data=json.load(f)
    except:
        print("Json format issue / file empty")
        data={}
    FilesDB={}
    if 'FilesDB' in data:
        FilesDB=data['FilesDB']
    metaData={}
    if 'METADATA' in data:
        metaData=data['METADATA']
    if 'timestamp' in metaData:
        print("   > The fileDB was last re-processed on ",metaData['timestamp'])
    
    
    if fky in FilesDB:
        print(f"{fky} was already done  ! will update the entry")
    FilesDB[fky]=Record
    now = datetime.datetime.now() # current date and time
    uq=uuid.uuid4().hex[:6].upper()
    unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
    cmd=f'cp {DB_FNAME} bkp/{unique_tag}.{DB_FNAME.split("/")[-1]}'
    cmd=f'zip bkp/{unique_tag}.{DB_FNAME.split("/")[-1]}.zip {DB_FNAME}'
    print("Backing up  file DB : ",cmd)
    os.system(cmd)
    with open(DB_FNAME,'w') as f:
        metaData['timestamp']=timestamp
        metaData['prev_tag']=unique_tag
        data['METADATA']=metaData
        data['FilesDB']=FilesDB
        json.dump(data,f,indent=4)

CMD_CMSRUN_TPL="""
cd $SCRATCH_DIR
cmsRun tau_tagAndProbeRun3_skimmer.py inputFiles=@@INPUTFILE secondaryInputFiles=@@SECONDARYFILES outputFile=@@OUTPUTFILE maxEvents=-1 
ECOD=$?
if [ $ECOD -eq 0 ]; then
    mv @@OUTPUTFILE @@DESTINATION
    if [ $? -eq 0 ] ; then
        cd @@PWD
        python3 python/updateSucessfullFilesDB.py --DBFileName @@SUCESSDBFILENAME --FileDBName @@FILEDBFNAME --key @@KEY --dataset @@DSET --reason "sucessfull skimming at jobs from @@TIMESTAMP!"
    else
        cd @@PWD
        python3 python/updateMissingFilesDB.py  --DBFileName @@FAILDBFILENAME --key @@KEY --reason "failure to copy file to destination"
    fi
else    
    cd @@PWD
    python3 python/updateMissingFilesDB.py  --DBFileName @@FAILDBFILENAME --key @@KEY --reason "cmsRun Failed with exit code $ECOD"
fi
"""

TEMPLATE_RUN_SCRIPT="""#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh 
set -x
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
cd /afs/cern.ch/work/a/athachay/private/l1egamma/2025/taus/CMSSW_15_0_6
set +x
eval `scramv1 runtime -sh`
set -x
SCRATCH_DIR=`mktemp -d`
cd $SCRATCH_DIR
cp /afs/cern.ch/work/a/athachay/private/l1egamma/2025/taus/CMSSW_15_0_6/TagAndProbeIntegrated/TagAndProbe/test/tau_tagAndProbeRun3_skimmer.py .
@@CODEBLOCK
echo JOB exiting at `date`
"""
   

   
