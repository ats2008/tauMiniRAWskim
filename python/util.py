import os, json
import datetime,uuid

def getAllAvalableMiniRawMaps(filesToProces,fileDB,unique_tag="",quiet=False,skip_on_disk_check=False):
    cmd_prts_tpl=f'dasgoclient -query="parent file=@@FILE " --json > _parents_{unique_tag}.json'
    cmd_siteQ_tpl=f'dasgoclient -query="site file=@@FILE " --json > _site_{unique_tag}.json'
    fileMaps={}
    allMissingFiles=[]
    nFound=0
    for fi,fky in enumerate(filesToProces):
        #if fi>4: break
        fl=fileDB[fky]['lfs']
        if 'parents' not in fileDB[fky]:
            print(f"[nProcessed : {fi+1:>3} / {len(filesToProces)} ] [ nFound  : {nFound} / {len(filesToProces)} ]  > Getting parents for {fl}")
            cmd=cmd_prts_tpl.replace("@@FILE",fl)
            print(cmd)
            os.system(cmd)
            with open(f"_parents_{unique_tag}.json") as f:
                pdata=json.load(f)
            pfiles=[]
            pfiles_isOnDisk=[]
            for item in pdata:
                pfiles.append(item['parent'][0]['name'])
            fileDB[fky]['parents']=pfiles
    
        pfiles=fileDB[fky]['parents']
        allParentsOnDisk=True
        for pfl in pfiles:
            if skip_on_disk_check:
                break
            #if not quiet: print(f"Checking site for parent {pfl}")
            cmd=cmd_siteQ_tpl.replace("@@FILE",pfl)
            #print(cmd)   
            os.system(cmd)
            isOnDisk=False
            with open(f"_site_{unique_tag}.json") as f:
                sdata=json.load(f)
                for i in range(len(sdata)):
                    for pfn in sdata[i]['site'][0]['pfns']:
                        if sdata[i]['site'][0]['pfns'][pfn]['type']=='DISK':
                            rse=sdata[i]['site'][0]['pfns'][pfn]['rse']
                            #if not quiet : print(f"   > found file in : {rse}")
                            isOnDisk=True
                            break
                    if isOnDisk:
                        break
            if not isOnDisk:
                allParentsOnDisk=False
                break
        if allParentsOnDisk:
            if not quiet:
                print(f"[nProcessed : {fi+1:>3} / {len(filesToProces)} ] [ nFound  : {nFound} / {len(filesToProces)} ]  | All the parents for the MIiniAOD {fky} is on disk")
            fileMaps[fky]={}
            fileMaps[fky]['miniAOD']=fl
            fileMaps[fky]['parents']=pfiles
            nFound+=1
        else:
            allMissingFiles.append(fky)
            if not quiet:
                print(f"[nProcessed : {fi+1:>3} / {len(filesToProces)} ] [ nFound  : {nFound} / {len(filesToProces)} ] | All parents not on disk ! will jave to skip this MiniAOD file ({fky})")

    return fileMaps

 
def updateMissingFilesDB(MISSINGFILES_DB_FNAME,fky,reason,timestamp=None):
    if isinstance(fky,str):
        fkyList=[fky]
    else:
        fkyList=fky
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
    
    for fky in fkyList:
        missingRecord={}
        missingRecord['time']=timestamp
        missingRecord['reason']=reason
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
    metaData['timestamp']=timestamp
    metaData['prev_tag']=unique_tag
    data['METADATA']=metaData
    data['FilesDB']=FilesDB
    cmd=f'cp {DB_FNAME} bkp/{unique_tag}.{DB_FNAME.split("/")[-1]}'
    cmd=f'zip bkp/{unique_tag}.{DB_FNAME.split("/")[-1]}.zip {DB_FNAME}'
    print("Backing up  file DB : ",cmd)
    os.system(cmd)
    with open(DB_FNAME,'w') as f:
        json.dump(data,f,indent=4)

CMD_CMSRUN_TPL="""
cd $SCRATCH_DIR
echo cmsRun tau_tagAndProbeRun3_skimmer.py inputFiles=@@INPUTFILE secondaryInputFiles=@@SECONDARYFILES outputFile=@@OUTPUTFILE maxEvents=-1 
cmsRun tau_tagAndProbeRun3_skimmer.py inputFiles=@@INPUTFILE secondaryInputFiles=@@SECONDARYFILES outputFile=@@OUTPUTFILE maxEvents=-1 
ECOD=$?
if [ $ECOD -eq 0 ]; then
    mv @@OUTPUTFILE @@DESTINATION
    if [ $? -eq 0 ] ; then
        cd @@PWD
        echo python3 python/updateSucessfullFilesDB.py --DBFileName @@SUCESSDBFILENAME --FileDBName @@FILEDBFNAME --key @@KEY --dataset @@DSET --reason "sucessfull skimming at jobs from @@TIMESTAMP!"
        python3 python/updateSucessfullFilesDB.py --DBFileName @@SUCESSDBFILENAME --FileDBName @@FILEDBFNAME --key @@KEY --dataset @@DSET --reason "sucessfull skimming at jobs from @@TIMESTAMP!"
        mv @@RUNSCRIPTNAME @@RUNSCRIPTNAME.sucess
    else
        cd @@PWD
        echo python3 python/updateMissingFilesDB.py  --DBFileName @@FAILDBFILENAME --key @@KEY --reason "failure to copy file to destination"
        python3 python/updateMissingFilesDB.py  --DBFileName @@FAILDBFILENAME --key @@KEY --reason "failure to copy file to destination"
    fi
else    
    cd @@PWD
    echo python3 python/updateMissingFilesDB.py  --DBFileName @@FAILDBFILENAME --key @@KEY --reason "cmsRun Failed with exit code $ECOD"
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
SCRATCH_DIR=`mktemp -d`
cd $SCRATCH_DIR
cp /afs/cern.ch/work/a/athachay/private/l1egamma/2025/taus/CMSSW_15_0_6/TagAndProbeIntegrated/TagAndProbe/test/tau_tagAndProbeRun3_skimmer.py .
@@CODEBLOCK
echo JOB exiting at `date`
"""

TEMPLATE_CRUN_SCRIPT="""#!/bin/bash
if [[ -z "${_CONDOR_SCRATCH_DIR}" ]]; then
    _CONDOR_SCRATCH_DIR=`mktemp -d`
    echo _CONDOR_SCRATCH_DIR was not set, setting it to $_CONDOR_SCRATCH_DIR
else
    echo CONDOR_SCRATCH_DIR is $_CONDOR_SCRATCH_DIR
fi
cd $_CONDOR_SCRATCH_DIR
pwd
export SCRATCH_DIR=$_CONDOR_SCRATCH_DIR
source /cvmfs/cms.cern.ch/cmsset_default.sh 
set -x
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
cd /afs/cern.ch/work/a/athachay/private/l1egamma/2025/taus/CMSSW_15_0_6
set +x
eval `scramv1 runtime -sh`
#set -x
cd $SCRATCH_DIR
pwd
cp /afs/cern.ch/work/a/athachay/private/l1egamma/2025/taus/CMSSW_15_0_6/TagAndProbeIntegrated/TagAndProbe/test/tau_tagAndProbeRun3_skimmer.py .
@@CODEBLOCK
echo JOB exiting at `date`
"""
   
   

TEMPLATE_CONDOR_SUB="""executable = $(filename)
output = @@CLOGBASE/$Fn(filename).$(Cluster).stdout
error = @@CLOGBASE/$Fn(filename).$(Cluster).stderr
log = @@CLOGBASE/$Fn(filename).$(Cluster).log
request_cpus = 2
+JobFlavour = "microcentury"
queue filename matching (@@CDIR/*.sh)
"""
   
