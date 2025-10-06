TEMPLATE_CMS_CFG="""
import FWCore.ParameterSet.Config as cms

from input_cfg import process


process.maxEvents.input = cms.untracked.int32(@@NEVENTS)
process.source.fileNames = cms.untracked.vstring(@@FLIST)
process.TFileService.fileName = cms.string('file:@@TEMPL_OUTFILE')
if hasattr(process.source,'secondaryFileNames'):
    process.source.secondaryFileNames.clear()

#Setup FWK for multithreaded
#process.options.numberOfThreads=cms.untracked.uint32(4)
process.options.numberOfStreams=cms.untracked.uint32(0)
process.options.numberOfConcurrentLuminosityBlocks=cms.untracked.uint32(1)
process.options.wantSummary = cms.untracked.bool(True)
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
export HOME=@@HOME
export X509_USER_PROXY=@@X509_USER_PROXY
cd @@CMSSW_DIR 
set +x
eval `scramv1 runtime -sh`
#set -x
cd $SCRATCH_DIR
pwd
#cp @@INPUT_CFG .
#cp @@CMSRUN_CFG .
cmsRun @@CMSRUN_CFG 
ECOD=$?
if [ $ECOD -eq 0 ]; then
    mv @@OUTPUTFILE @@DESTINATION
    if [ $? -eq 0 ] ; then
        mv @@RUNSCRIPTNAME @@RUNSCRIPTNAME.sucess
    else
        echo Failed at copy
    fi
else    
    echo Failed at cmsrun
fi

echo JOB exiting at `date`
"""
   
   

TEMPLATE_CONDOR_SUB="""executable = $(filename)
output = @@CLOGBASE/$Fn(filename).$(Cluster).stdout
error = @@CLOGBASE/$Fn(filename).$(Cluster).stderr
log = @@CLOGBASE/$Fn(filename).$(Cluster).log
request_cpus = 2
+JobFlavour = "longlunch"
queue filename matching (@@CDIR/*.sh)
"""
   
