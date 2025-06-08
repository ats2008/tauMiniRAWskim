#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
echo "=====" >> cronlog
echo "Beging the job ! MUON-0 " 
echo "Beging the job ! MUON-0" >> cronlog
date >> cronlog
date
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py  -c metadata/muon0_config.json --exec --doCondor --doCondorSubmission -q
echo "Run script made " >> cronlog
#./run.sh
echo "All done! MUON-0"
