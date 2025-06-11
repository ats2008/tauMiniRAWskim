#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
echo "=====" >> cronlog
echo "Beging the job ! MUON-0 " 
echo "Beging the job ! MUON-0" >> cronlog
date >> cronlog
date
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py -c metadata/muon0_config.json  --skip_db_update 
python3 python/getNewFiles.py -c metadata/muon0_config.json --exec --doCondor --doCondorSubmission -q > bkp/MU0.`date +%F-%Hh-%Mm.sh`
echo "Run script made " >> cronlog
python3 python/outputSummary.py  -c metadata/muon0_config.json   
#./run.sh
condor_q
echo "All done! MUON-0"
