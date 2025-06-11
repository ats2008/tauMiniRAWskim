#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh

echo "=====" >> cronlog_mu1
echo "Beging the job for muon 1! " 
echo "Beging the job for muon 1! " >> cronlog_mu1
date >> cronlog
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py  -c metadata/muon1_config.json --skip_db_update 
python3 python/getNewFiles.py  -c metadata/muon1_config.json --exec --doCondor --doCondorSubmission -q > bkp/MU1.`date +%F-%Hh-%Mm.sh`
echo "Run script made " >> cronlog_mu1
python3 python/outputSummary.py  -c metadata/muon1_config.json   
condor_q
echo "All done! muon 1"
