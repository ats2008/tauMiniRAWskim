#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh

echo "=====" >> cronlog_mu1
echo "Beging the job for muon 1! " 
echo "Beging the job for muon 1! " >> cronlog_mu1
date >> cronlog
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py  -c metadata/muon1_config.json --exec --doCondor --doCondorSubmission -q
echo "Run script made " >> cronlog_mu1
condor_q
echo "All done! muon 1"
