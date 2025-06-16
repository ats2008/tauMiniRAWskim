#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh

echo "=====" >> cronlog_mu1
echo "Beging the job for muon 1! " 
echo "Beging the job for muon 1! " >> cronlog_mu1
date >> cronlog
dasgoclient -query="dataset=/Muon1/Run2025*-PromptReco-v*/MINIAOD"  > mu1.ds
if diff mu1.ds metadata/mu1.ds.bak ; then
   echo "No new dataset appeared in Muon1"
else
   echo "New dataset arrived  for Muon1!"
   date > _msg
   echo "Please update the configs !! " >> _msg
   echo "New dataset list : " >> _msg
   cat mu1.ds >> _msg
   echo "Old dataset list : " >> _msg
   cat metadata/mu1.ds.bak >> _msg
   echo " Diff  > " >> _msg
   diff mu1.ds metadata/mu1.ds.bak >> _msg
   cat _msg | mail -s "NEW MUON1 DATASET ! `date` " athachay@cern.ch  aravindsugunan@gmail.com
fi
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py  -c metadata/muon1_config.json --skip_db_update 
python3 python/getNewFiles.py  -c metadata/muon1_config.json --exec --doCondor --doCondorSubmission -q &> bkp/MU1.`date +%F-%Hh-%Mm.sh`
echo "Run script made " >> cronlog_mu1
python3 python/outputSummary.py  -c metadata/muon1_config.json   
condor_q
echo "All done! muon 1"
