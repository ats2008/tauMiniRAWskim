#!/bin/bash
source /cvmfs/cms.cern.ch/cmsset_default.sh
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
CONTACT_EMAIL="athachay@cern.ch  aravindsugunan@gmail.com"

echo "=====" >> cronlog
echo "Beging the job ! MUON-0 " 
echo "Beging the job ! MUON-0" >> cronlog
date >> cronlog
date

dasgoclient -query="dataset=/Muon0/Run2025*-PromptReco-v*/MINIAOD"  > mu0.ds
if diff mu0.ds metadata/mu0.ds.bak ; then
   echo "No new dataset appeared in Muon0"
else
   echo "New dataset arrived  for Muon0!"
   date > _msg
   echo "Please update the configs !! " >> _msg
   echo "New dataset list : " >> _msg
   cat mu0.ds >> _msg
   echo "Old dataset list : " >> _msg
   cat metadata/mu0.ds.bak >> _msg
   echo " Diff  > " >> _msg
   diff mu0.ds metadata/mu0.ds.bak >> _msg
   cat _msg | mail -s "NEW MUON0 DATASET ! `date` " $CONTACT_EMAIL
fi

python3 python/skimNewFiles.py -c metadata/muon0_config.json  --skip_db_update 
python3 python/skimNewFiles.py -c metadata/muon0_config.json --exec --doCondor --doCondorSubmission -q &> bkp/MU0.`date +%F-%Hh-%Mm.sh`
echo "Run script made " >> cronlog
python3 python/outputSummary.py  -c metadata/muon0_config.json   
#./run.sh
condor_q
echo "All done! MUON-0"
