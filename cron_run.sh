#!/bin/bash
echo "=====" >> cronlog
echo "Beging the job ! " 
echo "Beging the job ! " >> cronlog
date >> cronlog
export HOME=/afs/cern.ch/user/a/athachay
export X509_USER_PROXY=/afs/cern.ch/user/a/athachay/private/.proxy/x509up_u134523
python3 python/getNewFiles.py  -c metadata/muon0_config.json --exec
echo "Run script made " >> cronlog
./run.sh
echo "All done!"
