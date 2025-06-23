# tauMiniRAWskim
crontab based skimmer for storing MiniAOD+RAW collection. To be used for re-emulating L1 taus with offline tag-and-probe selections for taus

### SUB-Modules
```
# Will make the script with template cmsRun commands
python3 python/getNewFiles.py  -c metadata/muon0_config.json
```

## Crontab config
To edit in lxplus , use `acrontab -e`
```
0 */3 * * * lxplus cd /afs/cern.ch/work/a/athachay/private/l1egamma/automation && sh cron_run_mu0.sh
45 */3 * * * lxplus cd /afs/cern.ch/work/a/athachay/private/l1egamma/automation && sh cron_run_mu1.sh
```


## MISC

```
python3 python/updateSucessfullFilesDB.py --DBFileName metadata/sucessFileDB.MUON0.json --FileDBName metadata/fileDB.MUON0.json --key 52c00283-256f-4a57-afff-ba3ea4e02f7b --dataset /Muon0/Run2025C-PromptReco-v1/MINIAOD --reason 'sucessfull skimming at jobs from 06/June/2025, 19:34:15!'
```

