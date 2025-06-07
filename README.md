# tauMiniRAWskim
crontab based skimmer for storing MiniAOD+RAW collection. To be used for re-emulating L1 taus with offline tag-and-probe selections for taus

### SUB-Modules
```
# Will make the script with template cmsRun commands
python3 python/getNewFiles.py  -c metadata/muon0_config.json
```
```
python3 python/updateSucessfullFilesDB.py --DBFileName metadata/sucessFileDB.MUON0.json --FileDBName metadata/fileDB.MUON0.json --key 52c00283-256f-4a57-afff-ba3ea4e02f7b --dataset /Muon0/Run2025C-PromptReco-v1/MINIAOD --reason 'sucessfull skimming at jobs from 06/June/2025, 19:34:15!'
```
