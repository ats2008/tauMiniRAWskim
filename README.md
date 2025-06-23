# tauMiniRAWskim
crontab based skimmer for storing MiniAOD+RAW collection. To be used for re-emulating L1 taus with offline tag-and-probe selections for taus

## Setup

### Proxy for accessing files and for querrying `das`
Setting up permenant location of proxy you can use in lxplus
    ```
    mkdir $HOME/.proxy
    export X509_USER_PROXY=$HOME/.proxy/<FNAME>
    ```

to get the <FNAME> create a proxy using the `voms` commad
    ```
    voms-proxy-init --voms cms
    ```

This will printout the filename of the created proxy details , you can get the filename for the line starting from `Created proxy in ....` , it will be something like `x509up_u$UID` , where `UID` is the user ID of your account. Once you know this filename , export :

```
export X509_USER_PROXY=$HOME/.proxy/<FNAME>
# ALSO ADD THIS export to the .bashrc
```
Create a proxy for 7 days
```
voms-proxy-init --voms cms --valid 196:00
```
Make sure that the made proxy in the `$HOME/.proxy/`.To check the vaidity of the proxy one can
```
voms-proxy-info
```
### Setting up the tag-and-probe package
* setup https://github.com/ramellar/TagAndProbeIntegrated

### Job configuration for skimmer jobs
* The driver scripts that will initiate the skim are `cron_run_mu0.sh` and `cron_run_mu1.sh`. Setup the paths here appropriately
    *  Also provide email-ids to which you can get notifications when a new dataset arrives in DAS
* Configure the datset and other parameters in `metadata/muon0_config.json` and `metadata/muon1_config.json`
    * `CONDOR_LOG_BASE` has to be an CERN AFS area for the condor to accept jobs
    * set `cmssw_base` approiately from the `Setting up the tag-and-probe package` section.
    * `run_cfg` should be the `tau_tagAndProbeRun3_skimmer.py` file, copy of which is also availabel as `metadata/tau_tagAndProbeRun3_skimmer.py`, but please do use the latest one availble from the l1-tau group !

### Job configuration for ntuplization jobs
* set `cmssw_base` approiately from the `Setting up the tag-and-probe package` section.
* `run_cfg` can be set to any unpaked/re-emulation cmsrun configuration file.
* set `fileList` to point to the list of files to be processed.
    * One can use `python/printRootFiles.py` to get filelist from a folder (recursively searching for all the root files). See next sections for a sample command one can use
* Files will be produced and stored in `destination`

## Crontab config
* Some info can be found here https://acrondocs.web.cern.ch/ about the generic lxplus-acron servise
* To edit acrontab in lxplus , use `acrontab -e` and add these lines ( after updating the paths obviously !)
```
0 */3 * * * lxplus cd /afs/cern.ch/work/a/athachay/private/l1egamma/automation && sh cron_run_mu0.sh
45 */3 * * * lxplus cd /afs/cern.ch/work/a/athachay/private/l1egamma/automation && sh cron_run_mu1.sh
```


## Helper scripts and handy commands
* Print the status of the dataset and skims without updating the database files used for keeping track of the skim
```
python3 python/getNewFiles.py -c metadata/muon1_config.json  --skip_db_update
```
* Print the status of the skimmed datasets
```
python3 python/outputSummary.py  -c metadata/muon0_config.json
```
* Produce the list of files missing from the skim, and also print the overall status of the skim
```
python3 python/updateSummaryFiles.py  -c metadata/muon1_config.json
## to export the missing files
python3 python/updateSummaryFiles.py  -c metadata/muon1_config.json
```

* Print the root files under a directory ( after a recusive scan for all the folders under the passed `path`). This file list could be used to run ntuplization jobs
```
python3 python/printRootFiles.py -b /eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/athachay/phase1/taus/skims/v0/MUON0,/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/athachay/phase1/taus/skims/v0/MUON1 > mu.fls
```

## SUB-Modules
```
# Will make the script with template cmsRun commands
python3 python/skimNewFiles.py  -c metadata/muon0_config.json
```

More options to the skimmer config
```
[athachay@lxplus955 automation]$ python3 python/skimNewFiles.py -h
usage: skimNewFiles.py [-h] [-c CONFIGFILE] [-n NMINIAODMAX] [-e] [--skip_db_update] [--doCondor] [--doCondorSubmission] [--force_filelist FORCE_FILELIST] [--skip_parents_on_disk_check] [-q]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIGFILE, --configFile CONFIGFILE
                        Config File
  -n NMINIAODMAX, --nMiniAODMax NMINIAODMAX
                        Number miniaods to process
  -e, --execute_parent_eval
                        Execute the skimming
  --skip_db_update      Skip updaing the database with new files
  --doCondor            Make condor jobs
  --doCondorSubmission  submit condor jobs
  --force_filelist FORCE_FILELIST
                        Force a filelist into the DB
  --skip_parents_on_disk_check
                        Force job creation without checking if parents are there on disk
  -q, --quiet           Quite mode
```


## MISC

```
python3 python/updateSucessfullFilesDB.py --DBFileName metadata/sucessFileDB.MUON0.json --FileDBName metadata/fileDB.MUON0.json --key 52c00283-256f-4a57-afff-ba3ea4e02f7b --dataset /Muon0/Run2025C-PromptReco-v1/MINIAOD --reason 'sucessfull skimming at jobs from 06/June/2025, 19:34:15!'
```
## Note

* **one can ignore the informations/numbers from `sucessDB` and `failDB` files**

