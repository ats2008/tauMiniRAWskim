import json
import argparse,os
import datetime,uuid 
import util as utl
import argparse
now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%m/%B/%Y, %H:%M:%S")
pwd=os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--configFile", help="Config File",default='muon0_config.json')
parser.add_argument('-e',"--execute_parent_eval", help="Execute the skimming",action='store_true')
parser.add_argument(     "--skip_db_update", help="Skip updaing the database with new files",action='store_true')
parser.add_argument( "-q" , "--quiet", help="Quite mode",action='store_true')
args = parser.parse_args()

#flistFileIn='runlumiFile.json'
#filedb_filename='fileDB.mu0.json'
#fail_filedb_filename='MissingFilesDB.mu0.json'
#sucessfull_filedb_filename='SucessfullFilesDB.mu0.json'
#dataset='Muon0'
with open(args.configFile) as f:
    run_config=json.load(f)
    dataset=run_config['dataset']
    filedb_filename=run_config['filelist_db']
    fail_filedb_filename=run_config['failedFile_db']
    sucessfull_filedb_filename=run_config['sucessFile_db']

destination='/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/athachay/phase1/taus/skims/v0/'
newFileStore={}

cmd_prts_tpl='dasgoclient -query="file run lumi dataset=@@DSET" --json > _allfiles.json'
cmd=cmd_prts_tpl.replace("@@DSET",dataset)
print(f"Reading latest filelist from {dataset}")
print(cmd)
os.system(cmd)
flistFileIn='_allfiles.json'
print(f"Opening file with run list {flistFileIn} ")
with open(flistFileIn) as f:
    allfiles=json.load(f)
    for item in allfiles:
        file_lfs=item['file'][0]['name']
        filename=file_lfs.split("/")[-1].replace(".root","")
        newFileStore[filename]={}
        newFileStore[filename]['lfs']=file_lfs
        newFileStore[filename]['runs']=[it['run_number'] for it in item['run'] ]
        newFileStore[filename]['lumis']=[it['number'] for it in item['lumi'] ]
    print("Number files got : ",len(newFileStore))

print(f"Opening file-database {filedb_filename} ")
if os.path.exists(filedb_filename):    
    with open(filedb_filename) as f:
        fileDataStore=json.load(f)
else:
    print("No such file ! starting fresh")
    fileDataStore={}
fileDB={}
if 'fileDB' in fileDataStore:
    fileDB=fileDataStore['fileDB']
metaData={}
if 'METADATA' in fileDataStore:
    metaData=fileDataStore['METADATA']

if 'timestamp' in metaData:
    print("   > The fileDB was last re-processed on ",metaData['timestamp'])

filesToProces={}
if dataset not in fileDB:
    fileDB[dataset]={}

for fky in newFileStore:
    if fky not in fileDB[dataset]:
        filesToProces[fky]=newFileStore[fky]
        fileDB[dataset][fky]=newFileStore[fky]
if len(filesToProces) < 1 :
    print("No New files to process ! exiting ")
    exit(0)

print(f"{len(filesToProces)}  MiniAOD Files to process.\n\t\t"+
       "\n\t\t".join([str(k) for  i,k  in enumerate(filesToProces) if i <10  ]))

if not args.skip_db_update:
    cmd=f'cp {filedb_filename} bkp/{unique_tag}.{filedb_filename.split("/")[-1]}'
    cmd=f'zip bkp/{unique_tag}.{filedb_filename.split("/")[-1]}.zip {filedb_filename}'
    print("Backing up fileDB : ",cmd)
    os.system(cmd)
    with open(filedb_filename,'w') as f:
        metaData['timestamp']=timestamp
        metaData['prev_tag']=unique_tag
        fileDataStore['METADATA']=metaData
        fileDataStore['fileDB']=fileDB
        json.dump(fileDataStore,f,indent=4)


if args.execute_parent_eval:
    fileMapToProcess=utl.getAllAvalableMiniRawMaps(filesToProces,fileDB[dataset])
    for fky in filesToProces:
        if (fky not in fileMapToProcess ) :
            print(" Registering ",fky," as missing dataset ")
            reason='Parent missing from disk'
            utl.updateMissingFilesDB(fail_filedb_filename,fky,reason,timestamp=timestamp)
    
    commandBlock=""
    cmd_cmsRun_tpl=str(utl.CMD_CMSRUN_TPL)
    for fky in fileMapToProcess:
        miniAODfile=fileDB[dataset][fky]['lfs']
        cmd ="\n\n"
        cmd+=f"echo Processing {miniAODfile}\n"
        cmd+=f"edmFileUtil root://cms-xrd-global.cern.ch//{miniAODfile}\n"
        cmd+="\n\n"
        parentFiles=','.join( fileDB[dataset][fky]['parents']  )
        cmd+=cmd_cmsRun_tpl.replace('@@INPUTFILE',miniAODfile)
        ofname=f"{fky}_{unique_tag}.root"
        cmd=cmd.replace('@@OUTPUTFILE',ofname)
        cmd=cmd.replace('@@SECONDARYFILES',parentFiles)
        dest=destination+'/'+'/'.join(miniAODfile.replace('/store/data/','').split("/")[:-1])
        cmd_mkd=f'mkdir -p {dest}'
        os.system(cmd_mkd)
        cmd=cmd.replace('@@DESTINATION',dest)
        cmd=cmd.replace('@@PWD',pwd)
        cmd=cmd.replace('@@KEY',fky)
        cmd=cmd.replace('@@DSET',dataset)
        cmd=cmd.replace('@@TIMESTAMP',timestamp)
        cmd=cmd.replace('@@FILEDBFNAME',filedb_filename)
        cmd=cmd.replace('@@SUCESSDBFILENAME',sucessfull_filedb_filename)
        cmd=cmd.replace('@@FAILDBFILENAME',fail_filedb_filename)
        commandBlock+=cmd
    with open('run.sh','w') as f:
        cmd=utl.TEMPLATE_RUN_SCRIPT
        cmd=cmd.replace("@@CODEBLOCK",commandBlock)
        f.write(cmd)
    os.system('chmod +x run.sh')
    cmd=f'cp run.sh bkp/runSripts/run_{unique_tag}.sh'
    print(cmd)
    os.system(cmd)

