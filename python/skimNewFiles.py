import json
import argparse,os
import datetime,uuid 
import util as utl
import argparse
now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%d/%B/%Y, %H:%M:%S")
pwd=os.getcwd()


HOME=os.environ.get('HOME')
X509_USER_PROXY=os.environ.get('X509_USER_PROXY')

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--configFile", help="Config File",default='muon0_config.json')
parser.add_argument('-n',"--nMiniAODMax", help="Number miniaods to process",default=1000000,type=int)
parser.add_argument('-e',"--execute_parent_eval", help="Execute the skimming",action='store_true')
parser.add_argument(     "--skip_db_update", help="Skip updaing the database with new files",action='store_true')
parser.add_argument(     "--doCondor", help="Make condor jobs",action='store_true')
parser.add_argument(     "--doCondorSubmission", help="submit condor jobs",action='store_true')
parser.add_argument(     "--force_filelist", help="Force a filelist into the DB",default=None)
parser.add_argument(     "--skip_parents_on_disk_check", help="Force job creation without checking if parents are there on disk",default=False,action='store_true')
parser.add_argument( "-q" , "--quiet", help="Quite mode",action='store_true')
args = parser.parse_args()

destination='/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/athachay/phase1/taus/skims/v0/'

with open(args.configFile) as f:
    run_config=json.load(f)
    tag=run_config['tag']
    unique_tag=tag+"_"+unique_tag
    dataset=run_config['dataset']
    destination=run_config['destination']
    filedb_filename=run_config['filelist_db']
    fail_filedb_filename=run_config['failedFile_db']
    sucessfull_filedb_filename=run_config['sucessFile_db']
    CONDOR_LOG_BASE=run_config['CONDOR_LOG_BASE']
    cmssw_base=run_config['cmssw_base']
    cmsrun_cfg=run_config['run_cfg']

newFileStore={}

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
if dataset not in fileDB:
    fileDB[dataset]={}

filesToProces={}
ntoDo=0
if args.force_filelist is None:
    cmd_prts_tpl=f'dasgoclient -query="file run lumi dataset=@@DSET" --json > _allfiles_{unique_tag}.json'
    cmd=cmd_prts_tpl.replace("@@DSET",dataset)
    print(f"Reading latest filelist from {dataset}")
    print(cmd)
    os.system(cmd)
    flistFileIn=f'_allfiles_{unique_tag}.json'
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

else:
    print("Opening file list from : ",args.force_filelist)
    with open(args.force_filelist) as f:
        txt=f.readlines()
        flist=[l[:-1] for l in txt]
    
    for file_lfs in flist:
        filename=file_lfs.split("/")[-1].replace(".root","")
        if filename in fileDB[dataset]:
            newFileStore[filename]=fileDB[dataset][filename]
        else : 
            newFileStore[filename]={}
            newFileStore[filename]['lfs']=file_lfs
            ### UPDATE runs and lumis after fetching it from das
            #newFileStore[filename]['runs']=[it['run_number'] for it in item['run'] ]
            #newFileStore[filename]['lumis']=[it['number'] for it in item['lumi'] ]
        
        filesToProces[filename]=newFileStore[filename]
        ntoDo+=1
        if ntoDo>=args.nMiniAODMax: break

print(f"Using filedb with {len(fileDB[dataset])} files")
ntoDo=0
for fi,fky in enumerate(newFileStore):
    if fky not in fileDB[dataset]:
        filesToProces[fky]=newFileStore[fky]
        fileDB[dataset][fky]=newFileStore[fky]
        ntoDo+=1
    if ntoDo>=args.nMiniAODMax: break


if len(filesToProces) < 1 :
    print("No New files to process ! exiting ")
    os.system("echo echo NO Files To Process  > run.sh ; chmod +x run.sh")
    os.system(f"rm *{unique_tag}*")
    exit(0)

print(f"{len(filesToProces)}  MiniAOD Files to process.\n\t\t"+
       "\n\t\t".join([str(k) for  i,k  in enumerate(filesToProces) if i <10  ]))

if args.execute_parent_eval:
    fileMapToProcess=utl.getAllAvalableMiniRawMaps(
                                filesToProces,fileDB[dataset],unique_tag=unique_tag,
                                quiet=args.quiet,skip_on_disk_check=args.skip_parents_on_disk_check
                           )
    for fky in filesToProces:
        if (fky not in fileMapToProcess ) :
            print(" Registering ",fky," as missing dataset ")
            reason='Parent missing from disk'
            #utl.updateMissingFilesDB(fail_filedb_filename,fky,reason,timestamp=timestamp)
    
    commandBlock=""
    cmd_cmsRun_tpl=str(utl.CMD_CMSRUN_TPL)
    if args.doCondor:
        cdir=pwd+f"/Condor/{unique_tag}/"
        cmd=f"mkdir -p {cdir}"
        os.system(cmd)
    print()
    for fi,fky in enumerate(fileMapToProcess):
        print(f"\r   Adding job for [{fi:>3} / {len(fileMapToProcess)}]   ",end="")
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
        cmd=cmd.replace('@@CMSRUN_CFG',cmsrun_cfg)
        commandBlock+=cmd
        if args.doCondor:
            ofname=f"{cdir}/run_{fi}.sh"
            with open(ofname,'w') as f:
                cmd=utl.TEMPLATE_CRUN_SCRIPT
                cmd=cmd.replace("@@HOME",HOME)
                cmd=cmd.replace("@@X509_USER_PROXY",X509_USER_PROXY)
                cmd=cmd.replace("@@CMSSW_DIR",cmssw_base)
                cmd=cmd.replace("@@CODEBLOCK",commandBlock)
                cmd=cmd.replace("@@RUNSCRIPTNAME",ofname)
                f.write(cmd)
            os.system(f'chmod +x {ofname}')
            commandBlock=""
    print()
    if args.doCondor:
        clogbaseDir=f"{CONDOR_LOG_BASE}/Condor/{unique_tag}/"
        cmd=f"mkdir -p {clogbaseDir}"
        os.system(cmd)
        sub=utl.TEMPLATE_CONDOR_SUB.replace("@@PWD",pwd)
        sub=sub.replace('@@CDIR',cdir)
        sub=sub.replace('@@CLOGBASE',clogbaseDir)
        condor_sub_filename=f"{cdir}/condor_submit.jdl"
        with open(condor_sub_filename,'w') as f:
            f.write( sub )
        print(f"Condor jobs made at {cdir}")
        if args.doCondorSubmission:
            cmd=f'condor_submit {condor_sub_filename}'
            print(cmd)
            os.system(cmd)

        # submit script
    else:
        with open('run.sh','w') as f:
            cmd=utl.TEMPLATE_RUN_SCRIPT
            cmd=cmd.replace("@@CODEBLOCK",commandBlock)
            f.write(cmd)
        os.system('chmod +x run.sh')
        cmd=f'cp run.sh bkp/runSripts/run_{unique_tag}.sh'
        print(cmd)
        os.system(cmd)

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

os.system(f"rm *{unique_tag}*")
