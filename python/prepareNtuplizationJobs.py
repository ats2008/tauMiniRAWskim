import json
import argparse,os
import datetime,uuid 
import util as utl
import template as tpl
import argparse

now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%B%Y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%d/%B/%Y, %H:%M:%S")
pwd=os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--configFile", help="Config File",default='muon0_config.json')
parser.add_argument('-b',"--base", help="Base folder to look at",default=None)
parser.add_argument('-n',"--nJobMax", help="Number of jobs to do",default=-1, type = int)
parser.add_argument(     "--doCondorSubmission", help="do condor submission too",action='store_true')
args = parser.parse_args()


HOME=os.environ.get('HOME')
X509_USER_PROXY=os.environ.get('X509_USER_PROXY')


N_FILE_PER_JOB=50

with open(args.configFile) as f:
    run_config=json.load(f)
    tag='def'
    if 'tag' in run_config:
        tag=run_config['tag']
        unique_tag=tag+"_"+unique_tag
    N_FILE_PER_JOB=50     
    if 'nFilesPerJob' in run_config:
        N_FILE_PER_JOB=run_config['nFilesPerJob']
    dest=run_config['destination']+'/'+unique_tag
    base=None
    if 'base' in run_config:
        base=run_config['base']
    fileList=None
    if 'fileList' in run_config:
        fileList=run_config['fileList']
    cmssw_base=run_config['cmssw_base']
    cmsrun_cfg=run_config['run_cfg']
    CONDOR_LOG_BASE=run_config['CONDOR_LOG_BASE']
if args.base is not None:
    base=args.base

print(f"Files under {base} is being looked at !")

flist=[]
if base is not None:
    for (root,dirs,files) in os.walk(base, topdown=True):
        fls=[root+'/'+i  for i in files if i.endswith('.root')]
        flist+=fls
if fileList is not None:
    with open(fileList) as f:
        txt=f.readlines()
        flist=['file:'+l[:-1] for l in txt]

print("Got : ",len(flist)," Files ")

cdir=pwd+f"/Condor/{unique_tag}/"
cmd=f"mkdir -p {cdir}"
os.system(cmd)
print()

split_flist=[]
i=0;
_tmp=[]
for fl in flist:
    if i < N_FILE_PER_JOB:
        _tmp.append(fl)
        i+=1
    else:
        i=0
        split_flist.append(_tmp)
        _tmp=[]

if len(_tmp) :
    split_flist.append(_tmp)

cmd_mkd=f'mkdir -p {dest}'
os.system(cmd_mkd)

cms_cfg_tpl=str(tpl.TEMPLATE_CMS_CFG)

run_template=str(tpl.TEMPLATE_CRUN_SCRIPT)
run_template=run_template.replace("@@HOME",HOME)
run_template=run_template.replace("@@X509_USER_PROXY",X509_USER_PROXY)
run_template=run_template.replace("@@CMSSW_DIR",cmssw_base)


cmd=f"cp {cmsrun_cfg} {cdir}/input_cfg.py"
print(cmd)
os.system(cmd)

for fi,fls in enumerate(split_flist):
    if (args.nJobMax > -1) and fi>=args.nJobMax:
        break
    print(f"\r   Adding job for [{fi:>3} / {len(split_flist)}]   ",end="")
    ofname=f"Ntuple_{fi}_{uq}.root"
    flist='\n\t\t'+',\n\t\t'.join(["'file:"+fl+"'" for fl in fls])
    cms_cfg=str(cms_cfg_tpl)
    cms_cfg=cms_cfg.replace("@@NEVENTS",'-1')
    cms_cfg=cms_cfg.replace("@@FLIST",flist)
    cms_cfg=cms_cfg.replace("@@TEMPL_OUTFILE",ofname)
    
    cfg_fname=f"{cdir}/runCFG_{fi}.py"
    with open(cfg_fname,'w') as f:
        f.write(cms_cfg)
    cmd=str(run_template)
    cmd=cmd.replace('@@OUTPUTFILE',ofname)
    cmd=cmd.replace('@@DESTINATION',dest)
    cmd=cmd.replace("@@CMSRUN_CFG",cfg_fname)
    cmd=cmd.replace('@@PWD',pwd)
    cmd=cmd.replace('@@TIMESTAMP',timestamp)
    
    ofname=f"{cdir}/run_{fi}.sh"
    with open(ofname,'w') as f:
        cmd=cmd.replace("@@RUNSCRIPTNAME",ofname)
        f.write(cmd)
    os.system(f'chmod +x {ofname}')

print()
clogbaseDir=f"{CONDOR_LOG_BASE}/Condor/{unique_tag}/"
cmd=f"mkdir -p {clogbaseDir}"
os.system(cmd)

sub=tpl.TEMPLATE_CONDOR_SUB.replace("@@PWD",pwd)
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



