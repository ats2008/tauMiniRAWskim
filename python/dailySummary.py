import json,glob
import argparse,os
import datetime,uuid 
import util as utl
import argparse

FAILURE_TRIG=0.15

now = datetime.datetime.now() # current date and time
uq=uuid.uuid4().hex[:6].upper()
unique_tag=now.strftime("%d%b%y_%Hh%Mm%Ss")+f"_{uq}"
timestamp=now.strftime("%d/%B/%Y, %H:%M:%S")
pwd=os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument('-b',"--base", help="Base folder to look at",default="Condor/M@")
parser.add_argument(     "--doFullSummary", help="Print daily summary",action='store_true')
args = parser.parse_args()

base=args.base.replace('@','*')

print("Base path under study : ",base)
#Condor/MUON0_21Aug25_06h01m00s_FB2DF9/condor_submit.jdl

all_ds=set()
all_dt=set()

search_str=f"{base}/*.sh"
flist=glob.glob(search_str)
print(f"Got {len(flist)} active scripts")
active_map={}
for fl in flist:
    fd=fl.split("/")[-2]
    items=fd.split("_")
    ds=items[0]
    dt=items[1]
    tm=items[2]
    all_ds.add(ds)
    all_dt.add(dt)
    if ds not in active_map:
        active_map[ds]={}
    if dt not in active_map[ds]:
        active_map[ds][dt]=[]
    
    #active_map[ds][dt].append(tm+"_"+fl.split("/")[-1])
    active_map[ds][dt].append(fl)

search_str=f"{base}/*.sh.suc*"
flist=glob.glob(search_str)
print(f"Got {len(flist)} completed scripts")
success_map={}
for fl in flist:
    fd=fl.split("/")[-2]
    items=fd.split("_")
    ds=items[0]
    dt=items[1]
    tm=items[2]
    all_ds.add(ds)
    all_dt.add(dt)
    if ds not in success_map:
        success_map[ds]={}
    if dt not in success_map[ds]:
        success_map[ds][dt]=[[],[]]
    #success_map[ds][dt].append(tm+"_"+fl.split("/")[-1])
    success_map[ds][dt].append(fl)
    

summary_table=[]
missing_table=[]
problem_table=[]

summary_fr=[]
missing_fr=[]
problem_fr=[]

for ds in all_ds:
    for dt in all_dt:
        if ds not in active_map:
            acn=0
        elif dt not in active_map[ds]:
            acn=0
        else:
            acn=len(active_map[ds][dt])
        if ds not in success_map:
            scn=0
        elif dt not in success_map[ds]:
            scn=0
        else:
            scn=len(success_map[ds][dt])
        fld=f"failed {acn:>3}/{acn+scn:>3} ( {acn*100.0/(acn+scn+1e-9):>5.1f} % )"
        scc=f"succes {scn:>3}/{acn+scn:>3} ( {scn*100.0/(acn+scn+1e-9):>5.1f} % )"
        ostr=f"{ds:>10} | {dt:>10} | {fld:>25} | {scc:>25}"
        summary_table.append(ostr)
        fr=acn*1.0/(acn+scn+1e-9)
        summary_fr.append(fr)
        if fr >FAILURE_TRIG:
            nm=active_map[ds][dt][0].split("/")[:-1]
            problem_table.append(ostr+" | "+'/'.join(nm))
            problem_fr.append(fr)

        if scn+acn < 1:
            missing_table.append(ostr)
            missing_fr.append(fr)

def argsort(seq):
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)

if True:
    print("Failed job_clusters above fraction of filed jobs > {FAILURE_TRIG}")
    srt_idx= argsort([ -1*i for i in problem_fr] )
    for i in srt_idx:
        print(problem_table[i])

if args.doFullSummary:
    print("daily summary sorted in the decending  order of fraction of failed jobs in the a submitted job cluster")
    srt_idx= argsort([ -1*i for i in summary_fr ] )
    for i in srt_idx:
        print(summary_table[i])
    print("daily summary sorted in the decending  order of fraction of failed jobs in the a submitted job cluster")


