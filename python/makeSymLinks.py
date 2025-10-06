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

parser = argparse.ArgumentParser()
parser.add_argument('-t',"--target", help="Target location",default='collected_files/')
parser.add_argument('-b',"--base", help="Base folder to look at",default=None)
parser.add_argument('-e',"--execute", help="Execute the symlink creation",default=False,action='store_true')
args = parser.parse_args()

if args.base is not None:
    base=args.base

print(f"Files under {base} is being looked at !")
target=args.target
flist=[]
totalsize=0.0
for (root,dirs,files) in os.walk(base, topdown=True):
    fls=[root+'/'+i  for i in files if i.endswith('.root')]
    flist+=fls
    totalsize+=sum([os.path.getsize(fp) for fp in fls])
totalsize/=2**30
print("Got : ",len(flist)," Files ")
print("Total size : ",f"{totalsize:.2f} GB" )

tpl=f"ln -s @@SRC @@TARGET"
tpl=tpl.replace("@@TARGET",target)

if args.execute:
    os.system('mkdir -p '+target)
    for i,fl in enumerate(flist):
        cmd=tpl.replace("@@SRC",fl)
        os.system(cmd)
        if (i%100)==0:
            print(f"\rProcessing  {i+1}/{len(flist)}",end="       ")
        if (i%1000)==0:
            print()
    print()
    exit()

fo=open("create_symlinks.sh","w")
fo.write('mkdir -p '+target+"\n")
for fl in flist:
    cmd=tpl.replace("@@SRC",fl)
    fo.write(cmd+"\n")
fo.close()

