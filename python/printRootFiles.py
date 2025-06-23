import argparse,os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-b',"--base", help="comma separated folders to look at",default=None)
args = parser.parse_args()

flist=[]
if args.base is not None:
    for base in args.base.split(","):
        for (root,dirs,files) in os.walk(base, topdown=True):
            fls=[root+'/'+i  for i in files if i.endswith('.root')]
            flist+=fls
for i in flist:
    print(i)
