import argparse,os
import smtplib
from email.mime.text import MIMEText

parser = argparse.ArgumentParser()
parser.add_argument('-m',"--minHours", help="minimum time left for your proxy for mailing ",default=12.0)
args = parser.parse_args()

X509_USER_PROXY=os.environ.get('X509_USER_PROXY')
HOME=os.environ.get('HOME')
USER=os.environ.get('USER')
CONTACT_EMAIL=os.environ.get('CONTACT_EMAIL')

me=USER+'@cern.ch'
if CONTACT_EMAIL is not None:
    target_email=  CONTACT_EMAIL.split()
else :
    target_email=[]
if len(target_email) < 1:
    target_email=[me,'aravindsugunan@gmail.com']
print("Collecting voms proxy info")
os.system("voms-proxy-info > _voms_info")
with open("_voms_info") as f:
    txt=f.readlines()
t=0
for l in txt:
    if 'timeleft' in l:
        tstr=l[:-1].split(":")
        h=float(tstr[1])
        m=float(tstr[2])
        s=float(tstr[3])
        t=h+(m+s/60.0)/60.0
        print(f"Time left in proxy in {t:.3} hours")
        break
if t >= args.minHours:
    exit()

if t==0:
    msg = MIMEText("Please update the proxin you lxplus account","plain")
    msg['Subject'] = 'Proxy is not valid! '
elif t <args.minHours:
    msg = MIMEText(f"Time left in voms-proxy is {t:.2f} hours. Please consider renewing","plain")
    msg['Subject'] = 'Proxy needs to be updated ! '
msg['From'] = me
msg['To']   = ','.join(target_email)
print("Informing to ",target_email)
#print("  > MSG \n",msg)
s = smtplib.SMTP('localhost')
s.sendmail(me, target_email, msg.as_string())
s.quit()
if t==0:
    exit(1)
