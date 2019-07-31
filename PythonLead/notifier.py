import pandas as pd,psycopg2,os,numpy as np
from sqlalchemy.sql import  text
from sqlalchemy import create_engine
import datetime,re
from datetime import timedelta
dat=datetime.datetime.today()
from collections import defaultdict
import smtplib
from collections import defaultdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE,formatdate
from email.mime.application import MIMEApplication

# get all records
def fetchAllRecords(engine):
    with engine.connect() as con:
        resoverall=con.execute('select * from records;')
        df = pd.DataFrame(resoverall.fetchall())
        df.columns = resoverall.keys()
        #del df['posted_day']
        return(df)

def sendMsg(list_of_files,actualFiles,to):
    msg = MIMEMultipart('alternative')
    sender='brai@defteam.com'
    subject = "Leads for the date : "+str(datetime.datetime.now()).split(" ")[0]
    links=""
    for heavyfile,linkFiles in zip(list_of_files,actualFiles):
        try:
            line= pd.read_csv(heavyfile).empty
            f=linkFiles.split("/")[1]
            print("F:   ",f)
            if line:
                links=links+"""<p>&emsp;&emsp; There is no new records on this file : """+f+"""</p>"""
            else:
                links=links+"""<p>&emsp;&emsp;<a href="""+linkFiles+""">"""+f+""" -click to download file</a></p>"""
        except OSError:
                print ("No file")
                
    body1="""\
                            <html>
                                   <head></head>
                                   <body>
                                          <p>Hi,</p>
                                                 <p>&emsp;Here are leads :</p>
                                            """+links+"""               
                                                 <p><br></p>
                                                 <P> From,</p>
                                                 <p> Analytics Team</p>                                          
                                   </body>       
                            </html>
                     """
    # set up the SMTP server
    s = smtplib.SMTP(host='smtp.office365.com', port=587)
    s.starttls()
    s.login('brai@defteam.com', 'BREN@9148318531')
    # setup the parameters of the message
    cc= "gs@defteam.com,jsanket@defteam.com,brai@defteam.com"
    recp=cc.split(",")+[to]
    msg['From']=sender
    msg['To']=to
    msg['Subject']=subject
    msg['Date']=formatdate(localtime=True)
    msg['Cc']=cc
    # add in the message body
    for filename in list_of_files:    
        attachment = MIMEApplication(open(filename,'r').read())
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
        msg.attach(attachment)
    msg.attach(MIMEText(body1, 'html'))
    # send the message via the server set up earlier.
    s.sendmail(msg['From'],recp,msg.as_string())
    s.quit()
    print("I have sent successfully")
    

def sendErrorMsg(error):
    msg = MIMEMultipart('alternative')
    msg1=MIMEMultipart()
    sender='kr19951111@gmail.com'
    subject = "Database Error has occurred for the date : "+str(datetime.datetime.now()).split(" ")[0]
    body = 'Hi Birendra,\n \t Jobs Crawled for the date: '+str(datetime.datetime.now()).split(" ")[0] +' is Unsuccessful.\n\n With Regards \n Birendra Rai'
    body1 = 'Hi Birendra,\n \t Jobs Crawled for the date: '+str(datetime.datetime.now()).split(" ")[0]+' couldnot Success because of '+ str(error)
    # set up the SMTP server
    s = smtplib.SMTP_SSL(host='smtp.gmail.com', port=465)
    #s.starttls()
    s.login('kr19951111@gmail.com', 'Google@199295')
    # setup the parameters of the message
    cc="kr19951111@gmail.com" #jsanket@defteam.com"
    to="brai@defteam.com"
    recp=cc.split(",")+[to]
    msg['From']=sender
    msg['To']=to
    msg['Subject']=subject
    msg['Date']=formatdate(localtime=True)
    msg['Cc']=cc
    # add in the message body
    msg.attach(MIMEText(body, 'plain'))
    # send the message via the server set up earlier.
    s.sendmail(msg['From'],recp,msg.as_string())
    #For Error to only Me
    msg1['From']=sender
    msg1['To']=to
    msg1['Subject']=subject
    msg1['Date']=formatdate(localtime=True)
    # add in the message body
    msg1.attach(MIMEText(body1, 'plain'))
    # send the message via the server set up earlier.
    s.sendmail(msg1['From'],to,msg1.as_string())
     
    s.quit()
    print("I have sent successfully")

numberOfReceivers=1
listOfReceiver=['vkapoor@defteam.com']

if __name__== '__main__':
    dataset= pd.DataFrame()
    try:
        engine=create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/job_data')
        dataset=fetchAllRecords(engine)
    except Exception as e:
        sendErrorMsg(e)
    #separating the datasets for indeed and monster
    neglectedCompany= pd.read_csv("C:/Users/brai/Documents/PythonLead/NeglectedCompany.csv")
    for c in neglectedCompany.company:
        dataset=dataset.loc[dataset['company_name'] !=c]
    #unique portal id
    numberOfPortal=dataset.portal_id.unique()
	
  
    #get portal address or name
    websiteportal_id=[]
    website=[]
    #getting all the id and portals from websiteportal db
    try:
        with engine.connect() as con1:
            result_set1=con1.execute('select id,portal from websiteportal;')
            for i in result_set1:
                websiteportal_id.append(i['id']);
                website.append(i['portal'])
    except Exception as e:
        print(e)
        #sendErrorMsg(e)

    try:    
        #for files
        list_of_files=defaultdict(list)
        actual_files=defaultdict(list)
        #for each portal iterate all the records which are past two days from current date and extract those records
        for i in numberOfPortal:
            anyPortalDf= dataset.loc[dataset.portal_id==i]
            print('Shape of ',website[i-1],anyPortalDf.shape)
            #removing Duplicates based on these columns
            notDup= anyPortalDf.drop_duplicates(subset=['portal_id', 'posted_date','designation','company_name'],keep=False)
                    
            #onlyRequiredDf=notDup.loc[notDup.posted_date > dat.date()- timedelta(days=15),dataset.columns[2:]]
            onlyRequiredDf=notDup.loc[notDup.posted_date.values == dat.date(),dataset.columns[3:-1]] #last column is record counter
            print('RemovingDuplicatioin ',website[i-1] ,': ',notDup.shape)
            print("Final_I.E_till_2_days_back:",onlyRequiredDf.shape)
            onlyRequiredDf['Job']=[ele.split(" ")[-1] for ele in onlyRequiredDf['location'].tolist()]
            onlyRequiredDf['location']=[ele.split(" ")[:-1] for ele in onlyRequiredDf['location'].tolist()]
            #creating files for receivers
            print(onlyRequiredDf)
            onlyRequiredDf['receiverId']=np.random.randint(0,numberOfReceivers,onlyRequiredDf.shape[0])
            #writing to file
            for receiver in range(0,numberOfReceivers):
                files="C:/Users/wamp/htdocs/"+str(website[i-1])+"_final_job_search_"+str(dat.strftime('%d_%m_%Y'))+'_'+str(receiver)+'_.csv'
                actual_file="52.170.238.21/"+str(website[i-1])+"_final_job_search_"+str(dat.strftime('%d_%m_%Y'))+'_'+str(receiver)+'_.csv'
                dff=onlyRequiredDf.loc[onlyRequiredDf.receiverId==receiver,:]
                dff.loc[:,dff.columns[:-1]].to_csv(files,index=False)
                list_of_files[receiver].append(files)
                actual_files[receiver].append(actual_file)
        for i in list_of_files.keys():
            sendMsg(list_of_files[i],actual_files[i],listOfReceiver[i])
    except Exception as e:
        sendErrorMsg(e)

