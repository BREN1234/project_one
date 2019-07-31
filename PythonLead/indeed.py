
# coding: utf-8

# In[19]:


import pandas as pd,re,requests,time
from bs4 import BeautifulSoup
import datetime
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text
#from notifier import sendErrorMsg

#returns the daily_id from dailytable
def getDailyIdFromDailyTable(engine,record_df):
    #get Cursor
    record_df.to_sql('daliytable', engine, if_exists='append',index=False)
    #alter the sequence of the id i.e. restarting from 1
    #ALTER SEQUENCE <tablename>_<id>_seq RESTART WITH 1
    with engine.connect() as connection:
        result_set=connection.execute("SELECT id from daliytable order by id desc limit 1;")
        for i in result_set:
            return(i['id'])

#this function gets portal id from websiteportal 
def getPortalIdFromWebsitePortal(engine):
    with engine.connect() as connection:
        result_set= connection.execute("SELECT id from websiteportal where portal = 'Indeed' limit 1;")
        for i in result_set:
            return(i['id'])

#inseting into recprds table
def insertRecordsToRecords(engine,data_matrix):
    data_matrix.to_sql('records',engine,if_exists='append',index=False)
    print("inserting success")


# In[37]:


if __name__=='__main__':    
    dat=datetime.datetime.today()
    start=0
    lengthOfRecord=0
    numberOfJobs=0
    dataFrame=pd.DataFrame()
    df=pd.DataFrame()
    date=[]
    company=[]
    location=[]
    summary=[]
    posted_date=[]
    posted_day=[]
    posted_links=[]
    companyUrl=[]
    designation=[]
    countries=['Canada','Singapore','USA','UK','Ireland','Oman','Qatar','Dubai','Bahrain']
    for job in ['jaspersoft','Talend','Talend+Developer','Power+BI','Power+Bi+Developer','looker','Couchbase','AngularJS','ReactJS','Tableau']:
        appendurl='/jobs?as_and='+str(job)+'&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&salary=&radius=25&l=&fromage=1&limit=50&start=%s&sort=date&psf=advsrch'
        urls=['https://ca.indeed.com','https://www.indeed.com.sg','https://www.indeed.com','https://www.indeed.co.uk','https://ie.indeed.com','https://om.indeed.com','https://qa.indeed.com','https://www.indeed.ae','https://bh.indeed.com']
        try:
            
            for url,country in zip(urls,countries):
                counter=0
                while lengthOfRecord <= numberOfJobs:
                    urll= ((url+appendurl)%start)
                    soup= BeautifulSoup(requests.get(urll).text,'lxml')
                    numberOfHits= soup.find("div",id="searchCount").text
                    numberOfHits= [re.sub(r'[^A-Za-z0-9]+', '', x) for x in numberOfHits.strip().split(" ")]
                    numberOfJobs=int(numberOfHits[-2])
                    # if len(numberOfHits) == 6:
                    #     numberOfJobs=int(numberOfHits[5])
                    # if len(numberOfHits) == 5:
                    #     numberOfJobs=int(numberOfHits[3])
                    print("Number of Jobs: ",numberOfJobs,"record count: ",start," Job: ",job,"country: ",country)
                    result= soup.find_all("div",class_="jobsearch-SerpJobCard unifiedRow row result".split())
                    lengthOfRecord= lengthOfRecord+len(result)
                    if lengthOfRecord <= numberOfJobs:
                        total=numberOfJobs
                        for firstLink in result:
                            try:
                                links= firstLink.find('a',{"class":"jobtitle turnstileLink "})
                                date.append(dat.strftime('%d-%m-%Y'))
                                company.append(firstLink.find('span',{"class":"company"}).text.strip())
                                try:
                                    location.append(firstLink.find('span',{"class":"location"}).text.strip())
                                except Exception as e:
                                    location.append(firstLink.find('div',class_="location").text.strip())
                                summary.append(firstLink.find('div',{"class":"summary"}).text.strip())
                                #if company name has link to it, it gives one as a length
                                companyl= firstLink.find('span',{"class":"company"}).findChildren()
                                #print(firstLink.find('span',{"class":"date"}))
                                #For posted date and day
                                try:
                                    #get the date or day from the webpage
                                    l=firstLink.find('span',{"class":"date"}).text
                                except Exception as e:
                                    print(e)
                                posted_day.append(l)
                                dt=None
                                #to check when the job was posted 'today' or '1 day ago'
                                #return first char of the  extracted date or day [Today-->'T',1 day ago-->1]
                                li=re.sub(r'[^0-9A-Z]','',l)
                                #converting days or date to particulat date format
                                try:
                                    #if li has 'T' then Date  is Todays date
                                    if li.isalpha():
                                        dt=dat.date()
                                    #else if li contains '1' then previous date
                                    else:
                                        dt=(dat-datetime.timedelta(days=int(li))).date()
                                except ValueError as e:
                                    print("i am out")
                                    print(e)
                               
                                posted_date.append(dt)
                                posted_links.append(url+links['href'])
                                designation.append((firstLink.find('a',class_="jobtitle turnstileLink ").text).strip())
                                companyUrl.append("NULL")
                                #if the companyl has length greater than 0 then it has href link
                                # if (len(companyl) != 0):
                                #     #first child contains the details
                                #     for i in companyl:
                                #         try:
                                #             companyUrl.append(url+i['href'])
                                #         except KeyError as e:
                                #             companyUrl.append("NULL")
                                #             print("KeyError: Not a Problem",e)
                                # else:
                                #     companyUrl.append("NULL")
                            except Exception as e:
                                print("Record wise:   ",e,firstLink)
                                break;
                                
                                
                    else:
                        break
                    counter +=1
                    start=start+50
                print(len(company),len(location),len(summary),len(posted_date),len(posted_day),len(posted_links),len(companyUrl))
                df['posted_date']=posted_date
                df['posted_day']=posted_day
                df['posted_links']=posted_links
                df['designation']=designation
                df['company_name']=company
                df['companyurl']=companyUrl
                df['location']= [ loc+' '+ job for loc in location]
                df['summary']=summary 
                print("DF shape and number of Jobs: ",df.shape,numberOfJobs)

                dataFrame=dataFrame.append(df)   
                start=0      #setting again 0 to new searches
                lengthOfRecord=0
                numberOfJobs=0
                df=pd.DataFrame()
                numberOfJobs=0
                date=[]
                company=[]
                location=[]
                summary=[]
                posted_date=[]
                posted_day=[]
                posted_links=[]
                companyUrl=[]
                designation=[]
        except Exception as e:
            print(e,"Url Problem: ",url)
            
    print(dataFrame.shape)
    dataFrame.to_csv("C:/Datasets/JobData/daily_indeed_"+str(dat.strftime('%d_%m_%Y'))+".csv")
    #framing the table and crawled date for DB
    record=[(dat.date(),'daily_indeed_'+str(dat.strftime('%d_%m_%Y')+".csv"))]
    record_df=pd.DataFrame(record,columns=['crawled_date','filename'])

    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/job_data')
    daily_id=getDailyIdFromDailyTable(engine,record_df)
    portal_id=getPortalIdFromWebsitePortal(engine)
    dataMatrix=dataFrame
    dataMatrix['day_id']=daily_id
    dataMatrix['portal_id']=portal_id
    dataMatrix['posted_date']=dataMatrix['posted_date'].apply(lambda d : datetime.datetime.strptime(d,"%Y-%m-%d").date() if isinstance(d, str) else d)
    insertRecordsToRecords(engine,dataMatrix)

