
# coding: utf-8

# In[11]:


import requests,pandas as pd
from bs4 import BeautifulSoup

import datetime
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from notifier import sendErrorMsg
from indeed import getDailyIdFromDailyTable,insertRecordsToRecords
#this function gets portal id from websiteportal 
def getPortalIdFromWebsitePortal(engine):
    with engine.connect() as connection:
        result_set= connection.execute("SELECT id from websiteportal where portal = 'Dice' limit 1;")
        for i in result_set:
            return(i['id'])
company=[]
companyurl=[]
posted_links=[]
designation=[]
summary=[]
location=[]
posted_day=[]
posted_date=[]
#for main dataframe
dataFrame=pd.DataFrame()
df=pd.DataFrame()

searchJob=[
    'Jaspersoft','Talend','Talend Developer','Power BI Developer',\
    'looker','AngularJS','Couchbase','ReactJS Developer','Tableau Developer',\
    'Tableau Developer Data Analyst','Tableau Developer Administrator'
    ]
searchInCountry=['Canada','New York','USA','Ireland','Saudi Arabia','Qatar','Oman','Bahrain','Kuwait']
dat=datetime.datetime.today()

for job in searchJob:
    for country in searchInCountry:
        url="https://www.dice.com/jobs/q-"+str(job)+"-sort-date-limit-100-l-"+str(country)+"-radius-100-startPage-1-limit-100-jobs?searchid=9420212359624&postedDate=1&stst="
        soup= BeautifulSoup(requests.get(url).text,'lxml')
        try: #to check the required jobs are there or not
            numberOfJobs=soup.find('span',id='posiCountMobileId').text
            results= soup.find_all("div",class_="complete-serp-result-div".split())
            #This are the crawling deep into website ,i.e. retrieving all the contents
            counter=0
            page=2
            while counter < int(numberOfJobs):
                for post in results:
                    try:
                        company.append(post.find('span',class_='compName').text)
                        companyurl.append("https://www.dice.com"+post.find('a',{"class":"dice-btn-link"})['href'])
                        designation.append(post.find('span',{"itemprop":"title"}).text)
                        location.append(post.find("span",{"class":"jobLoc"}).text)
                        posted_links.append("https://www.dice.com"+post.find('a',{"class":"circle".split()})['href'])
                        posted_day.append(post.find('li',{"class":"posted col-xs-12 col-sm-2 col-md-2 col-lg-2 margin-top-3 text-wrap-padding"}).text.splitlines()[1])
                        posted_date.append(post.find('span',{"itemprop":'datePosted'}).text.split('T')[0]) #only Date not Time
                        summary.append(post.find('span',{"itemprop":"description"}).text)
                    except Exception as e:
                        print(e,post)
                #every page is lists down 100 jobs so increment for next one
                counter +=100
                #if the all the jobs are crawled or i.e less than 100 then it wont proceed to next page 
                if(counter< int(numberOfJobs)):
                    url="https://www.dice.com/jobs/q-"+str(job)+"-sort-date-limit-100-l-"+str(country)+"-radius-100-startPage-%s-limit-100-jobs?searchid=9420212359624&stst="
                    url=(url%page)
                    page += 1
                    soup= BeautifulSoup(requests.get(url).text,'lxml')
                    results= soup.find_all("div",class_="complete-serp-result-div".split())
        except Exception as e:
            print("No Jobs are Found: ",job," in ",country)
        #print(company)
        #print(designation)
        #print(companyurl)
        #print(posted_links)
        #print(summary)
        #print(numberOfJobs)
        #print(location)
        print(len(company),job,country)
        df['posted_date']=posted_date
        df['posted_day']=posted_day
        df['posted_links']=posted_links
        df['designation']=designation
        df['company_name']=company
        df['companyurl']=companyurl
        df['location']= [ loc+' '+ job for loc in location]
        df['summary']=summary 
        
        dataFrame=dataFrame.append(df)
        company=[]
        companyurl=[]
        posted_links=[]
        designation=[]
        summary=[]
        location=[]
        posted_day=[]
        posted_date=[]
        df=pd.DataFrame()

dataFrame.to_csv("C:/Datasets/JobData/daily_dice_"+str(dat.strftime('%d_%m_%Y'))+".csv")
#framing the table and crawled date for DB
record=[(dat.date(),'daily_dice_'+str(dat.strftime('%d_%m_%Y')+".csv"))]
record_df=pd.DataFrame(record,columns=['crawled_date','filename'])
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/job_data')
daily_id=getDailyIdFromDailyTable(engine,record_df)
portal_id=getPortalIdFromWebsitePortal(engine)
dataMatrix=dataFrame
dataMatrix['day_id']=daily_id
dataMatrix['portal_id']=portal_id
dataMatrix['posted_date']=dataMatrix['posted_date'].apply(lambda d : datetime.datetime.strptime(d,"%Y-%m-%d").date() if isinstance(d, str) else d)
insertRecordsToRecords(engine,dataMatrix)

