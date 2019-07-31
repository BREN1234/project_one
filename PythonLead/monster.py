
# coding: utf-8

# In[7]:


from bs4 import BeautifulSoup
import requests,re,pandas as pd
import datetime

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from notifier import sendErrorMsg
from indeed import getDailyIdFromDailyTable,insertRecordsToRecords

#this function gets portal id from websiteportal 
def getPortalIdFromWebsitePortal(engine):
    with engine.connect() as connection:
        result_set= connection.execute("SELECT id from websiteportal where portal = 'Monster' limit 1;")
        for i in result_set:
            return(i['id'])

location=[]
company=[]
posted_day=[]
designation=[]
posted_links=[]
summary=[]
companyurl=[]
#dataframe for collecting data job wise
df=pd.DataFrame()
#Main dataframe
dataFrame=pd.DataFrame()

jobList=['Jaspersoft-__2CJaspersoft-Developer','Talend','PowerBI','looker','Couchbase','AngularJS','ReactJS','Tableau']
countries=['Canada','New York','USA','Ireland','Saudi Arabia','Qatar','Oman','Bahrain','Kuwait']
for country in countries:
    for job in jobList:
        page=1
        numberOfJobs=0
        #https://www.monster.com/jobs/search/?q=Python-Developer&where=USA&tm=3&stpage=1&page=3
        #repalce %s with page numbers
        #searhUrl="https://www.monster.com/jobs/search/?q=Jaspersoft&where=usa&page=%s"#"https://www.monster.com/jobs/search/?q="+str(job)+"&where="+str(country)+"&tm=0&page=%s"
        searhUrl="https://www.monster.com/jobs/search/?q="+str(job)+"&where="+str(country)+"&tm=3&stpage=1&page=%s"
        response=requests.get((searhUrl)%page) #first page to get number of jobs
        soup=BeautifulSoup(response.text,'lxml')
        #print(soup)
        try:
            numberOfJobs=int(re.findall(r"\d+",soup.find("h2",{"class":"figure"}).text)[0])
            print(numberOfJobs)
            
            #print("number Of jobs"+str(numberOfJobs))
            #manipulating the pages
            if numberOfJobs >200:
                page=10
            elif numberOfJobs >100:
                page=5
            elif numberOfJobs >75:
                page=4
            elif numberOfJobs >50:
                page=3
            elif numberOfJobs >25:
                page=2
            #collecting all the pages
            #print("selected page="+str(page))
            response=requests.get((searhUrl)%page) #first page to get number of jobs
            soup=BeautifulSoup(response.text,'lxml')
            searchResult=soup.find_all("section",class_="card-content ") #getting all the searched Results
            #print("length of the collected page: "+str(len(searchResult)))
            try:
                for i in searchResult:  #iterating one by one 
                    company.append(re.sub("\n","",i.find("div",{"class":"company"}).text))
                    location.append(re.sub("\n|\r","",i.find("div",{"class":"location"}).text))
                    posted_day.append(re.sub("\n","",i.find("time").text))
                    designation.append(re.sub("\n|\r","",i.find("h2").text))
                    posted_links.append(i.find("a",{"data-bypass":"true"})['href'])
            except Exception as e: # for search loop 
                print(e)
            #creating dataframe
            #print(len(posted_day),len(posted_links),len(company),len(location),len(designation))
            df['posted_day']=posted_day
            df['posted_date']=datetime.datetime.today().strftime('%Y-%m-%d')
            df['posted_links']=posted_links
            df['company_name']=company
            df['companyurl']=""
            df['location']=[ loc+' '+job for loc in location]
            df['locationurl']=""
            df['designation']=designation
            dataFrame=dataFrame.append(df)
            df=pd.DataFrame()
            location=[]
            company=[]
            posted_day=[]
            designation=[]
            posted_links=[]
            summary=[]
            companyurl=[]
        except Exception as e: #this for number of jobs
            print("There are no Records ",job,e)
print(dataFrame)
dataFrame.to_csv("C:/Datasets/JobData/daily_monsters_"+str(datetime.datetime.today().strftime('%Y_%m_%d'))+".csv")
record=[(datetime.datetime.today().date(),'daily_monster_'+str(datetime.datetime.today().strftime('%Y_%m_%d')+".csv"))]
record_df=pd.DataFrame(record,columns=['crawled_date','filename'])
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/job_data')
daily_id=getDailyIdFromDailyTable(engine,record_df)
portal_id=getPortalIdFromWebsitePortal(engine)
dataMatrix=dataFrame
dataMatrix['day_id']=daily_id
dataMatrix['portal_id']=portal_id
#print(dataMatrix)
print(daily_id,portal_id)
insertRecordsToRecords(engine,dataMatrix)
print(daily_id)

