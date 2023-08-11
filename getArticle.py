#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import os
import datetime
import re
import random
import pytz
import threading
import json
import csv
import time
import requests
import sqlite3
from multiprocessing.pool import ThreadPool

lockNews = threading.Lock()

def __init__(self):
    if os.path.exists("./result/apNews.txt"):
        pass
    else:
        fp = open("./result/apNews.txt","w")
        fp.close
def converToUTC(self, date):
    
    if date == "-":
        return date
    date_string = date
    time_format = "%Y-%m-%dT%H:%M:%S"
    date_time = datetime.datetime.strptime(date_string, time_format)
    utc_tz = pytz.timezone('UTC')
    date_time_utc = utc_tz.localize(date_time)
    date_time_utc_str = str(date_time_utc)
    date_time_utc_str = date_time_utc_str[:-6]
    
    return date_time_utc_str



def manageFile():
    
    fp = open("./config.json","r", encoding="utf-8")
    cfg = json.load(fp)
    fp.close()
    
    fileName = cfg["news_csv_setting"]["file_name"]
    fileCounter = cfg["news_csv_setting"]["file_counter"]
    fileType = cfg["news_csv_setting"]["file_type"]
        
    
    if fileCounter == -1:
        fullName = fileName + fileType
    else:
        fullName = fileName + str(fileCounter) + fileType


        
    if os.path.exists(fullName):
        if os.path.getsize(fullName) > 13000000:
            print(os.path.getsize(fullName))
            fileCounter = fileCounter + 1
            newName = fileName + str(fileCounter) + fileType
            lockNews.acquire()
            newLinkfp = open(newName,"w")
            newLinkfp.close()
            
            cfg["file_setting"]["file_counter"] = fileCounter
            
            fp = open("./config.json","w", encoding="utf-8")
            
            json.dump(cfg, fp, indent=4)
            
            fp.close()
            lockNews.release()
            
            return newName
        
        else:
            return fullName             
    else:
        fp = open(fullName,'w')
        w = csv.DictWriter(fp,['Title','Description','Date','URL','Author','Tags','Category','Translate'])
        w.writeheader()
        fp.close()
    
def proxy():
    proxyList=[
        "http://localhost:3026",
        "http://localhost:3027",
        "http://localhost:3028",
        "http://localhost:3029",
        "http://localhost:3030",
        "http://localhost:3031",
        "http://localhost:3032",
        "http://localhost:3033",
        "http://localhost:3034",
        "http://localhost:3035",
        "http://localhost:8150",
        "http://localhost:8151",
        "http://localhost:8152",
        "http://localhost:8153",
        "http://localhost:8154",
        
        ]
    
    ranPX = random.choice(proxyList)
    return ranPX

def getArticle(info):
    try:
        rowID = info[0]
        print(rowID)
        print(type(rowID))
    
        link = info[1]
        
        if "http" in link:
            pass
        else:
            link = "https://apnews.com" + link
        
        category = info[2]
        
        if category !="article":
            return 0
        
        
        os.environ["HTTP_PROXY"] = proxy()
                
        time.sleep(random.randint(1, 5))
        
        resp = requests.get(link)
        
        if resp.status_code == 200:
            
            decoding = resp.content.decode("utf-8") 
            
            bs = BeautifulSoup(decoding,"html5lib")
        
            attr = re.compile(".*Component-heading-.*")
            
            xTitle = bs.find("h1",attrs={"class":'Component-heading-0-2-21'})
            
            if xTitle:
                xTitle = xTitle.get_text().strip()
            else:
                xTitle = bs.find('meta',attrs={'property':"og:title"}).get("content")
            
            attr = re.compile(".*Component-bylines-.*")
            xAuthor = bs.find('span',attrs={'class':attr})
            if xAuthor:
                xAuthor= str(xAuthor.get_text().strip())
            
            elif bs.find('div',attrs={'class':"Page-authors"}):
                xAuthor = bs.find('div',attrs={'class':"Page-authors"}).get_text().strip()
            else:
                xAuthor = "-"
            
            attr = re.compile(".*Timestamp Component-.*")
            xDate = bs.find("span", attrs={'class':attr})
            if xDate:
                xDate = str(xDate.get("data-source"))
                xDate = re.findall(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", xDate)[0]
                
            else:
                xDate = bs.find("meta", attrs={'property':'article:published_time'}).get("content").replace("T", " ")
                #sample = "%Y-%m-%dT%H:%M:%S"
                #xDate = self.converToUTC(xDate)
            
            
            xTags = bs.find('meta',attrs={'name':"keywords"})
            if xTags:
                xTags = xTags.get("content")
            else:
                xTags = bs.find_all("meta",attrs={'property':'article:tag'})
                if xTags:
                    xTags = xTags.get("content")
                else:
                    xTags = "-"
                
            #mr = myRequiers.Needs()
            
            xDescription = ""
            #xGoogleTr = ""
            " "
            attr = re.compile(".*Component-root-.*")
            article = bs.find("div",attrs={'class':'Article'})
            if article:
                article = article.find_all('p',attrs={"class":attr})
                for p in article:
                    xDescription += p.get_text().strip()
                    #xGoogleTr += mr.googletranslation(p.get_text().strip())
            else:
                attr = re.compile(".*RichTextStoryBody.*")
                article = bs.find("div",attrs=attr)
                if article:
                    article = article.find_all('p')
                    for p in article:
                        xDescription += p.get_text().strip()
                else:
                    attr = re.compile(".*RichTextBody.*")
                    article = bs.find("div",attrs=attr)
                    if article:
                        article = article.find_all('p')
                        for p in article:
                            xDescription += p.get_text().strip()
                    else:
                        
                        err = " Can't Find Description in  " + str(info[0]) +"\n"
                        print(err)
                    
                        #xGoogleTr += mr.googletranslation(p.get_text().strip())
            
            xCategory = bs.find("meta",attrs={'property':"article:section"})
            if xCategory:
                xCategory = xCategory.get("content")
            else:
                xCategory = "-"
            
            
            
            if bool(xDescription):
                
                
                    
                # return {'title':xTitle , 'description' : xDescription,
                          
                #           'newsDate': xDate, 'url' : info[0], 'site':info[2],
                          
                #           'author': xAuthor, 'tags': xTags, 'xCategory':xCategory,
                          
                #           #'argosTr': xArgosTr, 'googleTr': xGoogleTr,
                          
                #           'crawl' : 1}
            
                newsDict={}
                newsDict['Title'] = xTitle.strip()
                newsDict['Description'] = xDescription.strip()
                newsDict['Date'] = xDate.strip()
                newsDict['URL'] = link.strip()
                newsDict['Author'] = xAuthor.strip()
                newsDict['Tags'] = xTags.strip()
                newsDict['Categories'] = xCategory.strip()
                
                
                
                lockNews.acquire()
                
                
                with open("./news.csv","a", encoding="utf-8") as f:
                    w = csv.DictWriter(f, ['Title','Description',"SubTitle",'Date','URL','Author','Tags','Categories','Translate'])
                    w.writerow(newsDict)
                    
                    conn = sqlite3.connect("./news.db")

                    cursor = conn.cursor()
                    cursor.execute(f'Update data_table set is_crawl=1 where rowid=={rowID} ')
                    conn.commit()
                    conn.close()
                    
                lockNews.release()
                
    except Exception as e:
        print(e)            




conn = sqlite3.connect("./news.db")
cursor = conn.cursor()
cursor.execute('select count(*) from data_table where is_crawl==0')
count = cursor.fetchall()
conn.close()
print(count)
right = int(count[0][0]/6)
left = 0

for i in range(0 , 7):
    conn = sqlite3.connect("./news.db")

    cursor = conn.cursor()
    cursor.execute(f'select rowid,url,category from data_table where is_crawl=0 and rowid BETWEEN {left} AND {right} ')
    infos = cursor.fetchall()
    conn.close()
    
    tpool = ThreadPool(4)
    tpool.map(getArticle, infos)
    
    left = right
    right = right*2



