#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 09:40:33 2023

@author: jack
"""

import requests
from bs4 import BeautifulSoup
import string
import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
from functools import partial
import os
import random
import csv
import time
import re
import sqlite3

lockNews = threading.Lock()
threadLock = threading.Lock()
try:
    fp = open("pageFailed.txt","x")
    fp.close()
    fp = open("errorFile.txt","x")
    fp.close()
except:
    pass


   
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


def reqPerPage(p,queryURL,fpName):
    
    try:
        time.sleep(random.randint(1, 5))
        os.environ["HTTP_PROXY"] = proxy()
        mainUrl = queryURL + f"&p={p}"
        resp = requests.get(mainUrl)
        decodedContent = resp.content.decode("utf-8")
        bs = BeautifulSoup(decodedContent,"html5lib")
        
        mainDiv = bs.find("div",attrs={'class':'PageList-items'})
        if mainDiv:
            items = mainDiv.find_all("div",attrs={"class":"PageList-items-item"})
            for itm in items:
                try:
                    titleDiv = itm.find("div",attrs={"class":"PagePromo-title"})
                    title = titleDiv.get_text()
                    url = titleDiv.find("a").get("href")
                    prps = str(url).split("/")
                    newPrps =list(filter(None, prps))
                    newsList =[title, url, newPrps[2]]
                    threadLock.acquire()
                    with open(fpName,"a", encoding="utf-8") as f:
                        w = csv.writer(f)
                        w.writerow(newsList)
                    threadLock.release()
                except:
                    pass
        
        
        print(mainUrl)
        
    except Exception as ce:
        print(ce)
        threadLock.acquire()

        err = str(ce) + "    in    " + mainUrl + "\n"
        fp = open("pageFailed.txt","a")
        fp.write(err)
        fp.close()
        threadLock.release()
        


def queryAlphabet(letter):
    try:
        csvName = f"links-of-{letter}.csv"
        fp = open(csvName,"x")
        fp.close()
    except:
        pass
    
    
    try:
        
        os.environ["HTTP_PROXY"] = proxy()
        queryURL = f"https://apnews.com/search?q={letter}&s=1"
        resp = requests.get(queryURL)
        decodedContent = resp.content.decode("utf-8")
        bs = BeautifulSoup(decodedContent,"html5lib")
        
        pageCount = bs.find("div",attrs={'class':'Pagination-pageCounts'})
        if pageCount:
            
            pageCount = str(pageCount.get_text().strip())
            pageCount = pageCount.split("of")
            pageCount = pageCount[1]
            pageCount = pageCount.replace(",", "")
            pageCount = int(pageCount)
        
        else:
            return f"None for {letter}"
        
        tpool = ThreadPool(4)
        fixArgs = partial(reqPerPage, queryURL=queryURL,fpName = csvName)
        tpool.map(fixArgs, range(1800,pageCount+1), chunksize= 10)
        
    except Exception as ce:
        print(ce)
        threadLock.acquire()

        err = str(ce) + "    in    " + queryURL
        fp = open("errorFile.txt","a")
        fp.write(err)
        fp.close()
        threadLock.release()

                       
alphabet = list(string.ascii_lowercase)
print(alphabet)



num_processes = multiprocessing.cpu_count()
pool = multiprocessing.Pool(num_processes - 2)
results = pool.map(queryAlphabet, alphabet)


print(results)



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
    cursor.execute(f'select rowid,url,category from data_table where is_crawl==0 and rowid BETWEEN {left} AND {right} ')
    infos = cursor.fetchall()
    conn.close()
    
    tpool = ThreadPool(4)
    tpool.map(getArticle, infos)
    
    left = right
    right = right*2