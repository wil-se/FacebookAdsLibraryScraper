import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import sqlite3
import logging
from logging import handlers
import time
import os
import re
import json
import pycountry
from requests_html import HTMLSession
from fake_useragent import UserAgent
import threading
from datetime import datetime, timedelta
import configparser
import random
from pprint import PrettyPrinter
import sys
pp = PrettyPrinter()


class FacebookScraperWorker():
    log = None
    settings = ''
    connection = None
    cursor = None
    id = 0
    active_ads = 1
    date_from = ''
    date_to = ''
    country = ''
    country_alpha2 = ''
    keyword = ''
    cookies = {}
    sleep = 10
    referrer = ''
    spawnTime = datetime.now()
    maxTimeAllowed = 15*60 # seconds
    
    
    def __init__(self, maxTimeAllowed=15*60, settings="./config.cfg", db_path='found.db', sleep=10, id=0, active_ads=1, date_from="", date_to="", country="", keyword=""):
        self.maxTimeAllowed = maxTimeAllowed
        self.settings = settings
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.id = id
        self.active_ads = active_ads
        self.date_from = date_from
        self.date_to = date_to
        (self.country_alpha2, self.country) = self.get_safe_country(country)
        self.keyword = keyword
        self.sleep = sleep
        if not os.path.exists('logs'):
            os.mkdir('logs')
        self.log = logging.getLogger(f'FacebookWorker_{self.id}_{self.keyword}_{random.randint(0, 99999999999)}')
        FORMAT = '[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s'
        logfile = time.strftime(f'logs/worker_{id}_{self.country_alpha2}_{keyword}_%Y-%m-%d.log')
        logging.basicConfig(level=logging.INFO, filename=logfile, filemode='a+', format=FORMAT)
        handler = handlers.RotatingFileHandler(logfile, maxBytes=1000000, backupCount=10)
        self.log.addHandler(handler)
        self.log.addHandler(logging.StreamHandler(sys.stdout))
        self.referrer = f'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={self.country_alpha2}&media_type=all'

    def __del__(self):
        self.connection.close()

    def get_safe_country(self, name):
        try:
            countries = {
                'United States of America': 'United States'
            }
            if name in countries.keys():
                return (pycountry.countries.get(name=countries[name]).alpha_2, countries[name])
            return (pycountry.countries.get(name=name).alpha_2, name)
        except:
            return ('','')

    def rget(self, url, headers={}, allow_redirects=True, timeout=10, proxies={}):
        try:
            r = requests.get(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)
            return r
        except Exception as e:
            # self.log.info(e)
            # self.log.info(f'GET request failed, retrying')
            time.sleep(self.sleep)
            self.rget(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)

    def rspost(self, url, session, data={}, headers={}, allow_redirects=True, timeout=10, proxies={}):
        try:
            r = session.post(url, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)
            return r
        except Exception as e:
            # self.log.info(e)
            # self.log.info(f'POST request failed, retrying')
            time.sleep(self.sleep)
            return self.rspost(url, session, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)

    def get_proxy(self):
        try:
            configParser = configparser.RawConfigParser()
            configParser.read(self.settings)
            proxy_username = configParser.get('credentials', 'username')
            proxy_apikey = configParser.get('credentials', 'apikey')
            proxy_ip = configParser.get('credentials', 'host')
            proxy_port = configParser.get('credentials','port')
            url = f'http://{proxy_username}:{proxy_apikey}@{proxy_ip}:{proxy_port}'
            return {
                'http': url,
                'https': url
            }
        except:
            self.log.info('there was an error while loading configurations, please check config.cfg')
            return {
                'http': '',
                'https': ''
            }

    def get_cookies(self, keepsession=False, keepcursor=False):
        try:
            self.log.info(f'Getting cookies {self.country_alpha2} {self.keyword}')
            r = self.rget(self.referrer, proxies=self.get_proxy())
            response_text = r.text
            self.cookies['user_id'] = re.findall('USER_ID\\":\\"(.*?)\\",', response_text)[0]
            self.cookies['lsd'] = re.findall('LSD[^:]+:\\"(.*?)\\"', response_text)[0]
            self.cookies['dtsg'] = re.findall('DTSGInitialData[\\s\\S]+?token\\":\\"(.*?)\\"', response_text)[0]
            self.cookies['hs'] = re.findall('haste_session\\":\\"(.*?)\\",', response_text)[0]
            self.cookies['hsi'] = re.findall('hsi\\":\\"(.*?)\\",', response_text)[0]
            if not keepsession:
                self.cookies['sessionid'] = re.findall('sessionId\\":\\"(.*?)\\",', response_text)[0]
            if not keepcursor:                
                self.cookies['forwardCursor'] = ''
                self.cookies['collationToken'] = ''
            return self.cookies
        except Exception as e:
            # self.log.info(e)
            self.log.info(f'Cookie generation failed for {self.country_alpha2} {self.keyword}, retrying')
            time.sleep(self.sleep)
            return self.get_cookies()

    def get_product(self, id):
        try:
            return self.cursor.execute(f'SELECT * FROM products WHERE adid="{id}"').fetchone()
        except:
            self.log.info('error while executing database query')

    def sanitize(self, string):
        return string.replace('"', '')

    def create_product(self, adid, active_ads, country, adtitle, keyword, pageurl, adsonpage, adurl, creation_time, tag='dropship'):
        try:
            self.log.info(f'creating product {adid} {keyword}')
            query = f'INSERT OR IGNORE INTO products (adid,active_ads,country,adtitle,keyword,pageurl,adsonpage,adurl,creation_time,tag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            self.cursor.execute(query, [adid, active_ads, country, self.sanitize(adtitle), keyword, pageurl, adsonpage, adurl, creation_time, tag])
            self.connection.commit()
        except:
            self.log.info('error while executing database query')


    def update_product(self, adid, active_ads, country, adtitle, keyword, pageurl, adsonpage, adurl, creation_time, tag='dropship'):
        try:
            self.log.info(f'updating product {adid} {keyword}')
            query = f'UPDATE products SET active_ads=?, country=?, adtitle=?, keyword=?, pageurl=?, adsonpage=?, adurl=?, creation_time=?, tag=? WHERE adid=?'
            self.cursor.execute(query, (active_ads, country, self.sanitize(adtitle), keyword, pageurl, adsonpage, adurl, creation_time, tag, adid))
            self.connection.commit()
        except:
            self.log.info('error while executing database query')

    def run(self):
        self.get_cookies()
        ua = UserAgent()
        s = requests.Session()
        # link = f'https://www.facebook.com/ads/library/async/search_ads/?q={self.keyword}&session_id={self.cookies["sessionid"]}&count=30&active_status=all&ad_type=all&countries[0]={self.country_alpha2}&start_date[min]={self.date_from}&start_date[max]={self.date_to}&media_type=all&search_type=keyword_unordered'
        s.headers['User-Agent'] = ua.Chrome
        s.headers['Content-Type'] = 'application/x-www-form-urlencoded'
        s.headers['Host'] = 'web.facebook.com'
        s.headers['Origin'] = 'https://web.facebook.com'
        s.headers['Referer'] = self.referrer
        s.headers['Sec-Fetch-Site'] = 'same-origin'

        now = datetime.now()

        while self.cookies['forwardCursor'] != None and now - self.spawnTime < timedelta(seconds=self.maxTimeAllowed):
            now = datetime.now()
            link = f'https://www.facebook.com/ads/library/async/search_ads/?q={self.keyword}&forward_cursor={self.cookies["forwardCursor"]}&session_id={self.cookies["sessionid"]}&collation_token={self.cookies["collationToken"]}&count=16&active_status=all&ad_type=all&countries[0]={self.country_alpha2}&start_date[min]={self.date_from}&start_date[max]={self.date_to}&media_type=all&search_type=keyword_unordered'
            payload = {
                '__user': 0,
                '__a': 1,
                '__csr': '',
                '__req': 1,
                '__hs': self.cookies['hs'],
                'dpr': 1,
                '__ccg': 'EXCELLENT',
                '__hsi': self.cookies['hsi'],
                'lsd': self.cookies['lsd'],
                # 'fb_dtsg': self.cookies['dtsg'],
                'forward_cursor': self.cookies['forwardCursor'],
                'collation_token': self.cookies['collationToken']
            }
            s.headers['x-fb-lsd'] = self.cookies['lsd']
            result = self.rspost(link, s, data=payload, proxies=self.get_proxy()).text.replace('for (;;);', '')
            try:
                parsed = json.loads(result)
            except:
                self.log.info('error parsing JSON result')
                continue
            try:
                containers = parsed['payload']['results']
            except Exception as e:
                self.log.info(f'declined ad {self.country_alpha2} {self.keyword}: {parsed["errorSummary"]}, retrying')
                self.get_cookies(keepcursor=True)
                time.sleep(self.sleep)
                continue
            
            self.cookies['collationToken'] = parsed['payload']['collationToken']
            self.cookies['forwardCursor'] = parsed['payload']['forwardCursor']
            
            for ad  in containers:
                ad = ad[0]
                adid = ad['adArchiveID']
                active_ads = ad['collationCount']
                try:
                    adtitle = ad['snapshot']['title']
                except:
                    adtitle = ad['snapshot']['page_name']
                finally:
                    if adtitle == None:
                        try:
                            adtitle = ad['snapshot']['page_name']
                        except:
                            adtitle = "MISSING"
                try:
                    pageurl = ad['snapshot']['link_url']
                    domain = re.search("https+://(www.)*[a-zA-Z0-9-]*.[a-zA-Z0]*", pageurl).group().replace("https", "").replace("http", "").replace("://", "").replace("www.", "")
                    adsonpage = f'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=ALL&q={domain}'
                except:
                    pageurl = ''
                    domain = ''
                    adsonpage = ''
                try:
                    creation_time = datetime.fromtimestamp(ad["snapshot"]["creation_time"])
                except:
                    creation_time = datetime.fromtimestamp(ad["startDate"])
                adurl = f'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=ALL&q={adid}&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped&search_type=keyword_unordered&media_type=all'
                if active_ads and int(active_ads) >= int(self.active_ads) and pageurl != '':
                    check = self.get_product(adid)
                    if check:
                        self.update_product(adid, active_ads, self.country_alpha2, adtitle, self.keyword, pageurl, adsonpage, adurl, creation_time)
                    else:
                        self.create_product(adid, active_ads, self.country_alpha2, adtitle, self.keyword, pageurl, adsonpage, adurl, creation_time)

            time.sleep(self.sleep)
            self.log.info(f'fetching next page')
        self.log.info(f'{self.country_alpha2} {self.keyword} done.')


class FacebookScraperMaster():
    log = None
    connection = None
    cursor = None
    maxTimeAllowed = 60*15 # seconds

    def __init__(self, db_path='found.db') -> None:
        if not os.path.exists('logs'):
            os.mkdir('logs')
        self.log = logging.getLogger('FacebookMaster')
        FORMAT = '[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s'
        logfile = time.strftime('logs/master_%Y-%m-%d.log')
        logging.basicConfig(level=logging.INFO, filename=logfile, filemode='a+', format=FORMAT)
        handler = handlers.RotatingFileHandler(logfile, maxBytes=1000000, backupCount=10)
        self.log.addHandler(handler)
        self.log.addHandler(logging.StreamHandler(sys.stdout))
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.check_db()

    def __del__(self):
        self.connection.close()

    def get_filters(self):
        try:
            return list(map(lambda filter: {
                'active_ads': filter[1],
                'date_from': filter[2],
                'date_to': filter[3],
                'country': filter[4],
                'keywords': filter[5].split(',')
            }, self.cursor.execute("SELECT * FROM filters").fetchall()))
        except:
            self.log.info('error while executing database query')


    def run(self):
        filters = self.get_filters()
        threads = []
        for filter in filters:
            for index, keyword in enumerate(filter['keywords']):
                worker = FacebookScraperWorker(
                    maxTimeAllowed=self.maxTimeAllowed,
                    id=index,
                    active_ads = int(filter['active_ads']),
                    date_from = filter['date_from'],
                    date_to = filter['date_to'],
                    country = filter['country'],
                    keyword=keyword.replace(' ', '-')
                    )
                t = threading.Thread(target=worker.run, args=())
                t.start()
                threads.append(t)

        for t in threads:
            t.join()
    
    def check_db(self):
        try:
            self.cursor.execute('SELECT * FROM products')
            self.cursor.execute('SELECT * FROM filters')
        except:
            self.cursor.execute("CREATE TABLE IF NOT EXISTS filters(id INTEGER PRIMARY KEY, active_ads INTEGER, date_from TEXT, date_to TEXT, country TEXT, keywords TEXT)")
            self.cursor.execute("CREATE TABLE IF NOT EXISTS products(adid INTEGER PRIMARY KEY, active_ads INTEGER, country TEXT, adtitle TEXT, keyword TEXT, pageurl TEXT, adsonpage TEXT, adurl TEXT, creation_time TEXT, tag TEXT)")


master = FacebookScraperMaster()
master.run()