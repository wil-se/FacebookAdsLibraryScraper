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
from datetime import datetime
import configparser
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
    proxy_ip = None
    proxy_port = None
    proxy_username = None
    proxy_apikey = None
    referrer = ''
    ids = []
    forwardCursor = ''
    collationToken = ''
    backwardCursor = ''
    
    
    def __init__(self, settings="./config.cfg", db_path='found.db', sleep=10, id=0, active_ads=1, date_from="", date_to="", country="", keyword=""):
        self.settings = settings
        self.connection = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self.id = id
        self.active_ads = active_ads
        self.date_from = date_from
        self.date_to = date_to
        self.country = country
        try:
            self.country_alpha2 = pycountry.countries.get(name=country).alpha_2
        except:
            print(country)
        self.keyword = keyword
        self.sleep = sleep
        if not os.path.exists('logs'):
            os.mkdir('logs')
        self.log = logging.getLogger(f'FacebookWorker_{self.id}')
        FORMAT = '[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s'
        logfile = time.strftime(f'logs/worker_{id}_{self.country_alpha2}_{keyword}_%Y-%m-%d.log')
        logging.basicConfig(level=logging.INFO, filename=logfile, filemode='a+', format=FORMAT)
        handler = handlers.RotatingFileHandler(logfile, maxBytes=1000000, backupCount=10)
        self.log.addHandler(handler)
        self.log.addHandler(logging.StreamHandler(sys.stdout))
        # self.log.info(f'address: {self.getIP()}')
        self.referrer = f'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={self.country_alpha2}&media_type=all'

    def __del__(self):
        self.connection.close()

    def getIP(self):
        try:
            d = requests.get('http://checkip.dyndns.com/', proxies=self.get_proxy()).text
            return re.compile(r'Address: (\d+\.\d+\.\d+\.\d+)').search(d).group(1)
        except:
            return self.getIP()

    def rget(self, url, headers={}, allow_redirects=True, timeout=10, proxies={}):
        try:
            r = requests.get(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)
            return r
        except Exception as e:
            self.log.info(e)
            self.log.info(f'GET request failed, retrying')
            self.rget(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)

    def rspost(self, url, session, data={}, headers={}, allow_redirects=True, timeout=10, proxies={}):
        try:
            r = session.post(url, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)
            return r
        except Exception as e:
            self.log.info(e)
            self.log.info(f'POST request failed, retrying')
            time.sleep(self.sleep)
            return self.rspost(url, session, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)

    def get_proxy(self):
        configParser = configparser.RawConfigParser()
        configParser.read(self.settings)
        self.proxy_username = configParser.get('credentials', 'username')
        self.proxy_apikey = configParser.get('credentials', 'apikey')
        self.proxy_ip = configParser.get('credentials', 'host')
        self.proxy_port = configParser.get('credentials','port')
        url = f'http://{self.proxy_username}:{self.proxy_apikey}@{self.proxy_ip}:{self.proxy_port}'
        return {
            'http': url,
            'https': url
        }

    def get_cookies(self, keepsession=False):
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
                self.cookies['forwardCursor'] = ''
                self.cookies['collationToken'] = ''
            return self.cookies
        except Exception as e:
            self.log.info(e)
            self.log.info(f'Cookie generation failed for {self.country_alpha2} {self.keyword}, retrying')
            time.sleep(self.sleep)
            return self.get_cookies()

    def get_product(self, id):
        return self.cursor.execute(f'SELECT * FROM products WHERE adid="{id}"').fetchone()

    def sanitize(self, string):
        return string.replace('"', '')

    # TODO: INIT DB
    # adid -> unique
    # fix insert values

    def create_product(self, adid, active_ads, country, adtitle, keyword, pageurl, adsonpage, adurl, creation_time, tag='dropship'):
        self.log.info(f'creating product {adid} {keyword}')
        query = f'INSERT OR IGNORE INTO products (adid,active_ads,country,adtitle,keyword,pageurl,adsonpage,adurl,creation_time,tag)\
            VALUES ("{adid}", {active_ads}, "{country}", "{self.sanitize(adtitle)}", "{keyword}", "{pageurl}", "{adsonpage}", "{adurl}", "{creation_time}", "{tag}" )'
        self.cursor.execute(query)
        self.connection.commit()

    def update_product(self, adid, active_ads, country, adtitle, keyword, pageurl, adsonpage, adurl, creation_time, tag='dropship'):
        self.log.info(f'updating product {adid} {keyword}')
        query = f'UPDATE products SET active_ads={active_ads}, country="{country}", adtitle="{self.sanitize(adtitle)}", keyword="{keyword}", pageurl="{pageurl}", adsonpage="{adsonpage}", adurl="{adurl}", creation_time="{creation_time}", tag="{tag}" WHERE adid={adid}'
        self.cursor.execute(query)
        self.connection.commit()

    def run(self):
        self.get_cookies()
        ua = UserAgent()
        s = requests.Session()
        link = f'https://www.facebook.com/ads/library/async/search_ads/?q={self.keyword}&session_id={self.cookies["sessionid"]}&count=30&active_status=all&ad_type=all&countries[0]={self.country_alpha2}&media_type=all&search_type=keyword_unordered'
        s.headers['User-Agent'] = ua.Chrome
        s.headers['Content-Type'] = 'application/x-www-form-urlencoded'
        s.headers['Host'] = 'web.facebook.com'
        s.headers['Origin'] = 'https://web.facebook.com'
        s.headers['Referer'] = link
        s.headers['x-fb-lsd'] = self.cookies['lsd']
        s.headers['Sec-Fetch-Site'] = 'same-origin'
        # s.headers['Accept'] = '*/*'
        # s.headers['Accept-Encoding'] = 'gzip, deflate, br'
        # s.headers['Connection'] = 'keep-alive'

        while self.cookies['forwardCursor'] != None:
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
            # pp.pprint(payload)
            # pp.pprint(s.headers)
            result = self.rspost(link, s, data=payload, proxies=self.get_proxy()).text.replace('for (;;);', '')
            parsed = json.loads(result)
            try:
                containers = parsed['payload']['results']
                self.cookies['collationToken'] = parsed['payload']['collationToken']
                self.cookies['forwardCursor'] = parsed['payload']['forwardCursor']
                self.log.info(f'{parsed["payload"]["forwardCursor"]}')
                self.log.info(f'{parsed["payload"]["collationToken"]}')
            except Exception as e:
                # print(f'parsed: {parsed["errorSummary"]}')
                # print(link)
                self.log.info(f'declined ad {self.country_alpha2} {self.keyword}: {parsed["errorSummary"]}, retrying')
                # self.log.info(e)
                # self.log.info(parsed)
                self.get_cookies(keepsession=True)
                time.sleep(self.sleep)
                continue
            
            if self.cookies['collationToken'] == parsed['payload']['collationToken']:
                print('WARNING!')
            if self.cookies['forwardCursor'] == parsed['payload']['forwardCursor']:
                print('WARNING!')
 
            self.log.info(self.cookies['collationToken'])
            self.log.info(self.cookies['forwardCursor'])
            
            for ad  in containers:
                ad = ad[0]
                adid = ad['adArchiveID']
                # if adid not in self.ids:
                #     self.ids.append(adid)
                # else:
                #     print(f'{self.keyword} warning!!')
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
                # ACTIVE ADS CAN BE NONE BUT AD CAN BE ACTIVE: SET TO 1?
                if active_ads and int(active_ads) >= int(self.active_ads) and pageurl != '':
                    check = self.get_product(adid)
                    if check:
                        print('HELLOOOO')
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
    
    def __del__(self):
        self.connection.close()

    def get_filters(self):
        return list(map(lambda filter: {
            'active_ads': filter[1],
            'date_from': filter[2],
            'date_to': filter[3],
            'country': filter[4],
            'keywords': filter[5].split(',')
        }, self.connection.execute("SELECT * FROM filters").fetchall()))

    def run(self):
        filters = self.get_filters()
        threads = []
        for filter in filters:
            for index, keyword in enumerate(filter['keywords']):
                worker = FacebookScraperWorker(
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
        

master = FacebookScraperMaster()
master.run()