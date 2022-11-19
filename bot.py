import requests
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


class colors:
    reset = '\033[0m'
    bold = '\033[01m'
    disable = '\033[02m'
    underline = '\033[04m'
    reverse = '\033[07m'
    strikethrough = '\033[09m'
    invisible = '\033[08m'

class fg:
    black = '\033[30m'
    red = '\033[31m'
    green = '\033[32m'
    orange = '\033[33m'
    blue = '\033[34m'
    purple = '\033[35m'
    cyan = '\033[36m'
    lightgrey = '\033[37m'
    darkgrey = '\033[90m'
    lightred = '\033[91m'
    lightgreen = '\033[92m'
    yellow = '\033[93m'
    lightblue = '\033[94m'
    pink = '\033[95m'
    lightcyan = '\033[96m'

class bg:
    black = '\033[40m'
    red = '\033[41m'
    green = '\033[42m'
    orange = '\033[43m'
    blue = '\033[44m'
    purple = '\033[45m'
    cyan = '\033[46m'
    lightgrey = '\033[47m'

class Country():
    def __init__(self):
        pass
    @staticmethod
    def get(name):
        replace = {
            'United States Of America': 'United States'
        }
        if name in replace.keys():
            name = replace[name]
        return pycountry.countries(name)


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
    lastCollationToken = ''
    lastForwardCursor = ''
    

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
            # self.log.info(e)
            self.log.info(f'{fg.red}GET request failed, retrying{colors.reset}')
            self.rget(url, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)

    def rspost(self, url, session, data={}, headers={}, allow_redirects=True, timeout=10, proxies={}):
        try:
            r = session.post(url, data=data, headers=headers, allow_redirects=allow_redirects, timeout=timeout, proxies=proxies)
            return r
        except Exception as e:
            # self.log.info(e)
            self.log.info(f'{fg.red}POST request failed, retrying{colors.reset}')
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

    def get_cookies(self, keep_cursor=False):
        try:
            self.log.info(f'Getting cookies {self.country_alpha2} {self.keyword}')
            r = self.rget(f'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country={self.country_alpha2}&media_type=all', proxies=self.get_proxy())
            response_text = r.text
            self.cookies['user_id'] = re.findall('USER_ID\\":\\"(.*?)\\",', response_text)[0]
            self.cookies['lsd'] = re.findall('LSD[^:]+:\\"(.*?)\\"', response_text)[0]
            self.cookies['dtsg'] = re.findall('DTSGInitialData[\\s\\S]+?token\\":\\"(.*?)\\"', response_text)[0]
            self.cookies['sessionid'] = re.findall('sessionId\\":\\"(.*?)\\",', response_text)[0]
            self.cookies['hs'] = re.findall('haste_session\\":\\"(.*?)\\",', response_text)[0]
            self.cookies['hsi'] = re.findall('hsi\\":\\"(.*?)\\",', response_text)[0]
            if not keep_cursor:
                self.cookies['forwardCursor'] = ''
                self.cookies['collationToken'] = ''
            return self.cookies
        except Exception as e:
            # self.log.info(e)
            self.log.info(f'{fg.red}Cookie generation failed for {self.country_alpha2} {self.keyword}, retrying{colors.reset}')
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
        s.headers['Host'] = 'www.facebook.com'
        s.headers['Origin'] = 'https://www.facebook.com'
        s.headers['Referer'] = link
        s.headers['x-fb-lsd'] = self.cookies['lsd']
        s.headers['Sec-Fetch-Site'] = 'same-origin'
        s.headers['Accept'] = '*/*'
        s.headers['Connection'] = 'keep-alive'

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
                'fb_dtsg': self.cookies['dtsg'],
                '__hs': self.cookies['hs'],
                'forward_cursor': self.cookies['forwardCursor'],
                'collation_token': self.cookies['collationToken']
            }
            result = self.rspost(link, s, data=payload, proxies=self.get_proxy()).text.replace('for (;;);', '')
            parsed = json.loads(result)
            try:
                containers = parsed['payload']['results']
            except Exception as e:
                # print(f'parsed: {parsed["errorSummary"]}')
                # print(link)
                self.log.info(f'{fg.red}declined ad {self.country_alpha2} {self.keyword}: {parsed["errorSummary"]}, retrying{colors.reset}')
                # self.log.info(e)
                # self.log.info(parsed)
                self.log.info(link)
                self.log.info(self.get_cookies())
                self.log.info(s.headers)
                self.log.info(payload)
                self.get_cookies(keep_cursor=True)
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
                # ACTIVE ADS CAN BE NONE BUT AD CAN BE ACTIVE: SET TO 1?
                if active_ads and int(active_ads) >= int(self.active_ads) and pageurl != '':
                    check = self.get_product(adid)
                    if check:
                        self.update_product(adid, active_ads, self.country_alpha2, adtitle, self.keyword, pageurl, adsonpage, adurl, creation_time)
                    else:
                        self.create_product(adid, active_ads, self.country_alpha2, adtitle, self.keyword, pageurl, adsonpage, adurl, creation_time)

            time.sleep(self.sleep)
            self.log.info(f'fetching next page')


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
        for filter in filters[:1]:
            for index, keyword in enumerate(filter['keywords'][:4]):
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