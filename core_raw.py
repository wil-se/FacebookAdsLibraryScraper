# Source Generated with Decompyle++
# File: core.pyc (Python 3.10)

import re
import random
import threading
from settings import *
from speedproxies import *
from database import *
from tkinter.messagebox import messagebox as tkMessageBox
import logging.handlers as logging
from pathlib import Path
Path('logs').mkdir(True, True, **('parents', 'exist_ok'))
logger = logging.getLogger(__name__)
FORMAT = '[%(asctime)s %(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s'
logging.basicConfig(FORMAT, logging.INFO, **('format', 'level'))
logfile = time.strftime('logs/main_%Y-%m-%d.log')
handler = logging.handlers.RotatingFileHandler(logfile, 1000000, 10, **('maxBytes', 'backupCount'))
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)
timeout = time.time() + 1800
all_proxies = formatted()
start_link = 'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=NL&media_type=all'
link = 'https://www.facebook.com/ads/library/async/search_ads/'

def get_filters():
    conn = sqlite3.connect('found.db')
    filters_data = conn.execute('Select * from filters').fetchall()
    filters = (lambda .0 = None: for fil in .0:
passcontinue{
'active_ads': fil[1],
'date_from': fil[2],
'date_to': fil[3],
'country': fil[4],
'keywords': fil[5].split(',') }[''])(filters_data)
    conn.close()
    return filters


def get_cookies(params, payload):
    global all_proxies
    proxy = random.choice(all_proxies)
    
    try:
        logger.info('Generating new cookies session')
        r = requests.get(start_link, proxy, **('proxies',))
        response_text = r.text
        user_id = re.findall('USER_ID\\":\\"(.*?)\\",', response_text)[0]
        lsd = re.findall('LSD[^:]+:\\"(.*?)\\"', response_text)[0]
        dtsg = re.findall('DTSGInitialData[\\s\\S]+?token\\":\\"(.*?)\\"', response_text)[0]
        sessionid = re.findall('sessionId\\":\\"(.*?)\\",', response_text)[0]
        hs = re.findall('haste_session\\":\\"(.*?)\\",', response_text)[0]
        hsi = re.findall('hsi\\":\\"(.*?)\\",', response_text)[0]
        c = {
            'dpr': '1.25',
            'wd': '498x731' }
        s = requests.Session()
        s.headers['User-Agent'] = ua.Chrome
        s.headers['Content-Type'] = 'application/x-www-form-urlencoded'
        s.headers['Host'] = 'web.facebook.com'
        s.headers['Origin'] = 'https://web.facebook.com'
        s.headers['Referer'] = 'https://www.facebook.com/ads/library/?active_status=all&ad_type=all&country=NL&q=gratis%20verzending&sort_data[direction]=desc&sort_data[mode]=relevancy_monthly_grouped&search_type=keyword_unordered&media_type=all'
        s.headers['x-fb-lsd'] = lsd
        payload['__user'] = user_id
        payload['lsd'] = lsd
        payload['fb_dtsg'] = dtsg
        payload['__hs'] = hs
        payload['__hsi'] = hsi
        params['session_id'] = sessionid
    finally:
        return None
        if len(all_proxies) == 1:
            all_proxies = all_proxies + formatted()
        all_proxies.remove(proxy)
        logger.info('Cookies generation failed,trying again')
        return None



def get_response(params, payload, s, proxy, c, filter, retry = (1,)):
    
    try:
        time.sleep(random.randint(5, 15))
        response = s.post(link, params, payload, c, 6, proxy, **('params', 'data', 'cookies', 'timeout', 'proxies'))
    finally:
        return None
        if retry <= 3:
            logger.info(f'''Retrying another proxy: {retry}''')
            return None
        (params, payload, s, proxy, c) = None(params, payload)
        return None



def get_content(response, params, payload, s, proxy, c, filter):
    if time.time() < timeout:
        containers = json.loads(response.text.replace('for (;;);', ''))
        if len(containers['payload']['results']):
            for container in containers['payload']['results']:
                for item in container:
                    loop_item(item, params, filter)
                params['forward_cursor'] = containers['payload']['forwardCursor']
                params['collation_token'] = containers['payload']['collationToken']
                logger.info('Getting next page')
                return get_response(params, payload, s, proxy, c, filter)
                return None
                return None
                return None


def loop_item(item, params, filter):
    if isinstance(item, list):
        for i in item:
            loop_item(i, filter)
        return None
    if None(item['collationCount'], int):
        adcount_int = int(item['collationCount'])
        if adcount_int >= int(filter.get('active_ads')):
            logger.info(f'''[{item['adArchiveID']} ===> {item['collationCount']}]''')
            data = get_data(item, params['countries[0]'], str(item['collationCount']), params['q'])
            if data != None:
                logger.info('Sending ad to database')
                send_data_to_db(data, logger)
                return None
            None.info(f'''Declined ad [{item['adArchiveID']} ==> {item['collationCount']}]''')
            return None
        return None
    None.info(f'''Declined ad [{item['adArchiveID']} ==> {item['collationCount']}]''')

filters_list = get_filters()
# WARNING: Decompyle incomplete
