import requests
import random
import json 
import re
import os 
import base64
import time 
import glob 
import pandas as pd 
import asyncio
import multiprocessing
import datetime
import argparse
import urllib3
from bs4 import BeautifulSoup
from collections import Counter
from proxybroker import Broker

urllib3.disable_warnings()


def get_proxy_source1(config):
    url = 'https://www.sslproxies.org/'
    res = requests_get(url, config)
    if res:
        soup = BeautifulSoup(res['html'], 'html')
        table = soup.find('table', class_="table table-striped table-bordered")
        df = []
        if table is not None:
            cols = ['ip', 'port', 'code', 'country', 'level', 'google', 'https', 'last_checked']
            tbody = table.find('tbody')
            for tr in tbody.find_all('tr'):
                obj = dict(zip(cols, [td.text.strip() for td in tr.find_all('td')]))
                obj['level'] = obj['level'].split()[0]
                if obj['https'] == 'yes':
                    obj['https'] = True
                else:
                    obj['https'] = False
                df.append(obj)
        if df:
            df = pd.DataFrame(df)[['ip', 'port', 'country', 'level', 'https']]
            df['created'] = datetime.datetime.now().isoformat()[:10]
            return df


def get_proxy_source2(config):
    urls1 = [
        'http://free-proxy.cz/en/proxylist/country/all/http/ping/level1',
        'http://free-proxy.cz/en/proxylist/country/all/http/ping/level2',
        'http://free-proxy.cz/en/proxylist/country/all/https/ping/level1',
        'http://free-proxy.cz/en/proxylist/country/all/https/ping/level2',
    ]
    urls2 = [url+'/'+i for url in urls1 for i in '2345']
    df = []
    for url in urls1+urls2:
        res = requests_get(url, config)
        if res:
            soup = BeautifulSoup(res['html'], 'html')
            table = soup.find('table', {'id': 'proxy_list'})
            if table:
                cols = ['ip', 'port', 'https', 'country', 'region', 'city', 'level', 'speed', 'uptime', 'response', 'last_checked']
                tbody = table.find('tbody')
                for tr in tbody.find_all('tr'):
                    obj = dict(zip(cols, [td.text.strip() for td in tr.find_all('td')]))
                    if obj['level'] == 'High anonymity':
                        obj['level'] = 'elite'
                    else:
                        obj['level'] = 'anonymous'
                    if obj['https'] == 'HTTPS':
                        obj['https'] = True
                    else:
                        obj['https'] = False
                    df.append(obj)
    if df:
        df = pd.DataFrame(df)[['ip', 'port', 'country', 'level', 'https']]
        df['created'] = datetime.datetime.now().isoformat()[:10]
        return df


def get_proxy_source3(config):
    url = 'https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&proxy_format=protocolipport&format=text'
    res = requests_get(url, config)
    if res:
        df = []
        for line in res['html'].split():
            if line.strip():
                obj = {'ip': line.split('//')[-1].split(':')[0],
                       'port': line.split('//')[-1].split(':')[1]}
                if line.split(':')[0] == 'http':
                    obj['https'] = False
                elif line.split(':')[0] == 'https':
                    obj['https'] = True
                else:
                    continue
                df.append(obj)
        df = pd.DataFrame(df)
        df['created'] = datetime.datetime.now().isoformat()[:10]
        return df


def requests_get(url, config=dict()):
    ntry = config.get('ntry', 1)
    verbose = config.get('verbose', False)
    for itry in range(ntry):
        if verbose:
            print(f'get({itry+1}/{ntry}):', url)
        if config.get('headers', None):
            if isinstance(config['headers'], str):
                headers = random.sample(json.load(open(config['headers'], 'r')), 1)[0]
                if verbose:
                    print('sampled one header from:', config['headers'])
            else:
                headers = config['headers']
        else:
            headers = None
        if config.get('proxies', None):
            if isinstance(config['proxies'], str):
                df = pd.read_csv(config['proxies'], dtype=str)
                for fkey, fvalue in config.get('proxies_filter', dict()).items():
                    if fkey in df.columns:
                        df = df[df[fkey].isin(fvalue)]
                if len(df) > 0:
                    irow = random.sample(list(df.index.values), 1)[0]
                    proxies = {
                        'http': 'http://'+df.loc[irow, 'ip']+':'+df.loc[irow, 'port'],
                        'https': 'https://'+df.loc[irow, 'ip']+':'+df.loc[irow, 'port'],
                    }
                    if verbose:
                        print('sampled one proxy from:', config['proxies'], '->', proxies)
                else:
                    print('No proxy meet the requirements:', config['proxies'])
                    proxies = None
            else:
                proxies = config['proxies']
        else:
            proxies = None
        try:
            t0 = time.time()
            res = requests.get(url,
                               headers=headers,
                               proxies=proxies,
                               timeout=config.get('timeout', 30), 
                               allow_redirects=config.get('allow_redirects', False),
                               verify=config.get('verify', True))
            latency = time.time()-t0
            if res.status_code == 200:
                if verbose:
                    print('Success in getting:', url, 'latency:', latency)
                return {'html': res.text, 'latency': latency}
            else:
                if verbose:
                    print(f'Abnormal status code ({res.status_code}) in getting({itry+1}/{ntry}):', url)
        except:
            if verbose:
                print(f'Failed in getting({itry+1}/{ntry}):', url)
        time.sleep(config.get('sleep', 0))


def get_option_dates(html):
    soup = BeautifulSoup(html, 'html')
    tmp = soup.find('div', {'data-testid': 'options-toolbar'})
    dates = []
    if tmp:
        print('optiontable')
        for d in tmp.find('div', {'role': 'listbox'}).find_all('div', {'role': 'option'}):
            tdate = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=int(d["data-value"]))
            dates.append((d["data-value"],
                          tdate.isoformat()[:10]))
    return dates


def main(sources=[get_proxy_source1, get_proxy_source3],
         dir_source='/tmp', fsave=None, nprocess=32):
    url = 'http://www.aphanti.com/myip'
    # url = 'https://www.hforganic.com/about/'
    # url = 'http://finance.yahoo.com/'
    config = {'verbose': True, 'ntry': 1, 'sleep': 0, 'timeout': 10,
              'verify': False, 'allow_redirects': True, 
              'headers': random.sample(json.load(open('../data/headers.json', 'r')), 1)[0],}    
    dfs = []
    for fn in glob.glob(dir_source+'/*.csv'):
        dfs.append(pd.read_csv(fn, dtype=str))

    for source in sources:
        df = source(config)
        fn = dir_source+'/'+source.__name__+'_'+datetime.datetime.now().isoformat()[:19]+'.csv'
        if df is not None:
            print(source.__name__, 'save', len(df), 'proxies to', fn)
            df.to_csv(fn, index=None)
            dfs.append(df)

    if dfs:
        df = pd.concat(dfs, axis=0, ignore_index=True)
        df = df.drop_duplicates(subset=['ip', 'port'])
        print('Total pool:', len(df))
        arglist = [(url, {**config, **{'proxies': {'http': 'http://'+df.loc[i, 'ip']+':'+df.loc[i, 'port']}}}) for i in df.index.values]
        with multiprocessing.Pool(processes=nprocess) as pool:
            results = pool.starmap(requests_get, arglist)
        df['latency'] = [x['latency'] if x else 1e9 for x in results]
        df['valid'] = [True if x else False for x in results]
        df['checked'] = datetime.datetime.now().isoformat()[:19]
    print('total:', len(df), 'valid:', Counter(df['valid'].values))
    print(df.loc[df['valid']==True, ['valid', 'latency']].describe())
    if fsave:
        os.makedirs(os.path.dirname(fsave), exist_ok=True)
        with open(fsave, 'w') as fid:
            df.to_csv(fsave, index=None)
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--sources", type=str, required=False, 
        default='1,3', help="proxies sources")
    parser.add_argument("-d", "--directory", type=str, required=True, 
        default='/tmp', help="sources directory")
    parser.add_argument("-c", "--save_csv", type=str, required=True, 
        default='/tmp/checked.csv', help="save to csv file")
    parser.add_argument("-n", "--nprocess", type=int, required=False, 
        default=64, help="multiple processes")
    args = parser.parse_args()

    sources = []
    if '1' in args.sources.split(','):
        sources.append(get_proxy_source1)
    if '2' in args.sources.split(','):
        sources.append(get_proxy_source2)
    if '3' in args.sources.split(','):
        sources.append(get_proxy_source3)
    main(sources, args.directory, args.save_csv, args.nprocess)
