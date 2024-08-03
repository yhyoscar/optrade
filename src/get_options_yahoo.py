import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import random
import datetime
import multiprocessing
from collections import defaultdict
import time
import glob
import os
import urllib3
import argparse

urllib3.disable_warnings()

data_path_template = '{symbol}/{profile|financials|stock|option}/{date}/{time}.csv|.json|.html|.txt'
list_path_template = 'list/{company|etf}.csv'


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
                    print('sampled one header from:', config['headers'], '->', headers)
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
                for fkey, fvalue in config.get('proxies_filter_range', dict()).items():
                    if fkey in df.columns:
                        df = df[fvalue[0] <= df[fkey].astype(float) <= fvalue[1]]
                if len(df) > 0:
                    irow = random.sample(list(df.index.values), 1)[0]
                    proxies = {
                        'http': 'http://'+df.loc[irow, 'ip']+':'+df.loc[irow, 'port'],
                        # 'https': 'https://'+df.loc[irow, 'ip']+':'+df.loc[irow, 'port'],
                    }
                    if verbose:
                        print('sampled one proxy from:', config['proxies'], len(df), '->', proxies)
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
        dt = config.get('sleep', 0)
        if config.get('sleep_random', False):
            dt *= random.random()
        time.sleep(dt)


def get_symbol_list(list_data_dir):
    fns = sorted(glob.glob(list_data_dir+'/nasdaq_screener_*.csv'))
    if fns:
        df = pd.read_csv(fns[-1])
    return df


def save_data(obj, fn):
    # print('saving data to:', fn)
    fdir = os.path.dirname(fn)
    if not os.path.exists(fdir):
        os.makedirs(fdir)
    if fn[-5:].lower() == '.html':
        with open(fn, 'w') as fid:
            fid.write(obj.strip())
    elif fn[-4:].lower() == '.csv':
        obj.to_csv(fn, index=None)
    elif fn[-5:].lower() == '.json':
        with open(fn, 'w') as fid:
            json.dump(obj, fid, indent=2)
    else:
        raise ValueError('Unsupported written file type:', fn)


def fetch_offline_data(data_dir, file_pattern):
    fns = sorted(glob.glob(data_dir+'/'+file_pattern))
    if fns:
        fn = fns[-1]
        # print('Found the latest offline data:', fn)
        if fn[-5:].lower() == '.html':
            with open(fn, 'r') as fid:
                return fid.read()
        elif fn[-4:].lower() == '.csv':
            return pd.read_csv(fn)
        elif fn[-5:].lower() == '.json':
            with open(fn, 'r') as fid:
                return json.load(fid)
        else:
            raise ValueError('Unsupported reading file type:', fn)
    else:
        # print('No offline data:', data_dir, file_pattern)
        return


def get_option(symbol, config, symbol_data_dir, date=None, redo=False):
    url = f'https://finance.yahoo.com/quote/{symbol}/options/'
    if date is None:
        date = datetime.datetime.now().isoformat()[:10]
    dfs = dict()

    fhtmls = sorted(glob.glob(f'{symbol_data_dir}/{symbol}/option/{date}/T*/*.html'))
    if fhtmls:
        res = {'html': fetch_offline_data(os.path.dirname(fhtmls[0]), '*.html')}
    else:
        res = requests_get(url, config)
    if res:
        all_dates = get_option_dates(res['html'])
        if len(all_dates) == 0:
            return 'no option'
        for idate, option_date, dstr in all_dates:
            data_dir = f'{symbol_data_dir}/{symbol}/option/{date}/T{dstr}'
            html = fetch_offline_data(data_dir, '*.html')
            df = fetch_offline_data(data_dir, '*.csv')
            data_version = str(int(time.time()))
            if (html is None) or redo:
                if idate > 0:
                    res = requests_get(f'{url}?date={option_date}', config)
                if res and res['html']:
                    save_data(res['html'], f'{data_dir}/{data_version}.html')
            if (df is None) or redo:
                if res and res['html']:
                    df = parse_option_table(res['html'])
                    if df is not None:
                        dfs[dstr] = df
                        save_data(df, f'{data_dir}/{data_version}.csv')
            else:
                dfs[dstr] = df
    return dfs


def get_option_dates(html):
    soup = BeautifulSoup(html, 'lxml')
    tmp = soup.find('div', {'data-testid': 'options-toolbar'})
    dates = []
    if tmp and len(tmp.find_all('div', {'role': 'listbox'})) == 4:
        for i, d in enumerate(tmp.find('div', {'role': 'listbox'}).find_all('div', {'role': 'option'})):
            tdate = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=int(d["data-value"]))
            dates.append((i, d["data-value"],
                          tdate.isoformat()[:10]))
    return dates


def parse_option_table(html):
    soup = BeautifulSoup(html, 'lxml')
    tables = soup.find('section', {'data-testid': 'options-list-table'})
    if tables:
        tables = tables.find_all('div', class_='tableContainer')
        if tables:
            df1 = defaultdict(list)
            for j, tr in enumerate(tables[0].find_all('tr')):
                if j==0:
                    columns = [td.text.strip() for td in tr.find_all('th')]
                else:
                    for k, td in enumerate(tr.find_all('td')):
                        df1[columns[k]].append(td.text.strip())
            df1 = pd.DataFrame(df1)
            df1['option_type'] = 'call'
        
            df2 = defaultdict(list)
            for j, tr in enumerate(tables[1].find_all('tr')):
                if j==0:
                    columns = [td.text.strip() for td in tr.find_all('th')]
                else:
                    for k, td in enumerate(tr.find_all('td')):
                        df2[columns[k]].append(td.text.strip())
            df2 = pd.DataFrame(df2)
            df2['option_type'] = 'put'
            return pd.concat([df1, df2], axis=0, ignore_index=True)


def main(data_root_dir):
    dflist = get_symbol_list(data_root_dir+'/list').sort_values('Market Cap', ascending=False)
    now_date = datetime.datetime.now().isoformat()[:10]
    for i, symbol in enumerate(dflist['Symbol'].values):
        config = {'verbose': False, 'ntry': 1, 'sleep': 4, 'sleep_random': True, 'timeout': 10,
              'verify': False, 'allow_redirects': False, 
              'headers': '../data/headers.json'}
        dfs = get_option(symbol.replace('/', '-'), config, symbol_data_dir=data_root_dir+'/symbols', date=now_date, redo=False)
        if dfs:
            if isinstance(dfs, str):
                print(f'{i+1}/{len(dflist)}\t{symbol}\t - no option')
            else:
                print(f'{i+1}/{len(dflist)}\t{symbol}\t - success: {len(dfs)}')
        else:
            print(f'{i+1}/{len(dflist)}\t{symbol}\t - fail')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data_root_dir", type=str, required=False, 
        default='/home/alice/data', help="data root directory")
    args = parser.parse_args()

    main(args.data_root_dir)
