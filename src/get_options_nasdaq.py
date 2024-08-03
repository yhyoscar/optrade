import os 
import json 
import time
import glob
import random 
import datetime
import selenium
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support import expected_conditions as EC   
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from collections import Counter
import multiprocessing
import argparse


def setup_driver(proxy=None, headless=True):
    # proxy: https://x.x.x.x:port
    options = webdriver.ChromeOptions()
    # options.binary_location = "/usr/local/bin/chromedriver"
    if headless:
        options.headless = headless
    #options.add_argument("--window-size=1920,1200")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("start-maximized")
    options.add_argument("--disable-blink-features") 
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    # options.add_argument("--remote-debugging-port=9222")
    options.add_argument("disable-infobars")

    if proxy is None:
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # return webdriver.Firefox()
    else:
        prox = Proxy()
        prox.proxy_type = ProxyType.MANUAL
        prox.http_proxy = proxy.split("//")[-1]
        prox.ssl_proxy = proxy.split("//")[-1]
        capabilities = webdriver.DesiredCapabilities.CHROME
        prox.add_to_capabilities(capabilities)
        return webdriver.Chrome(desired_capabilities=capabilities, options=options)


def get_url(driver, symbol, timeout=10, ntry=3):
    # return: -1: fail; 0: no option; 1: has options
    url = f'https://www.nasdaq.com/market-activity/stocks/{symbol.lower()}/option-chain'
    for itry in range(ntry):
        try:
            driver.command_executor.set_timeout(timeout)
            driver.get(url)
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, 600)")
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "jupiter22-options-chain__table")))
            div = driver.find_element(By.CSS_SELECTOR, "div[class*='jupiter22-option-chain__skeleton']")
            if 'Option Chain is currently not available.' in div.text:
                # print(symbol, 'does not have option')
                return 0
            else:
                # print(symbol, 'has options')
                return 1
        except:
            print(f'try({itry+1}/{ntry}) {symbol} failed')
            time.sleep(1)
    return -1


def change_filter(driver, wait_time=7):
    div = driver.find_element(By.CSS_SELECTOR, "div[class*='jupiter22-option-chain-filter-moneyness']")
    label = div.find_element(By.CSS_SELECTOR, "button[class*='jupiter22-option-chain-filter-toggle-moneyness']")
    driver.execute_script("arguments[0].click();", label)
    target = div.find_element(By.CSS_SELECTOR, "button[data-value='all']")
    driver.execute_script("arguments[0].click();", target)
    # time.sleep(wait_time)
    # driver.execute_script("window.scrollTo(0, 600)")

    div = driver.find_element(By.CSS_SELECTOR, "div[class*='jupiter22-option-chain-filter-month']")
    label = div.find_element(By.CSS_SELECTOR, "button[class*='jupiter22-option-chain-filter-toggle-month']")
    driver.execute_script("arguments[0].click();", label)
    target = div.find_element(By.CSS_SELECTOR, "button[data-value='all']")
    driver.execute_script("arguments[0].click();", target)
    time.sleep(wait_time)
    driver.execute_script("window.scrollTo(0, 600)")


def get_all_table(driver, wait_time=0.5):
    # print('page: 1')
    dfs = [parse_table(driver)]
    ipage = 1
    while next_page(driver, wait_time=wait_time):
        ipage += 1
        # print('page:', ipage)
        dfs.append(parse_table(driver))
    return pd.concat(dfs, axis=0, ignore_index=True), len(dfs)


def adjust_exp_date(df, year=2024):
    iyear = year
    dates = []
    for i in df.index.values:
        drow = pd.to_datetime(df.loc[i, 'Exp. Date']+', '+str(iyear))
        if i > 0:
            dpre = pd.to_datetime(df.loc[i-1, 'Exp. Date']+', '+str(iyear))
        else:
            dpre = pd.to_datetime(df.loc[i, 'Exp. Date']+', '+str(iyear))
        if ((drow-dpre).days < 0) and (drow.isoformat()[:10] not in dates):
            iyear += 1
            # print(i, dpre, drow)
        dstr = pd.to_datetime(df.loc[i, 'Exp. Date']+', '+str(iyear)).isoformat()[:10]
        df.loc[i, 'exp_date'] = dstr
        if dstr not in dates:
            dates.append(dstr)
    return df


def parse_table(driver, max_try=5):
    for itry in range(max_try):
        try:
            soup = BeautifulSoup(driver.page_source, 'lxml')
            table = soup.find('table', class_='jupiter22-options-chain__table')
            rows = table.thead.findChildren('tr', recursive=False)
            if len(rows) == 2:
                cols1 = []
                for cell in rows[0].findChildren('th', recursive=False):
                    if cell.text.strip():
                        col = cell.text.strip()
                    cols1.append(col)
                cols2 = [cell.text.strip() for cell in rows[1].findChildren('th', recursive=False)]
                cols = []
                for x, y in zip(cols1, cols2):
                    if y in ['Exp. Date', 'Strike']:
                        cols.append(y)
                    else:
                        cols.append(x+'_'+y)
                df = []
                for row in table.tbody.findChildren('tr', recursive=False):
                    data = [cell.text.strip() for cell in row.findChildren('td', recursive=False)
                            if cell.text.strip() and
                            ('jupiter22-options-chain__cell--c_colour' not in cell['class']) and
                            ('jupiter22-options-chain__cell--p_colour' not in cell['class'])]
                    # print(data, len(data), len(cols))
                    if len(data) == len(cols):
                        df.append(data)
                return pd.DataFrame(df, columns=cols)
        except:
            time.sleep(1)


def next_page(driver, wait_time=0):
    button = driver.find_element(By.CLASS_NAME, "pagination__next")
    if button.get_attribute("disabled"):
        return False
    else:
        driver.execute_script("arguments[0].click();", button)
        time.sleep(wait_time)
        driver.execute_script("window.scrollTo(0, 600)")
        # WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "jupiter22-options-chain__table")))
        return True


def get_multi_symbols(symbols, data_root_dir, filter_wait_time):
    dstr = datetime.datetime.now().isoformat()[:10]    
    driver = setup_driver(proxy=None, headless=True)
    results = []
    for symbol in symbols:
        results.append(get_one_symbol(symbol, f'{data_root_dir}/symbols/{symbol}/option/{dstr}/all', filter_wait_time, driver))
    return results


def get_one_symbol(symbol, save_dir, filter_wait_time=7, driver=None):
    # return: -1: failed; 0: no option; 1: get options online; 2: already have csv offline 
    if not os.path.exists(save_dir):
        os.makedirs(save_dir, exist_ok=True)
        time.sleep(0.1)
    fns = sorted(glob.glob(save_dir+'/*.csv'))
    if fns:
        df = pd.read_csv(fns[-1])
        exp_dates = df.groupby('exp_date').first().index.values
        print(f'{datetime.datetime.now().isoformat()} - {symbol} - local - {len(df)} entries - {len(exp_dates)} exp_dates: {exp_dates[0]} -> {exp_dates[-1]}')
        return 2
    try:
        fsave = f'{save_dir}/{int(time.time())}.csv'
        if driver is None:
            driver = setup_driver(proxy=None, headless=True)
        flag = get_url(driver, symbol)
        if flag > 0:
            change_filter(driver, wait_time=filter_wait_time)
            df, npage = get_all_table(driver, wait_time=0)
            df = adjust_exp_date(df)
            df.to_csv(fsave, index=None)
            exp_dates = df.groupby('exp_date').first().index.values
            print(f'{datetime.datetime.now().isoformat()} - {symbol} - web - {len(df)} entries - {npage} pages - {len(exp_dates)} exp_dates: {exp_dates[0]} -> {exp_dates[-1]}')
            if driver is None:
                driver.close()
            return 1
        elif flag == 0:
            print(f'{datetime.datetime.now().isoformat()} - {symbol} - no option')
            if driver is None:
                driver.close()
            return 0
        else:
            print(f'{datetime.datetime.now().isoformat()} - {symbol} - fail')
            if driver is None:
                driver.close()
            return -1            
    except:
        print(f'{datetime.datetime.now().isoformat()} - {symbol} - fail')
        return -1
    

def get_symbol_list(list_data_dir):
    fns = sorted(glob.glob(list_data_dir+'/nasdaq_screener_*.csv'))
    df = pd.read_csv(fns[-1]).sort_values('Market Cap', ascending=False)
    return [x.replace('/', '-') for x in df['Symbol'].values if isinstance(x, str)]


def main(data_root_dir):
    symbols = get_symbol_list(f'{data_root_dir}/list')
    filter_wait_time = 7
    nprocess = 12
    arglist = [(symbols[i::nprocess], data_root_dir, filter_wait_time) for i in range(nprocess)]
    with multiprocessing.Pool(processes=nprocess) as pool:
        results = pool.starmap(get_multi_symbols, arglist)

    dstr = datetime.datetime.now().isoformat()[:10]
    # arglist = [(s, f'{data_root_dir}/symbols/{s}/option/{dstr}/all', filter_wait_time, None) for s in symbols]
    # with multiprocessing.Pool(processes=nprocess) as pool:
    #     results = pool.starmap(get_one_symbol, arglist)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--data_root_dir", type=str, required=False, 
        default='/home/alice/data', help="data root directory")
    args = parser.parse_args()

    main(args.data_root_dir)


