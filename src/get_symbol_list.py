import os 
import json 
import time
import random 
import datetime
import argparse
import glob
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


# function to take care of downloading file
def enable_download_headless(browser, download_dir):
    browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd':'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    browser.execute("send_command", params)


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
    # options.add_experimental_option("prefs", {
    #     "download.default_directory": "/home/alice/data",
    #     "download.prompt_for_download": False,
    #     "download.directory_upgrade": True,
    #     "safebrowsing_for_trusted_sources_enabled": False,
    #     "safebrowsing.enabled": False
    # })

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


def main(download_dir, tmp_dir='/tmp/nasdaq', headless=True):
    os.makedirs(download_dir, exist_ok=True)
    os.system('rm -rf '+tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    url = 'https://www.nasdaq.com/market-activity/stocks/screener'
    driver = setup_driver(proxy=None, headless=headless)
    enable_download_headless(driver, tmp_dir)
    
    driver.get(url)
    time.sleep(1)
    driver.execute_script("window.scrollTo(0, 300)")
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "nasdaq-screener__download")))
    div = driver.find_element(By.CLASS_NAME, "nasdaq-screener__form-button--download")
    driver.execute_script("arguments[0].click();", div)
    time.sleep(5)

    fns = glob.glob(tmp_dir+'/*.csv')
    if fns:
        df = pd.read_csv(fns[0])
        print('Successful downloaded', os.path.basename(fns[0]), f'({len(df)}) to', download_dir)
        os.system('cp '+fns[0]+' '+download_dir)
    else:
        raise ValueError('Failed in downloading company list from nasdap!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--download_dir", type=str, required=True, 
        default='/tmp', help="download directory")
    parser.add_argument("-t", "--tmp_dir", type=str, required=False, 
        default='/tmp/nasdaq', help="temporary directory")
    parser.add_argument("-l", "--headless", type=bool, required=False, 
        default=True, help="healess")
    args = parser.parse_args()

    main(args.download_dir, args.tmp_dir, args.headless)
