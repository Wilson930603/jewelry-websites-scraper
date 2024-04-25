from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import os
from selenium.webdriver.common.by import By as by
import json
import re
from time import sleep
from scrapy import Selector
from selenium.common.exceptions import TimeoutException   
from datetime import datetime
import pandas as pd
from scrapy import Selector
from selenium.webdriver.common.action_chains import ActionChains
from random import randint
from urllib.parse import urlparse, parse_qs
import tldextract
from tqdm import tqdm

def get_driver():
    options = webdriver.ChromeOptions()
    user_data_dir = r"E:\Mubashir\User Data"
    #user_data_dir = r"C:\Users\Atmost\AppData\Local\Google\Chrome\User Data"
    #profile = 'default'
    user_agent = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"
    #options.add_argument(f'user-agent={user_agent}')
    #options.add_argument(f'user-data-dir={user_data_dir}')
    #options.add_argument(f'profile-directory={profile}')
    prefs = {"profile.default_content_setting_values.notifications" : 2}
    #options.add_extension(r'C:\Users\AT-MOST\Downloads\captcha.crx')
    options.add_experimental_option("prefs",prefs)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--host-resolver-rules=MAP www.google-analytics.com 127.0.0.1")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("no-sandbox")
    options.add_argument("--disable-setuid-sandbox")
    options.add_argument("--disable-webgl")
    # PROXY = "83.97.23.90:41203"
    # options.add_argument(f'--proxy-server={PROXY}')
    return webdriver.Chrome(ChromeDriverManager().install(),options=options)

driver = get_driver()

items = {
    'DATE':[],
    'DOMAIN':[],
    'DOMAIN_URL':[],
    'DOMAIN_LOCALE':[],
    'BRAND':[],
    'OWN_PROMOTED_GS':[],
    'MPN':[],
    'SKU':[],
    'SKU_TITLE':[],
    'SKU_SHORT_DESCRIPTION':[],
    'SKU_LINK':[],
    'GTIN8':[],
    'GTIN12':[],
    'GTIN13':[],
    'GTIN14':[],
    'BASE_PRICE':[],
    'SALES_PRICE':[],
    'CURRENCY':[],
    'AVAILABILITY':[],
    'AVAILABILITY_MESSAGE':[],
    'SHIPPING_LEAD_TIME':[],
    'SHIPPING_EXPENSES':[],
    'MARKETPLACE_RETAILER_NAME':[],
    '1_IMAGE_URL':[],
    '2_IMAGE_URL':[],
    '3_IMAGE_URL':[],
    '4_IMAGE_URL':[],
    '5_IMAGE_URL':[],
    '6_IMAGE_URL':[],
    '7_IMAGE_URL':[],
    '8_IMAGE_URL':[],
    'SIZE':[],

}


driver.get('https://www.google.com/search?q=pdpaola&tbm=shop')
def check_currency(price):
    if '€' in price:
        return 'EUR'
    elif '$' in price:
        return 'USD'
    elif '£' in price:
        return 'GBP'
    return ''

def iterate_pannels(driver):
    elements = driver.find_elements(by.XPATH,'//div[contains(@class,"sh-pr__product-results-grid")]//div//h3')
    short_desc = []
    for itr,element in tqdm(enumerate(elements)):
        actions = ActionChains(driver)
        actions.move_to_element(element).perform()
        try:

            element.click()
        except:
            driver.execute_script("arguments[0].click();", element)

        sleep(.25)
        
        actions.move_to_element(element).perform()
        try:

            element.click()
        except:
            driver.execute_script("arguments[0].click();", element)
    return driver

def extract_sku(sku_pattern,text):
    for regu_match in sku_pattern:

        sku_match = re.search(regu_match, text)
        if sku_match:
            return sku_match.group()
    return ''

def filter_numbers(sizes):
    return [item for item in sizes if item.isdigit()]

import re
from time import strftime, gmtime
domain = 'https://www.google.com'
brand_Check = 'pdpaola'
# Define the regular expression to match the SKU pattern
sku_pattern = [
    r'\b[A-Za-z0-9]+-\d{1,4}-[A-Za-z0-9]+\b',
    r'\b[A-Za-z]{2}\d{2}-\d{3}\b',
    r'\b[A-Za-z]{2}\d{2}-\d{3}[-\s]?\d{1,2}\b'
    ]

while(True):
    response = Selector(text=driver.page_source)
    docIds = response.xpath('//div[contains(@class,"sh-pr__product-results-grid")]/div/@data-docid').extract()
    driver = iterate_pannels(driver)
    response = Selector(text=driver.page_source)
    panals = response.xpath('//div[contains(@class,"sh-pr__product-results-grid")]//div//h3')
    for itr,data in enumerate(panals):
        link = data.xpath('../../../..//span[contains(text(),"€")]/../../../../../@href').get(default='')
        sku_title = data.xpath('./text()').get(default='')
        price = data.xpath('../../../..//span[contains(text(),"€")]/text()').extract()
        sale_price = ''
        if price[0] != price[1][:-1]:
            sale_price = price[0]
            price = price[1][:-1]
            if ' ' in price:
                price = price.split()[-1]
        else:
            price = price[0]
        currency = check_currency(price)
        price = price.replace('€','').replace(',','.').strip()
        sale_price = sale_price.replace('€','').replace(',','.').strip()
        brand = data.xpath('../../../..//div[3]/text()').get(default='')
        short_desc = response.xpath(f'//div[contains(@data-docid,"{docIds[itr]}")]//span[contains(@class,"sh-ds__trunc-txt")]/text()').get(default='')
        promoted_gs = 'Yes' if brand_Check in brand else 'No'
        img_list = response.xpath(f'//div[contains(@data-docid,"{docIds[itr]}")]//div[contains(@class,"main-image")]//img/@src').extract()
        sku = extract_sku(sku_pattern, sku_title)
        if sku == '':
            sku = extract_sku(sku_pattern,short_desc)
        
        parsed_url = urlparse(link)
        real_url = parse_qs(parsed_url.query)['url'][0]
        sku_link = real_url
        shipping_expense = data.xpath('../../../..//div[contains(text(),"Envío")]/text()').get(default='').strip()
        if 'delivery' in shipping_expense:
            if '&' in shipping_expense:
                shipping_expense = shipping_expense.split('&')[0].strip()
            elif '·' in shipping_expense:
                shipping_expense = shipping_expense.split('·')[0].strip()
        extracted = tldextract.extract(real_url)
        domain_without_tld = extracted.domain
        domain = domain_without_tld
        domain_url = extracted.registered_domain
        if domain_url=='':
            domain = 'google'
            domain_url = 'google.com'
            sku_link = 'https://www.google.com/'+real_url
        sizes = list(set(filter_numbers(response.xpath(f'//div[contains(@data-docid,"{docIds[itr]}")]//ul[contains(@aria-labelledby,"Otras opciones")]/div/a/text()').extract())))
        if len(sizes) ==0:
            sizes.append('')
        for size in sizes:
            items['DATE'].append(strftime('%Y-%m-%d',gmtime()))
            items['DOMAIN'].append(domain)
            items['DOMAIN_URL'].append(domain_url)
            items['DOMAIN_LOCALE'].append('es')
            items['BRAND'].append(brand_Check)
            items['OWN_PROMOTED_GS'].append(promoted_gs)
            items['MPN'].append('')
            items['SKU'].append(sku)
            items['SKU_TITLE'].append(sku_title)
            items['SKU_SHORT_DESCRIPTION'].append(short_desc)
            items['SKU_LINK'].append(sku_link)
            items['GTIN8'].append('')
            items['GTIN12'].append('')
            items['GTIN13'].append('')
            items['GTIN14'].append('')
            items['BASE_PRICE'].append(price.replace('€',''))
            items['SALES_PRICE'].append(sale_price.replace('€',''))
            items['CURRENCY'].append(currency)
            items['AVAILABILITY'].append('')
            items['AVAILABILITY_MESSAGE'].append('')
            items['SHIPPING_LEAD_TIME'].append('')
            items['SHIPPING_EXPENSES'].append(shipping_expense)
            items['MARKETPLACE_RETAILER_NAME'].append('')

            img_name = '_IMAGE_URL'
            for x in range(8):
                temp_name = f'{x+1}_IMAGE_URL'
                if x<len(img_list):

                    items[temp_name].append(img_list[x])
                else:
                    items[temp_name].append('')

            items['SIZE'].append(size)
    try:
        driver.find_element(by.XPATH,'//span[text()="Siguiente"]').click()
        sleep(2)
    except:
        break

file_name = 'google_store_es.csv'
if not os.path.exists(file_name):
    try:
        pd.DataFrame(items).to_csv(file_name,index=False,encoding='utf-8')
    except:
        pd.DataFrame(items).to_csv(file_name,index=False,encoding='latin-1')
else:
    pd.DataFrame(items).to_csv(file_name,index=False,header=False,mode='a')
