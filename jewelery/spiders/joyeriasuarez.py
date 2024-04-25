from scrapy import Request, Spider, Selector
from time import strftime, gmtime
import json
import sys, pandas as pd
from webcolors import CSS3_NAMES_TO_HEX, HTML4_NAMES_TO_HEX, CSS21_NAMES_TO_HEX
import tldextract
from random import randint
from bs4 import BeautifulSoup
from ..items import new_fields
import datetime
def extract_domain_domainUrl(real_url):
    extracted = tldextract.extract(real_url)
    domain_without_tld = extracted.domain
    domain = domain_without_tld
    domain_url = extracted.registered_domain
    return domain, domain_url
def check_currency(price):
    if '€' in price:
        return 'EUR'
    elif '$' in price:
        return 'USD'
    elif '£' in price:
        return 'GBP'
    return ''
class Joyeriasuarez(Spider):
    name = "suarez"
    base_url = "https://www.joyeriasuarez.com"
    start_urls = ["https://www.joyeriasuarez.com/es/en_US/Home"]

    # handle_httpstatus_list = [400]
    download_delay = 0.2

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/100.0.100.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/604.1 Edg/100.0.100.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edge/18.19042",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edge/18.19042",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/65.0.3467.48",
        "Mozilla/5.0 (X11; CrOS x86_64 10066.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36",
    ]
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "user-agent": user_agents[randint(0, len(user_agents) - 1)],
    }
    joyerisasurez_fields = [
        "DATE",
        "DOMAIN",
        "DOMAIN_URL",
        "DOMAIN_LOCALE",
        "CATEGORY_1",
        "CATEGORY_2",
        "CATEGORY_3",
        "COLLECTION_NAME",
        "SEASON",
        "BRAND",
        "PRODUCT_BADGE",
        "MANUFACTURER",
        "AUDIENCE_GENDER",
        "MPN",
        "SKU",
        "SKU_TITLE",
        "SKU_SHORT_DESCRIPTION",
        "SKU_LONG_DESCRIPTION",
        "SKU_LINK",
        "GTIN8",
        "GTIN12",
        "GTIN13",
        "GTIN14",
        "BASE_PRICE",
        "SALES_PRICE",
        "SALES_PRICE_STARTDATE",
        "SALES_PRICE_ENDDATE",
        "CURRENCY",
        "AVAILABILITY",
        "CONDITION",
        "SKU_COLOR",
        "1_IMAGE_URL",
        "2_IMAGE_URL",
        "3_IMAGE_URL",
        "4_IMAGE_URL",
        "5_IMAGE_URL",
        "6_IMAGE_URL",
        "7_IMAGE_URL",
        "8_IMAGE_URL",
        "MAIN_MATERIAL",
        "MATERIAL_FINISHING",
        "MATERIAL_RECYCLED",
        "SECONDARY_MATERIAL",
        "SIZE",
        "SIZE_TYPE",
        "SIZE_SYSTEM",
        "SIZE_AGEGROUP",
        "SIZE_SUGGESTED_AGE",
        "PRODUCT_HEIGHT",
        "PRODUCT_WIDTH",
        "PRODUCCT_LENGTH",
        "PRODUCCT_THICKNESS",
        "PRODUCT_OUTTER_DIAMETER",
        "PRODUCT_INNER_DIAMETER",
        "PRODUCT_WEIGHT",
        "REVIEWS_RATING_VALUE",
        "REVIEWS_NUMBER",
        "MANUFACTURING_TECHNIQUE",
        "MANUFACTURING_ORIGIN",
        "RAW_METAL",
        "RAW_METAL_WEIGHT",
        "RAW_MATERIAL_STAMP",
        "RAW_MATERIAL_PURITY",
        "PLATING_METAL",
        "PLATING_METAL_STAMP",
        "JEWELRY_SETTING_STYLE",
        "PRODUCT_SHAPE_MOTIFF",
        "JEWEL_ENGRAVABLE",
        "JEWEL_INSCRIPTION",
        "TOTAL_GEMSTONES_WEIGHT",
        "TOTAL_GEMSTONES_WEIGHT_SYSTEM",
        "GEMSTONE_1_NAME",
        "GEMSTONE_1_SETTING",
        "GEMSTONE_1_CATEGORY",
        "GEMSTONE_1_COLOR",
        "GEMSTONE_1_NUMBER",
        "GEMSTONE_1_SETTING",
        "GEMSTONE_1_WEIGHT",
        "GEMSTONE_1_CUT",
        "GEMSTONE_1_CREATION_METHOD",
        "GEMSTONE_1_TREATMENT",
        "GEMSTONE_1_CLARITY",
        "GEMSTONE_1_HEIGHT",
        "GEMSTONE_1_LENGTH",
        "GEMSTONE_1_WIDTH",
        "GEMSTONE_2_NAME",
        "GEMSTONE_2_SETTING",
        "GEMSTONE_2_CATEGORY",
        "GEMSTONE_2_COLOR",
        "GEMSTONE_2_NUMBER",
        "GEMSTONE_2_SETTING",
        "GEMSTONE_2_WEIGHT",
        "GEMSTONE_2_CUT",
        "GEMSTONE_2_CREATION_METHOD",
        "GEMSTONE_2_TREATMENT",
        "GEMSTONE_2_CLARITY",
        "GEMSTONE_2_HEIGHT",
        "GEMSTONE_2_LENGTH",
        "GEMSTONE_2_WIDTH",
        "GEMSTONE_3_NAME",
        "GEMSTONE_3_CATEGORY",
        "GEMSTONE_3_COLOR",
        "GEMSTONE_3_NUMBER",
        "GEMSTONE_3_SETTING",
        "GEMSTONE_3_WEIGHT",
        "GEMSTONE_3_CUT",
        "GEMSTONE_3_CREATION_METHOD",
        "GEMSTONE_3_TREATMENT",
        "GEMSTONE_3_CLARITY",
        "GEMSTONE_3_HEIGHT",
        "GEMSTONE_3_LENGTH",
        "GEMSTONE_3_WIDTH",
        "GEMSTONE_4_NAME",
        "GEMSTONE_4_SETTING",
        "GEMSTONE_4_CATEGORY",
        "GEMSTONE_4_COLOR",
        "GEMSTONE_4_NUMBER",
        "GEMSTONE_4_WEIGHT",
        "GEMSTONE_4_CUT",
        "GEMSTONE_4_CREATION_METHOD",
        "GEMSTONE_4_TREATMENT",
        "GEMSTONE_4_CLARITY",
        "GEMSTONE_4_HEIGHT",
        "GEMSTONE_4_LENGTH",
        "GEMSTONE_4_WIDTH",
        "GEMSTONE_5_NAME",
        "GEMSTONE_5_SETTING",
        "GEMSTONE_5_CATEGORY",
        "GEMSTONE_5_COLOR",
        "GEMSTONE_5_NUMBER",
        "GEMSTONE_5_WEIGHT",
        "GEMSTONE_5_CUT",
        "GEMSTONE_5_CREATION_METHOD",
        "GEMSTONE_5_TREATMENT",
        "GEMSTONE_5_CLARITY",
        "GEMSTONE_5_HEIGHT",
        "GEMSTONE_5_LENGTH",
        "GEMSTONE_5_WIDTH",
        "CHAIN_LENGTH",
        "CHAIN_WIDTH",
        "CHAIN_THICKNESS",
        "CHAIN_ADJUSTABLE",
        "EARRING_DROP_LENGTH",
        "EARRING_SOLD_UNITS",
        "CLASP_TYPE",
        "BAIL_SIZE",
        "STANDARD_FEATURES",
    ]
    joyerisasurez_fields2= [
        'DATE',
        'DOMAIN',
        'DOMAIN_URL',
        'DOMAIN_COUNTRY_CODE',
        'COLLECTION_NAME',
        'SEASON',
        'BRAND',
        'PRODUCT_BADGE',
        'MANUFACTURER',
        'MPN',
        'SKU',
        'SKU_TITLE',
        'SKU_SHORT_DESCRIPTION',
        'SKU_LONG_DESCRIPTION',
        'SKU_LINK',
        'GTIN8',
        'GTIN12',
        'GTIN13',
        'GTIN14',
        'BASE_PRICE',
        'SALES_PRICE',
        'ACTIVE_PRICE',
        'CURRENCY',
        'AVAILABILITY',
        'AVAILABILITY_MESSAGE',
        'SHIPPING_LEAD_TIME',
        'SHIPPING_EXPENSES',
        'MARKETPLACE_RETAILER_NAME',
        'CONDITION_PRODUCT',
        'SKU_COLOR',
        'REVIEWS_RATING_VALUE',
        'REVIEWS_NUMBER',
        'IMAGE_URL_1',
        'IMAGE_URL_2',
        'IMAGE_URL_3',
        'IMAGE_URL_4',
        'IMAGE_URL_5',
        'IMAGE_URL_6',
        'IMAGE_URL_7',
        'IMAGE_URL_8',
        'SIZE_DEFINITION',
        'SIZE_CODE',
        'AUDIENCE_GENDER',
        'SIZE_AGEGROUP',
        'SIZE_SUGGESTED_AGE',
        'SIZE_DIMENSIONS',
        'PRODUCT_HEIGHT',
        'PRODUCT_WIDTH',
        'PRODUCT_LENGTH',
        'PRODUCT_THICKNESS',
        'PRODUCT_OUTTER_DIAMETER',
        'PRODUCT_INNER_DIAMETER',
        'PRODUCT_WEIGHT',
        'MAIN_MATERIAL',
        'SECONDARY_MATERIAL',
        'SPECIAL_PRICES',
        'NEW_IN',
        'BEST_SELLERS',
        'IMAGE_SAS_URL_1',
        'IMAGE_SAS_URL_2',
        'IMAGE_SAS_URL_3',
        'IMAGE_SAS_URL_4',
        'IMAGE_SAS_URL_5',
        'IMAGE_SAS_URL_6',
        'IMAGE_SAS_URL_7',
        'IMAGE_SAS_URL_8',
    ]
    custom_settings = {
        "FEED_EXPORT_FIELDS": 
        joyerisasurez_fields2, "CONCURRENT_REQUESTS": 8,
        'ITEM_PIPELINES': {
            'jewelery.pipelines.JeweleryPipeline': 300,
        }
        }

    def start_requests(self):

        yield Request(self.start_urls[0], headers=self.headers, callback=self.main_page)

    def main_page(self,response):

        category_url = response.xpath('//div[contains(@class,"categories")]//a/@href').extract() #Remove duplicates
        paginate = '?start={offset}&sz={size}&format=page-element'
        for url in category_url:
            yield Request(url+paginate.format(offset=0,size=12),callback=self.pagination, headers=self.headers, meta = {"offset":0,"size":12,"url":url})
            #break
    
    def pagination(self,response):
        url = response.meta.get("url")
        offset = response.meta.get("offset")
        size = response.meta.get("size")
        paginate = '?start={offset}&sz={size}&format=page-element'

        names = response.xpath('//a[@class="name-link font_slider_description"]/text()').extract()
        nameslinks = [self.base_url+cat_url for cat_url in response.xpath('//a[@class="name-link font_slider_description"]/@href').extract()]
        for name in nameslinks:
            yield Request(name,callback=self.product_page,headers=self.headers)
            break
        if len(names)==0:
            print(len(names))
            print('Finished')
        else:
            if len(names)<12:
                size = 1
            print(url+paginate.format(offset=offset+size,size=size))
            print(len(names))
            yield Request(url+paginate.format(offset=offset+size,size=size),callback=self.pagination,headers=self.headers,meta={"offset":offset+size,"size":12,"url":url})
    
    def find_region(self, value):
        regions = {
            "uk": "United Kingdom",
            "/es/": "es",
            ".es": "es",
            "com": "United States",
        }
        for region in regions.keys():
            if region in value:
                return regions[region]
    def product_page(self,response):

        script_breadCrumbs = response.xpath('//div[@class="header"]//script[@type="application/ld+json"]/text()').get()
        main_script = response.xpath('//div[@class="pdp-main"]//script[@type="application/ld+json"]/text()').get()
        print(f'Script:{main_script}')
        main_json = json.loads(main_script)
        category = []
        bread_crumbs = json.loads(script_breadCrumbs)
        sku_title = main_json.get("name","")
        if bread_crumbs.get("itemListElement"):
            category = [cat.get("name") for cat in bread_crumbs["itemListElement"]]
        else:
            category = []
        description = main_json.get("description","")
        # Create BeautifulSoup object and extract text
        if description is not None:
            soup = BeautifulSoup(description, 'html.parser')
            description = soup.get_text()
        else:
            description = ""
        carat_weight = self.get_carat_weight(description)
        cut = self.get_cut(description)
        brand = main_json.get("brand",{}).get("name","")
        if brand == "Suarez":
            brand = "joyeriasuarez"
        SKU = main_json.get("sku")
        price = main_json.get("offers",{}).get("price","")
        currency = main_json.get("offers",{}).get("priceCurrency","")
        itemCondition = main_json.get("offers",{}).get("itemCondition","").split('/')[-1]
        available = main_json.get("offers",{}).get("availability","").split('/')[-1]
        available_msg = available
        if "InStock" in available:
            available = "Yes"
        else:
            available = "No"

        shipping_cost = response.xpath('//p[contains(text(),"SHIPPING")]/text()').get(default='').replace('SHIPPING','').strip()
        if shipping_cost !='':
            shipping_cost = shipping_cost.split('€')[0].strip()
        gtin = main_json.get("gtin","")
        images = main_json.get("image",[])

        engraving = response.xpath('//label[@for="engraving-activated"]/text()').get()
        sizes = [x.strip() for x in response.xpath('//select[@id="va-sizesSua"]/option//text()').extract()]
        
        items = {}
        imageName = "IMAGE_URL_"
        for itr, img in enumerate(images):
            if itr == 8:
                break
            newName = imageName+str(itr + 1) 
            items[newName] = img

        if len(gtin) == 8:
            items["GTIN8"] = gtin
        elif len(gtin) == 12:
            items["GTIN12"] = gtin
        elif len(gtin) == 13:
            items["GTIN13"] = gtin
        elif len(gtin) == 14:
            items["GTIN14"] = gtin

        items["SHIPPING_EXPENSES"] = shipping_cost
        items["CONDITION_PRODUCT"] = itemCondition
        domain, domain_url = extract_domain_domainUrl(response.url)

        items["DATE"] = datetime.datetime.now()
        items["DOMAIN"] = domain
        items["DOMAIN_URL"] = domain_url
        items['DOMAIN_COUNTRY_CODE'] = self.get_domain_country_code(response.url)
        items["BRAND"] = brand
        items["MANUFACTURER"] = "joyeriasuarez"
        items["SKU"] = SKU
        items["SKU_TITLE"] = sku_title
        items["SKU_SHORT_DESCRIPTION"] = description
        items["BASE_PRICE"] = price
        items["ACTIVE_PRICE"] = price
        items["CURRENCY"] = currency

        items["AVAILABILITY"] = available
        items["AVAILABILITY_MESSAGE"] = available_msg
        items["SKU_LINK"] = response.url


        ### LOGIC PROVIDED IN FORM OF SUDO CODE

        if "vermeil" in description.lower():
            items["MAIN_MATERIAL"] = "Silver"
            items["SECONDARY_MATERIAL"] = "Gold"
           
        if "gold plated" in description.lower() and "silver" in description.lower():
            items["MAIN_MATERIAL"] = "Silver"
            items["SECONDARY_MATERIAL"] = "Gold"
        if "gold plated" in description.lower() and "brass" in description.lower():
            items["MAIN_MATERIAL"] = "Brass"
            items["SECONDARY_MATERIAL"] = "Gold"
        if "gold plated" in description.lower() and "steel" in description.lower():
            items["MAIN_MATERIAL"] = "Steel"
            items["SECONDARY_MATERIAL"] = "Gold"
        if "sterling silver" in description.lower():
            items["MAIN_MATERIAL"] = "Silver"
        if len(sizes)>0:
            for size in sizes:
                items["SIZE_DEFINITION"] = size
                items["SIZE_CODE"] = self.get_size_code(size)
                yield items
        else:
            yield items
    def get_domain_country_code(self,url):
        data = {
            '.es':'ESP', '/es/':'ESP',
        }
        for key in data.keys():
            if key in url:
                return data[key]
    def get_size_code(self,size):
        sizes ={
            "extra-extra-small":"XS",
            "extra-small":"XS",
            "small":"S",
            "medium":"M",
            "large":"L",
            "extra-large":"XL",
            "extra-extra-large":"XXL",
            "extra-small":"XS",
        }
        return sizes.get(size.lower(),"")
    def has_numbers(self,inputString):
        return any(char.isdigit() for char in inputString)

    def get_carat_weight(self,description):
        match_word = 'carats'
        if match_word in description.lower() and 'total weight' in description.lower():
            words = description.split()
            for itr in range(len(words)):
                if match_word == words[itr].lower() or match_word==words[itr][:-1].lower():
                    if self.has_numbers(words[itr-1]):
                        return words[itr-1]
    
    def get_cut(self,description):
        match_word = '-cut'
        if match_word in description.lower():
            words = description.split()
            for itr in range(len(words)):
                if match_word in words[itr].lower():
                    temp = words[itr].lower().split(match_word)[0]
                    temp +=match_word
                    return temp
    
    def get_material_stamp(self,data):
        match_words = ["18 carat","18 karat", "18-karat", "18k", "18kt","18-carat"]
        for word in match_words:
            if word in data.lower():
                return "18kt"
