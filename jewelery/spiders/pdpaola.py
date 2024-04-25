from scrapy import Request, Spider, Selector
from time import strftime, gmtime
import json
try:
    from ..items import JeweleryItem, new_fields
except:
    from items import JeweleryItem
import sys, pandas as pd
from webcolors import CSS3_NAMES_TO_HEX, HTML4_NAMES_TO_HEX, CSS21_NAMES_TO_HEX
from random import randint
import json
import tldextract
import datetime
def extract_domain_domainUrl(real_url):
    extracted = tldextract.extract(real_url)
    domain_without_tld = extracted.domain
    domain = domain_without_tld
    domain_url = extracted.registered_domain
    return domain, domain_url

class Pdpaola(Spider):
    name = "pdpaola"
    base_url = "https://www.pdpaola.com"
    base_url_product = "https://www.pdpaola.com/es/products/"
    delivery_api = "https://assets.pdpaola.com/shipping_methods/shipping_methods.json"
    delivery_data = []
    download_delay = 0.7
    handle_httpstatus_list = [430]
    color_map = {}
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/100.0.100.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/604.1 Edg/100.0.100.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edge/18.19042',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edge/18.19042',
        'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/65.0.3467.48',
            'Mozilla/5.0 (X11; CrOS x86_64 10066.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        ]
    start_urls = [
        "https://www.pdpaola.com/es",
    ]
    filter_urls = [
            'https://www.pdpaola.com/es/collections/bracelets/gold',
            'https://www.pdpaola.com/es/collections/fine-jewelry-earrings/earrings-sets',
            'https://www.pdpaola.com/es/collections/necklaces/solitary',
            'https://www.pdpaola.com/es/collections/necklaces/favorites',
            'https://www.pdpaola.com/es/collections/rings/silver',
            'https://www.pdpaola.com/es/collections/fine-jewelry-rings/eternity',
            'https://www.pdpaola.com/es/collections/ear-piercings/hoops',
            'https://www.pdpaola.com/es/collections/rings/statement',
            'https://www.pdpaola.com/es/collections/necklaces/chain',
            'https://www.pdpaola.com/es/collections/fine-jewelry-earrings/hoops',
            'https://www.pdpaola.com/es/collections/earrings/hoops',
            'https://www.pdpaola.com/es#gifts-menu',
            'https://www.pdpaola.com/es/collections/bracelets/favorites',
            'https://www.pdpaola.com/es/collections/rings/eternity',
            'https://www.pdpaola.com/es/collections/earrings/gold',
            'https://www.pdpaola.com/es/collections/earrings/basic',
            'https://www.pdpaola.com/es/collections/necklaces/zodiac',
            'https://www.pdpaola.com/es/collections/fine-jewelry-earrings/diamond',
            'https://www.pdpaola.com/es/collections/rings/basic',
            'https://www.pdpaola.com/es/collections/necklaces/basic',
            'https://www.pdpaola.com/es/collections/necklaces/gold',
            'https://www.pdpaola.com/es/collections/fine-jewelry-necklaces/basic',
            'https://www.pdpaola.com/es/collections/earrings/zodiac',
            'https://www.pdpaola.com/es/collections/fine-jewelry-rings/solitary',
            'https://www.pdpaola.com/es/collections/rings/customized',
            'https://www.pdpaola.com/es/collections/bracelets/customized',
            'https://www.pdpaola.com/es/collections/rings/signet',
            'https://www.pdpaola.com/es/collections/necklaces/customized',
            'https://www.pdpaola.com/es/collections/earrings/ear-cuffs',
            'https://www.pdpaola.com/es/collections/bracelets/silver',
            'https://www.pdpaola.com/es/collections/ear-piercings/gold',
            'https://www.pdpaola.com/es/collections/ear-piercings/pin-earrings',
            'https://www.pdpaola.com/es/pages/gifts',
            'https://www.pdpaola.com/es/collections/fine-jewelry-necklaces/solitary',
            'https://www.pdpaola.com/es/collections/fine-jewelry-rings/diamond',
            'https://www.pdpaola.com/es/collections/fine-jewelry-earrings/pin-earrings',
            'https://www.pdpaola.com/es/collections/fine-jewelry-earrings/basic',
            'https://www.pdpaola.com/es/collections/necklaces/silver',
            'https://www.pdpaola.com/es/collections/earrings/favorites',
            'https://www.pdpaola.com/es/collections/bracelets/chain',
            'https://www.pdpaola.com/es/collections/ear-piercings/silver',
            'https://www.pdpaola.com/es/collections/bracelets/basic',
            'https://www.pdpaola.com/es/collections/fine-jewelry-rings/basic',
            'https://www.pdpaola.com/es/collections/rings/gold',
            'https://www.pdpaola.com/es/collections/rings/solitary',
            'https://www.pdpaola.com/es/collections/ear-piercings/gemstones',
            'https://www.pdpaola.com/es/collections/medium-budget-gift',
            'https://www.pdpaola.com/es#',
            'https://www.pdpaola.com/es/collections/earrings/pin-earrings',
            'https://www.pdpaola.com/es/collections/earrings/customized',
            'https://www.pdpaola.com/es/collections/fine-jewelry-necklaces/diamond',
            'https://www.pdpaola.com/es#submenu-fine',
            'https://www.pdpaola.com/es/collections/rings/favorites',
            'https://www.pdpaola.com/es/pages/fine-jewelry-diamonds-18k-gold',
            'https://www.pdpaola.com/es/products/jewelry-digital-gift-card',
            'https://www.pdpaola.com/es/collections/letter-necklaces',
            'https://www.pdpaola.com/es/collections/earrings/silver',
            'https://www.pdpaola.com/es/collections/zodiac-necklaces'
    ]
    category_pages = "https://www.pdpaola.com/es/collections/{category}?page={page}&1678818627494"
    color_maping_url = (
        "https://assets.pdpaola.com/color-mappings/products-color-mapping.json"
    )
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }
    pdpaola_fields_2= [
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
    pdpaola_fields = [
        'DATE',
        'DOMAIN',
        'DOMAIN_URL',
        'DOMAIN_LOCALE',
        'CATEGORY_1',
        'CATEGORY_2',
        'CATEGORY_3',
        'COLLECTION_NAME',
        'SEASON',
        'BRAND',
        'PRODUCT_BADGE',
        'MANUFACTURER',
        'AUDIENCE_GENDER',
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
        'SALES_PRICE_STARTDATE',
        'SALES_PRICE_ENDDATE',
        'CURRENCY',
        'AVAILABILITY',
        'CONDITION',
        'SKU_COLOR',
        '1_IMAGE_URL',
        '2_IMAGE_URL',
        '3_IMAGE_URL',
        '4_IMAGE_URL',
        '5_IMAGE_URL',
        '6_IMAGE_URL',
        '7_IMAGE_URL',
        '8_IMAGE_URL',
        'MAIN_MATERIAL',
        'MATERIAL_FINISHING',
        'MATERIAL_RECYCLED',
        'SECONDARY_MATERIAL',
        'SIZE',
        'SIZE_TYPE',
        'SIZE_SYSTEM',
        'SIZE_AGEGROUP',
        'SIZE_SUGGESTED_AGE',
        'PRODUCT_HEIGHT',
        'PRODUCT_WIDTH',
        'PRODUCCT_LENGTH',
        'PRODUCCT_THICKNESS',
        'PRODUCT_OUTTER_DIAMETER',
        'PRODUCT_INNER_DIAMETER',
        'PRODUCT_WEIGHT',
        'REVIEWS_RATING_VALUE',
        'REVIEWS_NUMBER',
        'MANUFACTURING_TECHNIQUE',
        'MANUFACTURING_ORIGIN',
        'RAW_METAL',
        'RAW_METAL_WEIGHT',
        'RAW_MATERIAL_STAMP',
        'PLATING_METAL',
        'PLATING_METAL_STAMP',
        'JEWELRY_SETTING_STYLE',
        'PRODUCT_SHAPE_MOTIFF',
        'JEWEL_ENGRAVABLE',
        'JEWEL_INSCRIPTION',
        'TOTAL_GEMSTONES_WEIGHT',
        'TOTAL_GEMSTONES_WEIGHT_SYSTEM',
        'GEMSTONE_1_NAME',
        'GEMSTONE_1_SETTING',
        'GEMSTONE_1_CATEGORY',
        'GEMSTONE_1_COLOR',
        'GEMSTONE_1_NUMBER',
        'GEMSTONE_1_SETTING',
        'GEMSTONE_1_WEIGHT',
        'GEMSTONE_1_CUT',
        'GEMSTONE_1_CREATION_METHOD',
        'GEMSTONE_1_TREATMENT',
        'GEMSTONE_1_CLARITY',
        'GEMSTONE_1_HEIGHT',
        'GEMSTONE_1_LENGTH',
        'GEMSTONE_1_WIDTH',
        'GEMSTONE_2_NAME',
        'GEMSTONE_2_SETTING',
        'GEMSTONE_2_CATEGORY',
        'GEMSTONE_2_COLOR',
        'GEMSTONE_2_NUMBER',
        'GEMSTONE_2_SETTING',
        'GEMSTONE_2_WEIGHT',
        'GEMSTONE_2_CUT',
        'GEMSTONE_2_CREATION_METHOD',
        'GEMSTONE_2_TREATMENT',
        'GEMSTONE_2_CLARITY',
        'GEMSTONE_2_HEIGHT',
        'GEMSTONE_2_LENGTH',
        'GEMSTONE_2_WIDTH',
        'GEMSTONE_3_NAME',
        'GEMSTONE_3_CATEGORY',
        'GEMSTONE_3_COLOR',
        'GEMSTONE_3_NUMBER',
        'GEMSTONE_3_SETTING',
        'GEMSTONE_3_WEIGHT',
        'GEMSTONE_3_CUT',
        'GEMSTONE_3_CREATION_METHOD',
        'GEMSTONE_3_TREATMENT',
        'GEMSTONE_3_CLARITY',
        'GEMSTONE_3_HEIGHT',
        'GEMSTONE_3_LENGTH',
        'GEMSTONE_3_WIDTH',
        'GEMSTONE_4_NAME',
        'GEMSTONE_4_SETTING',
        'GEMSTONE_4_CATEGORY',
        'GEMSTONE_4_COLOR',
        'GEMSTONE_4_NUMBER',
        'GEMSTONE_4_WEIGHT',
        'GEMSTONE_4_CUT',
        'GEMSTONE_4_CREATION_METHOD',
        'GEMSTONE_4_TREATMENT',
        'GEMSTONE_4_CLARITY',
        'GEMSTONE_4_HEIGHT',
        'GEMSTONE_4_LENGTH',
        'GEMSTONE_4_WIDTH',
        'GEMSTONE_5_NAME',
        'GEMSTONE_5_SETTING',
        'GEMSTONE_5_CATEGORY',
        'GEMSTONE_5_COLOR',
        'GEMSTONE_5_NUMBER',
        'GEMSTONE_5_WEIGHT',
        'GEMSTONE_5_CUT',
        'GEMSTONE_5_CREATION_METHOD',
        'GEMSTONE_5_TREATMENT',
        'GEMSTONE_5_CLARITY',
        'GEMSTONE_5_HEIGHT',
        'GEMSTONE_5_LENGTH',
        'GEMSTONE_5_WIDTH',
        'CHAIN_LENGTH',
        'CHAIN_WIDTH',
        'CHAIN_THICKNESS',
        'CHAIN_ADJUSTABLE',
        'EARRING_DROP_LENGTH',
        'EARRING_SOLD_UNITS',
        'CLASP_TYPE',
        'BAIL_SIZE',
        'STANDARD_FEATURES',
    ]
    custom_settings = {
        'FEED_EXPORT_FIELDS': pdpaola_fields_2,
        'CONCURRENT_REQUESTS': 8,
        'ITEM_PIPELINES': {
            'jewelery.pipelines.JeweleryPipeline': 300,
        }
    }
    def rotate_headers(self , ref='',headers = None):
        
        user_agents = self.user_agents
        if headers is None:
            headers = self.headers
        
        headers["user-agent"] = user_agents[randint(0, len(user_agents) - 1)]
        if ref != '':
            headers['referer'] = ref
        return headers
    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)

    def start_requests(self):
        yield Request(self.delivery_api, callback=self.delivery_call,headers=self.headers)
        yield Request(
            self.color_maping_url, callback=self.parse_color_map, headers=self.headers
        )
    def delivery_call(self,response):
        try:
            self.delivery_data = json.loads(response.text)
        except:
            self.delivery_data = []
    def main_page(self,response):
        category_urls = set([self.base_url+url for url in response.xpath('//li[@class="site-nav--has-submenu"]//ul[@class="jewelry-linklist1"]//li/a/@href').extract()])

        for itr,link in enumerate(category_urls):
            if link in self.filter_urls:
                continue
            yield Request(
                link, callback= self.parse_category, headers= self.headers, 
                meta={
                    "page":1,
                    "category":link.split('/')[-1],
                    'total':0,
                    # 'BEST_SELLERS':1
                    }
            )
        yield Request(
            self.category_pages.format(category='sales',page=1),
            callback= self.parse_category, headers= self.headers, 
            meta={
                "page":1,
                "category":'sales',
                'total':0,
                'SPECIAL_PRICES':1
                }
        )
        yield Request(
            self.category_pages.format(category='new-in',page=1),
            callback= self.parse_category, headers= self.headers, 
            meta={
                "page":1,
                "category":'new-in',
                'total':0,
                'NEW_IN':1
                }
        )
    def parse_category(self,response):
        if response.status in self.handle_httpstatus_list:
            yield Request(
                self.category_pages.format(category=response.meta.get('category'),page=str(response.meta.get('page'))),
                callback=self.parse_category,
                dont_filter=True,
                headers= self.rotate_headers(), 
                meta={
                    "page":response.meta.get('page'),
                    "category":response.meta.get("category"),
                    "total":response.meta.get('total'),
                    "BEST_SELLERS" : response.meta.get('BEST_SELLERS'),
                    "SPECIAL_PRICES" : response.meta.get('SPECIAL_PRICES'),
                    "NEW_IN" : response.meta.get('NEW_IN'),
                    }             
            )
            return
        productLinks = response.xpath('//div[@id="products"]/article')
        total = response.meta.get('total')+len(productLinks)
        for product in productLinks:
            tempUrl = product.xpath('.//a[@class="product-link"]/@href').get()
            if tempUrl is None:
                print('None Found')
                continue
            newUrl = self.base_url+ tempUrl
            artile_id = product.xpath('./@data-product-item').get()
            set_urls = self.get_color_list(artile_id)
            set_urls.append(newUrl)
            set_urls = set(set_urls)
            badge = ' - '.join(product.xpath('.//div[@class="collection-tweak"]//text()| .//a[@class="collection-tweak"]//text()').extract())
            for all_url in set_urls:
                yield Request(
                    all_url, 
                    callback= self.parse_product, 
                    headers= self.headers,
                    dont_filter=True,
                    meta={
                        "page":response.meta.get('page'),
                        "category":response.meta.get('category'),
                        "total":total,
                        "badge":badge,
                        "BEST_SELLERS" : response.meta.get('BEST_SELLERS'),
                        "SPECIAL_PRICES" : response.meta.get('SPECIAL_PRICES'),
                        "NEW_IN" : response.meta.get('NEW_IN'),
                    }
                )
        if len(productLinks) == 0:
            print(f'total links: {total}')
        else:
            yield Request(
                self.category_pages.format(category=response.meta.get('category'),page=str(response.meta.get('page')+1)),
                callback=self.parse_category,
                headers= self.rotate_headers(), meta={
                    "page":response.meta.get('page')+1,
                    "category":response.meta.get("category"),
                    "total":total,
                    "BEST_SELLERS" : response.meta.get('BEST_SELLERS'),
                    "SPECIAL_PRICES" : response.meta.get('SPECIAL_PRICES'),
                    "NEW_IN" : response.meta.get('NEW_IN'),
                    }             
            )
    def parse_color_map(self,response):

        self.color_map = json.loads(response.text)
        yield Request(self.start_urls[0],
                        callback=self.main_page,
                        headers=self.headers)

    def parse_product(self,response):
        if response.status in self.handle_httpstatus_list:
            yield Request(
                response.url, 
                callback= self.parse_product, 
                headers= self.rotate_headers(),
                dont_filter=True,
                meta={
                    "page":response.meta.get('page'),
                    "badge":response.meta.get("badge"),
                    "BEST_SELLERS" : response.meta.get('BEST_SELLERS'),
                    "SPECIAL_PRICES" : response.meta.get('SPECIAL_PRICES'),
                    "NEW_IN" : response.meta.get('NEW_IN'),
                }
            )
            return
        if response.xpath('//section[@class="f-26 not-available-text"]'):
            print('Product Not available')
            return
        
        badge = response.meta.get("badge",'')
        time_stamp = datetime.datetime.now()
        domain_wo_tld = self.domain_without_TLD(response.url)
        fulldomain = self.base_url
        sites_region = self.find_region(response.url)
        product_brand = response.xpath('//meta[@itemprop="brand"]/@content').get(default='')
        product_raw_metal, product_metal = '',''
        if '/' in product_brand:
            product_raw_metal = product_brand.split('/')[0].strip()
            product_metal = product_brand.split('/')[-1].strip()
        script = response.xpath('//div[@class="product"]//script[@type="application/json"]/text()').get(default='')
        price_script = response.xpath('//script[@id="web-pixels-manager-setup"]/text()').get()
        price_script = price_script.split('initData:')[-1].split('function pageEvents')[0].strip()[:-3]
        price_script = json.loads(price_script)
        currency = response.xpath('//meta[@property="og:price:currency"]/@content').get(default='')
        price = response.xpath('//meta[@property="og:price:amount"]/@content').get(default='')
        jsonData = json.loads(script)
        product_name = jsonData.get('title','')
        product_description = ' '.join([x.strip() for x in response.xpath('//div[@class="rte"]/ul//li//text()').extract() if x.strip()!=''])
        category = jsonData.get('type','')
        product_skus = [var.get('sku','') for var in jsonData['variants']]
        images = [var.get('src') for var in jsonData['media']]
        available = self.check_availability(jsonData.get('available'))

            
        
        short_description = response.xpath('//div[@class="product_seo_description"]/p/text()').get(default='')
        details = [detail.strip().replace(':','') for detail in response.xpath('//div[@class="rte"]/ul//li//text()').extract() if detail.strip() !=""]
        material = self.get_detail_data('material',details)
        finish = self.get_detail_data('finishing',details)
        if 'plating' in finish:
            platting_material = finish.split('k')[-1].strip()
        else:
            platting_material = ''
        if material == '':
            material = response.xpath('//span[contains(text(),"METAL:")]/../text()').get(default='NA')
        if material =='NA':
            material = response.xpath('//span[text()="Material:"]/following-sibling::text()[1]').get(default='NA').strip()
        product_color = self.find_color_name(finish)
        stone = self.get_detail_data('stones',details)
        if self.has_numbers(self.get_detail_data('pendant measurements',details)):
            productMeasurement = self.get_detail_data('pendant measurements',details)
        else:
            productMeasurement = ''
        if self.has_numbers(self.get_detail_data('largo',details)):
            productLength = self.get_detail_data('largo',details)
        else:
            productLength = ''
        if self.has_numbers(self.get_detail_data('ancho',details)):
            productWidth = self.get_detail_data('ancho',details)
        else:
            productWidth = ''
        if productLength=='' and productWidth and 'x' in productMeasurement:
            tempProductMeaure, tempProductMeasureUnit = productMeasurement.split()[0], productMeasurement.split()[-1]
            productLength = tempProductMeaure.split('x')[0]+' '+tempProductMeasureUnit
            productWidth = tempProductMeaure.split('x')[-1]+' '+tempProductMeasureUnit
        if self.has_numbers(self.get_detail_data('DIÁMETRO EXTERIOR DEL ANILLO',details)):
            outer_diameter = self.get_detail_data('DIÁMETRO EXTERIOR DEL ANILLO',details)
        else:
            outer_diameter = ''
        if self.has_numbers(self.get_detail_data('espesor de los anillos',details)): 
            thickness = self.get_detail_data('espesor de los anillos',details)
        else:
            thickness = ''
        if thickness == '':
            if self.has_numbers(self.get_detail_data('GROSOR DEL ARO',details)):
                thickness = self.get_detail_data('GROSOR DEL ARO',details)
            else:
                thickness = ''
        if self.has_numbers(self.get_detail_data('PESO',details)):
            weight = self.get_detail_data('PESO',details)
        else:
            weight =''

        choices = response.xpath('//input[@class="variant-option"]')
        for choice in choices:
            choice.xpath('./@value').get()
            choice.xpath('./@data-price').get(default='NA').replace('€','')
        secondary_material = response.xpath('//span[contains(text(),"ACABADO")]/../text()').get(default='NA').strip()
        items = {}

        imageName = 'IMAGE_URL_'
        for itr,img in enumerate(images):
            if itr == 8:
                break
            newName = imageName+str(itr+1)
            items[newName] = img
        domain, domain_url = extract_domain_domainUrl(response.url)
        shipping_days = ""
        shipping_price = ""
        for x in self.delivery_data:
            if x.get("country") == "Spain":
                shipping_price = x['sh_price_2'].replace('€','').replace(',','').strip()
                express_shipping_time_min = x['sh_time_min_2']
                express_shipping_time_max = x['sh_time_max_2']
                days = f'{express_shipping_time_min}-{express_shipping_time_max}'
                break
        if response.meta.get('SPECIAL_PRICES'):
            items['SPECIAL_PRICES'] = response.meta.get('SPECIAL_PRICES')
        else:
            items['SPECIAL_PRICES'] = 0
        if response.meta.get('NEW_IN'):
            items['NEW_IN'] = response.meta.get('NEW_IN')
        else:
            items['NEW_IN'] = 0
        if response.meta.get('BEST_SELLERS'):
            items['BEST_SELLERS'] = response.meta.get('BEST_SELLERS')
        else:
            items['BEST_SELLERS'] = 0
        items["PAGE_NAME"] = response.meta.get('page_name')
        items['DATE'] = time_stamp #okay
        items["DOMAIN"] = domain
        items["DOMAIN_URL"] = domain_url
        items['DOMAIN_COUNTRY_CODE'] = self.get_domain_country_code(response.url)
        items['PRODUCT_BADGE'] = badge
        items['MANUFACTURER'] = 'pdpaola'
        items['SKU_TITLE'] = product_name
        items['SKU_SHORT_DESCRIPTION'] = short_description
        items['SKU_LONG_DESCRIPTION'] = product_description
        items['SKU_LINK'] = response.url
        items['BASE_PRICE'] = price
        items['ACTIVE_PRICE'] = price
        items['CURRENCY'] = currency
        items['AVAILABILITY'] = available
        items['SHIPPING_LEAD_TIME']= days
        items['SHIPPING_EXPENSES']= shipping_price
        items['SKU_COLOR'] = product_color
        items['MAIN_MATERIAL'] = material
        items['PRODUCT_WEIGHT'] = weight
        items['PRODUCT_OUTTER_DIAMETER'] = outer_diameter
        try:
            items['PRODUCT_LENGTH'] = self.cm_to_mm(productLength.replace(' ','').replace(',','.'))
        except:
            items['PRODUCT_LENGTH'] = ''
        items['PRODUCT_HEIGHT'] = ""
        items['PRODUCT_WIDTH'] = productWidth
        items['SECONDARY_MATERIAL'] = secondary_material
        length = items['PRODUCT_LENGTH']
        width = items['PRODUCT_WIDTH']
        height = items['PRODUCT_HEIGHT']

        # Join the dimensions using 'x', and exclude any empty strings
        items['SIZE_DIMENSIONS'] = 'x'.join([d for d in [str(length), str(width), str(height)] if d != ''])

        
        for var in jsonData['variants']:
            if category == 'Ring':

                items['SKU'] = var.get('sku')
                items['SIZE_DEFINITION'] = var.get('title')
                items['GTIN13'] = var.get('barcode')
                items['AVAILABILITY'] = self.check_availability(var.get('available'))
                items['BASE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                items['ACTIVE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                if var.get('title') != "Default Title":
                    items['SKU_TITLE'] = var.get('name')
                if var.get('compare_at_price'):
                    items['SALES_PRICE'] = items['BASE_PRICE']
                    items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                    items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)

                yield items
            elif category == 'Ear Piercing':
                items['SKU'] = var.get('sku')
                items['AVAILABILITY'] = self.check_availability(var.get('available'))
                items['GTIN13'] = var.get('barcode')
                items['BASE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                items['ACTIVE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                choices = response.xpath('//input[@class="variant-option"]')
                if len(choices)>0:
                    for choice in choices:
                        tempName = choice.xpath('./@name').get()
                        items['SKU_TITLE'] = product_name+ " & "+tempName
                        price = choice.xpath('./@data-price').get(default='NA').replace('€','').replace(',','.')
                        items['BASE_PRICE'] = price
                        items['ACTIVE_PRICE'] = price
                        if var.get('compare_at_price'):
                            items['SALES_PRICE'] = items['BASE_PRICE']
                            items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                            items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                        yield items
                else:
                    if var.get('title') != "Default Title":
                        items['SKU_TITLE'] = var.get('name')
                    if var.get('compare_at_price'):
                        items['SALES_PRICE'] = items['BASE_PRICE']
                        items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                        items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                    yield items
                   
            elif category == 'Necklace' or category == 'Bridge Product':

                items['SKU'] = var.get('sku')
                items['AVAILABILITY'] = self.check_availability(var.get('available'))
                items['GTIN13'] = var.get('barcode')
                items['BASE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                items['ACTIVE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                if var.get('title') != "Default Title":
                        items['SKU_TITLE'] = var.get('name')
                if var.get('compare_at_price'):
                    items['SALES_PRICE'] = items['BASE_PRICE']
                    items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                    items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                letters = response.xpath('//div[@class="letters-section"]//li/a/text()').extract()
                    
                
                yield items
                variants  = [self.base_url+variant for variant in response.xpath('//div[@class="letters-section"]//li/a/@href').extract()]
                for variant in variants:
                    yield Request(
                        variant, callback=self.parse_product, headers=self.headers,
                        meta={
                            "BEST_SELLERS" : response.meta.get('BEST_SELLERS'),
                            "SPECIAL_PRICES" : response.meta.get('SPECIAL_PRICES'),
                            "NEW_IN" : response.meta.get('NEW_IN'),
                            }
                )
            elif category == 'Charm':
                items['SKU'] = var.get('sku')
                items['AVAILABILITY'] = self.check_availability(var.get('available'))
                items['GTIN13'] = var.get('barcode')
                
                choices = response.xpath('//label[contains(@for,"ProductSelect-option-charm")]')
                price = self.get_variantPrice(var.get('sku'),price_script)
                if len(choices)>0:
                    for choice in choices:
                        tempChoice = [x.strip() for x in choice.xpath('.//text()').extract() if x.strip()!='']
                        if '€' in tempChoice[-1]:
                            price = tempChoice[-1].replace('€','').replace(',','.')
                        items['SKU_TITLE'] = product_name+ " & " + tempChoice[0]
                        items['BASE_PRICE'] = price
                        items['ACTIVE_PRICE'] = price
                        if var.get('compare_at_price'):
                            items['SALES_PRICE'] = items['BASE_PRICE']
                            items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                            items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                        yield items
                else:
                    if var.get('compare_at_price'):
                        items['SALES_PRICE'] = items['BASE_PRICE']
                        items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                        items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                    if var.get('title') != "Default Title":
                        items['SKU_TITLE'] = var.get('name')
                    yield items
            else:
                items['GTIN13'] = var.get('barcode')
                items['SKU'] = var.get('sku')
                items['AVAILABILITY'] = self.check_availability(var.get('available'))
                items['BASE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                items['ACTIVE_PRICE'] = self.get_variantPrice(var.get('sku'),price_script)
                if var.get('compare_at_price'):
                    items['SALES_PRICE'] = items['BASE_PRICE']
                    items['BASE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                    items['ACTIVE_PRICE'] = "{:.2f}".format(var.get('compare_at_price')/100)
                if var.get('title') != "Default Title":
                    items['SKU_TITLE'] = var.get('name')
                yield items
    def get_variantPrice(self,id,variant_json):
        for variant in variant_json['productVariants']:
            if id == variant.get('sku'):
                return variant.get('price').get('amount')
        return ''
    def cm_to_mm(self,length):
        """
        Converts a length value from centimeters (cm) or millimeters (mm) to millimeters (mm).
        
        Parameters:
        length (str): the length value as a string, with units denoted as "mm" or "cm"
        
        Returns:
        float or tuple: the length value(s) converted to millimeters, as a float or tuple of floats
        """
       
        if "-" in length:
            start, end = length.split("-")
            if 'cm' in start or 'mm' in start:
                start_mm = self.cm_to_mm(start)
            else:
                start_mm = self.cm_to_mm(start+length.strip()[len(length)-2:len(length)])
            end_mm = self.cm_to_mm(end)
            return f'{start_mm} - {end_mm}'
        elif "mm" in length:
            return float(length.strip()[:-2].strip())
        elif "cm" in length:
            return float(length.strip()[:-2].strip()) * 10.0
        else:
            return ''
    def has_numbers(self,inputString):
        return any(char.isdigit() for char in inputString)
    def check_availability(self,potential_availablity):
        if potential_availablity:
            return 'Yes'
        else:
            return 'No'
    def domain_without_TLD(self,value):
        return value.split('.')[1]
    def get_detail_data(self,find_string,details):

        for itr, detail in enumerate(details):
            if find_string.lower() == detail.lower():
                return details[itr+1]
        return ''
    
    def find_region(self,value):
        regions = {'uk':'United Kingdom','/es/':'Spain',
                    'com':'United States'}
        for region in regions:
            if region in value:
                break
    
    def find_color_name(self,string):
        """
        Find a color name in a string and return it.
        """
        words = string.split()
        for word in words:
            if word.lower() in CSS3_NAMES_TO_HEX or word.lower() in HTML4_NAMES_TO_HEX or word.lower() in CSS21_NAMES_TO_HEX:
                return word.lower()
        return ''
    
    def get_chain_length(self,string):
        data = ['adjustable', 'clasp','from']
        string = string.lower()
        for x in data:
            if x in string:
                string = string.replace(x,'')
        return string.strip()
    
    def get_color_list(self,id):
        temp_list = []
        for key in self.color_map.keys():
            for item in self.color_map[key]:
                if str(item.get('id')) == id:
                    for final in self.color_map[key]:
                        temp_list.append(self.create_urls(final.get('handle')))
        return temp_list
    def create_urls(self,handle):
        return self.base_url_product+handle

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
    
    def get_domain_country_code(self,url):
        data = {
            '.es':'ESP', '/es/':'ESP',
        }
        for key in data.keys():
            if key in url:
                return data[key]