from scrapy import Request,Spider,Selector
from time import strftime,gmtime
import json
from ..items import JeweleryItem, new_fields
import sys, pandas as pd
import re
import mysql.connector
from collections import OrderedDict
import datetime
class Aristocrazy(Spider):
    name = 'aristocrazy'
    base_url = 'https://www.aristocrazy.com'
    start_urls = [
        'https://www.aristocrazy.com/es'
    ]
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }
    aristocrazy_fields_2= [
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
        'IMAGE_SAS_URL_1',
        'IMAGE_SAS_URL_2',
        'IMAGE_SAS_URL_3',
        'IMAGE_SAS_URL_4',
        'IMAGE_SAS_URL_5',
        'IMAGE_SAS_URL_6',
        'IMAGE_SAS_URL_7',
        'IMAGE_SAS_URL_8',
    ]
    aristocrazy_fields =[
        'DATE',
        'DOMAIN',
        'DOMAIN_URL',
        'DOMAIN_LOCALE',
        'CATEGORY_1',
        'CATEGORY_2',
        'CATEGORY_3',
        'GOOGLE_PRODUCT_CATEGORY',
        'COLLECTION_NAME',
        'COLLECTION _STYLE',
        'SEASON',
        'BRAND',
        'OCCASION',
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
        'COLOR_HEX_CODE',
        'IMAGE_URL_1',
        'IMAGE_URL_2',
        'IMAGE_URL_3',
        'IMAGE_URL_4',
        'IMAGE_URL_5',
        'IMAGE_URL_6',
        'IMAGE_URL_7',
        'IMAGE_URL_8',
        'MAIN_MATERIAL',
        'MATERIAL_RECYCLED',
        'SECONDARY_MATERIAL',
        'PATTERN',
        'SIZE',
        'SIZE_TYPE',
        'SIZE_SYSTEM',
        'SIZE_AGEGROUP',
        'SIZE_SUGGESTED_AGE',
        'SIZE_HASMEASUREMENT',
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
        'MATERIAL_FINISHING',
        'RAW_METAL',
        'RAW_MATERIAL_STAMP',
        'PLATING_METAL',
        'PLATING_METAL_STAMP',
        'JEWELRY_SETTING_STYLE',
        'PRODUCT_SHAPE',
        'PRODUCT_MOTIFF',
        'PRODUCT_SHAPE_MOTIF',
        'JEWEL_ENGRAVABLE',
        'JEWEL_INSCRIPTION',
        'JEWEL_IS_OPEN',
        'TOTAL_GEMSTONES_WEIGHT',
        'GEMSTONE_1_NAME',
        'GEMSTONE_1_CATEGORY',
        'GEMSTONE_1_COLOR',
        'GEMSTONE_1_COLOR_HEX_CODE',
        'GEMSTONE_1_NUMBER',
        'GEMSTONE_1_WEIGHT',
        'TOTAL_GEMSTONE_1_WEIGHT',
        'GEMSTONE_1_WEIGHT_SYSTEM',
        'GEMSTONE_1_CUT',
        'GEMSTONE_1_CREATION_METHOD',
        'GEMSTONE_1_TREATMENT',
        'GEMSTONE_1_CLARITY',
        'GEMSTONE_1_HEIGHT',
        'GEMSTONE_1_LENGTH',
        'GEMSTONE_1_WIDTH',
        'GEMSTONE_2_NAME',
        'GEMSTONE_2_CATEGORY',
        'GEMSTONE_2_COLOR',
        'GEMSTONE_2_COLOR_HEX_CODE',
        'GEMSTONE_2_NUMBER',
        'GEMSTONE_2_WEIGHT',
        'TOTAL_GEMSTONE_2_WEIGHT',
        'GEMSTONE_2_WEIGHT_SYSTEM',
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
        'GEMSTONE_3_COLOR_HEX_CODE',
        'GEMSTONE_3_NUMBER',
        'GEMSTONE_3_WEIGHT',
        'TOTAL_GEMSTONE_3_WEIGHT',
        'GEMSTONE_3_WEIGHT_SYSTEM',
        'GEMSTONE_3_CUT',
        'GEMSTONE_3_CREATION_METHOD',
        'GEMSTONE_3_TREATMENT',
        'GEMSTONE_3_CLARITY',
        'GEMSTONE_3_HEIGHT',
        'GEMSTONE_3_LENGTH',
        'GEMSTONE_3_WIDTH',
        'GEMSTONE_4_NAME',
        'GEMSTONE_4_CATEGORY',
        'GEMSTONE_4_COLOR',
        'GEMSTONE_4_COLOR_HEX_CODE',
        'GEMSTONE_4_NUMBER',
        'GEMSTONE_4_WEIGHT',
        'TOTAL_GEMSTONE_4_WEIGHT',
        'GEMSTONE_4_WEIGHT_SYSTEM',
        'GEMSTONE_4_CUT',
        'GEMSTONE_4_CREATION_METHOD',
        'GEMSTONE_4_TREATMENT',
        'GEMSTONE_4_CLARITY',
        'GEMSTONE_4_HEIGHT',
        'GEMSTONE_4_LENGTH',
        'GEMSTONE_4_WIDTH',
        'GEMSTONE_5_NAME',
        'GEMSTONE_5_CATEGORY',
        'GEMSTONE_5_COLOR',
        'GEMSTONE_5_COLOR_HEX_CODE',
        'GEMSTONE_5_NUMBER',
        'GEMSTONE_5_WEIGHT',
        'TOTAL_GEMSTONE_5_WEIGHT',
        'GEMSTONE_5_WEIGHT_SYSTEM',
        'GEMSTONE_5_CUT',
        'GEMSTONE_5_CREATION_METHOD',
        'GEMSTONE_5_TREATMENT',
        'GEMSTONE_5_CLARITY',
        'GEMSTONE_5_HEIGHT',
        'GEMSTONE_5_LENGTH',
        'GEMSTONE_5_WIDTH',
        'GEMSTONE_1_SETTING',
        'GEMSTONE_2_SETTING',
        'GEMSTONE_3_SETTING',
        'GEMSTONE_4_SETTING',
        'GEMSTONE_5_SETTING',
        'TOTAL_GEMSTONES_WEIGHT_SYSTEM',
        'CHAIN_LENGTH',
        'CHAIN_WIDTH',
        'CHAIN_THICKNESS',
        'CHAIN_ADJUSTABLE',
        'EARRING_BACK_FINDING',
        'EARRING_DROP_LENGTH',
        'CLASP_TYPE',
        'BAIL_SIZE',
        'STANDARD_FEATURES',
    ]
    custom_settings = {
        'FEED_EXPORT_FIELDS': aristocrazy_fields_2,
        'ITEM_PIPELINES': {
            'jewelery.pipelines.JeweleryPipeline': 300,
        }
    }
    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
    def closed(self,reason):
        file = sys.argv[-1]
        df = pd.read_csv(file).fillna('N/A').drop_duplicates()#.to_csv(file,index=False)
        for col in df.columns:
            if col not in self.aristocrazy_fields:
                df.drop(col,axis=1,inplace=True)
        df.to_csv(file,index=False)

    def domain_without_TLD(self,value):
        return value.split('.')[1]
    def find_region(self,value):
        regions = {'uk':'United Kingdom','/es/':'Spain',
                    'com':'United States'}
        for region in regions:
            if region in value:
                break
        return regions.get(region,'N/A')
    def striped(self,value):
        return value.strip().strip('$').replace("\n",'')
    def if_na(self,value):
        if value == None or value == '':
            return ''
        return self.striped(value)
    def start_requests(self):
        yield Request(self.start_urls[0],
                        callback=self.main_page,
                        headers=self.headers)
    def main_page(self,response):
        categories_url = response.xpath('//li[@class="top-category"]/a')
        for category in categories_url:
            category_text = category.xpath('.//text()').get()
            category_link = category.xpath('.//@href').get()
            yield Request(category_link,
                            callback=self.product_urls,
                            headers=self.headers,
                            meta = {'category_text':category_text},
                            dont_filter=True)
    
    def product_urls(self,response):
        meta = response.meta
        pagination_flag = meta.get('pagination',True)
        category_text = meta.get('category_text')
        list_urls =  [self.base_url + url for url in  response.xpath('//div[@class="product-image"]/a/@href').extract()]
        if len(list_urls) == 0:
            list_urls = response.xpath('//div[@class="containerMain"  or @class="image-linkable"]//a[.//img]/@href').extract()
            for url in list_urls:
                yield Request(url,
                                callback=self.product_urls,
                                headers=self.headers,
                                meta = {'category_text':category_text})
            return
        else:
            ###Pagination Logic
            if pagination_flag:
                pagination = '?start={startmul}&sz={start}&format=page-element'
                for num in range(1,101):
                    new_url = response.url+pagination.format(start = 12*num, startmul= 12*(1+num))
                    yield Request(new_url,
                                    callback=self.product_urls,
                                    headers=self.headers,
                                    meta = {'pagination':False,'category_text':category_text})
        for url in list_urls:
            yield Request(url,
                            callback=self.product_information,
                            headers=self.headers,
                            meta = {'category_text':category_text},
                            # dont_filter=True
            )
    def sku_clean(self,value):
        value = value.replace('\n','')
        value = value.split(':')[-1]
        return value
    def find_color(self,value):
        colors = {'Oro':'Golden'}
        for color in colors:
            if color in value:
                return color
    def clean_description(self,value,sep = ' '):
        return self.striped( sep.join([inst.strip().replace('•','') for inst in value if inst != '\n'] ) )
    def short_long_des(self,value):
        short_description = 4
        return self.clean_description(value.split('.')[0:short_description])
    
    def return_yes_no(self,value,keyword):
        if keyword in value:
            return 'Yes'
        return 'No'
    
    def gems_names(self,values):
        values = values.lower()
        possible_gems = [r.lower() for r in ['Diamante',
                         'Rubí',
                         'Esmeralda',
                         'Aguamarina',
                         'Zafiro',
                         'tsavorita',
                         'Cuarzo',
                         'Topacio',
                         'Ojo de tigre',
                         'Citrino',
                         ]]
        start = 1
        gems = {}
        query = 'GEMSTONE_{}_NAME'
        for stone in possible_gems:
            if stone in values:
                gems[query.format(start)] = stone
                start += 1
        return gems
    def gems_cut(self,values):
        possible_gems = [r.lower() for r in [
                         'talla brillante',
                         'Pera'
                         ]]
        start = 1
        gems = {}
        query = 'GEMSTONE_{}_CUT'
        for stone in possible_gems:
            if stone in values:
                gems[query.format(start)] = stone
                start += 1
        return gems
    def gems_number(self,values):
        possible_gems = [r.lower() for r in ['uno', 'dos', 'tres', 'cuatro', 'cinco', 'seis', 'siete', 'ocho', 'nueve', 'diez']]
        start = 1
        gems = {}
        query = 'GEMSTONE_{}_NUMBER'
        for stone in possible_gems:
            if stone in values:
                gems[query.format(start)] = stone
                start += 1
        return gems
    def gems_color(self,values):
        possible_gems = [r.lower() for r in ["Rojo", "Azul", "Verde", "Negro", "Blanco", "Rosa", "Púrpura", "Amarillo"]
                         ]
        start = 1
        gems = {}
        query = 'GEMSTONE_{}_COLOR'
        for stone in possible_gems:
            if stone in values:
                gems[query.format(start)] = stone
                start += 1
        return gems
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
    def product_information(self,response):
        
        time_stamp = datetime.datetime.now()
        meta = response.meta
        category_text = self.if_na(meta.get('category_text'))
        script_data = json.loads(response.xpath('//script[contains(text(),"Product") and contains(text(),"description")]/text()').get())

        availability = self.return_yes_no(script_data.get('offers').get('availability'),'InStock')
        
        product_sku = script_data.get('sku')#self.sku_clean(self.if_na(response.xpath('//span[contains(@class,"product-ref ")]/text()').get()))
        product_name = self.if_na(response.xpath('//h1[@class="product-name"]/text()').get())
        price_attributes = response.xpath('//span[@class="price-sales"]')
        currency = self.if_na(price_attributes.xpath('.//@data-currencycode').get())
        product_price = self.if_na(price_attributes.xpath('.//@data-price').get())
        product_description = ' \n'.join(list(OrderedDict.fromkeys([x.strip() for x in response.xpath('//div[@class="details-content"]/*[contains(@class,"long-content")]//text()').extract() if x.strip() !=''])))
        product_images = {num+1:link for num,link in enumerate(response.xpath('//ul[@class="bxslider"]//img/@src').extract())}
        product_brand = script_data.get('brand').get('name')
        product_gtin = script_data.get('gtin')
        product_color = self.if_na(response.xpath('//span[@id="ari_color"]/text()').get())#self.find_color(product_name)
        product_material = self.if_na(response.xpath('//span[contains(@id,"ari_material")]/text()').get())
        product_rating = self.clean_description(response.xpath('//div[@class="rating"]//text()').extract())
        product_reviews =  self.if_na(self.clean_description(response.xpath('//label[@class="tab-label" and contains(text(),"Reviews")]//text()').extract()).replace("Reviews",''))

        sites_region = self.find_region(response.url)
        domain_wo_tld = self.domain_without_TLD(response.url)
        fulldomain = self.base_url
        collection_name = response.xpath('//span[@id="ari_line"]/text()').get(default='').strip()
        main_material = self.if_na(response.xpath('//span[@id="ari_acabado"]/text()').get())
        size = self.if_na(response.xpath('//span[@class="size selected"]/text()').get())
        product_details = ', '.join(r for r in response.xpath('//label[contains(text(),"Product Details")]/following-sibling::div//text()').extract())
        product_details = product_details.lower()
        pattern = r'envío\s+([\d,]+)\s+€'
        shipping_price = response.xpath('//div[@class="shipping-and-returns"]/span[contains(text(),"Envío")]/text()').get(default='')
        match = re.search(pattern, shipping_price,flags=re.IGNORECASE)
        if match:
            # Extract the matched shipping price and remove commas
            shipping_price = match.group(1).replace(',', '.')
        

        items = {}
        items['DATE'] = time_stamp
        items['DOMAIN'] = domain_wo_tld
        items['DOMAIN_URL'] = fulldomain
        items['DOMAIN_COUNTRY_CODE'] = self.get_domain_country_code(response.url)
        items['COLLECTION_NAME'] = collection_name
        items['BRAND'] = product_brand
        items['MANUFACTURER'] = 'Aristo Crazy'
        items['SKU'] = product_sku
        items['SKU_TITLE'] = product_name
        items['SKU_SHORT_DESCRIPTION'] = product_description
        items['SKU_LONG_DESCRIPTION'] = product_description
        items['SKU_LINK'] = response.url
        items[f'GTIN{len(product_gtin)}'] = product_gtin
        items['BASE_PRICE'] = product_price
        items['ACTIVE_PRICE'] = product_price
        items['CURRENCY'] = currency
        items['AVAILABILITY'] = availability
        items['SHIPPING_EXPENSES'] = shipping_price
        items['SKU_COLOR'] = product_color
        items['MAIN_MATERIAL'] = product_material
        items['REVIEWS_RATING_VALUE'] = product_rating
        items['REVIEWS_NUMBER'] = product_reviews
        items['MAIN_MATERIAL'] = main_material
        items['SIZE_DEFINITION'] = size
        items['SIZE_CODE'] = self.get_size_code(size)
        images_items = 'IMAGE_URL_{num}'
        for num in range(1,9):
            image_link = product_images.get(num,'')
            items[images_items.format(num=num)] = image_link
        yield items
        
        