from scrapy import Request,Spider,Selector
from time import strftime,gmtime
import json
from ..items import JeweleryItem, new_fields
import sys, pandas as pd
from webcolors import CSS3_NAMES_TO_HEX, HTML4_NAMES_TO_HEX, CSS21_NAMES_TO_HEX
import datetime
import csv
from tqdm import tqdm
import mysql.connector
import pymysql.cursors
from ..settings import MYSQL_HOST, MYSQL_DBNAME, MYSQL_PASSWORD, MYSQL_USER,MYSQL_SSL
import math
import csv
def load_csv_to_dict(filename):
    data = []
    with open(filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Replace NaN values with None
            row = {k: v if not isinstance(v, str) and math.isnan(v) else None if v == 'nan' else v for k, v in row.items()}
            data.append(row)
    return data
class Monicavainader(Spider):
    name = 'monica'
    base_url = 'https://www.monicavinader.com'
    start_urls = [
        'https://www.monicavinader.com/es'
    ]
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    }

    monica_vinader_fields = [
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
    monica_vinader_fields2= [
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
        'FEED_EXPORT_FIELDS': monica_vinader_fields2,
        'ITEM_PIPELINES': {
            'jewelery.pipelines.JeweleryPipeline': 300,
        }
    }
    def bulk_upload(self,cols,items):
        placeholders = ', '.join(['%s'] * len(cols))
        columns = ', '.join(cols)
        query = f"INSERT INTO {self.name} ({columns}) VALUES ({placeholders})"
        new_data = []
        for item in items:
            item = {k: v if not isinstance(v, str) and math.isnan(v) else None if v == 'nan' else v for k, v in item.items()}

            base_price = None if not item.get('BASE_PRICE') or item['BASE_PRICE'] == '' or item['BASE_PRICE'] == 'N/A' else float(item['BASE_PRICE'])
            sale_price = None if not item.get('SALES_PRICE') or item['SALES_PRICE'] == '' or item['SALES_PRICE'] == 'N/A' else float(item['SALES_PRICE'])
            active_price = None if not item.get('ACTIVE_PRICE') or item['ACTIVE_PRICE'] == '' or item['ACTIVE_PRICE'] == 'N/A' else float(item['ACTIVE_PRICE'])
            shipping_expense = None if not item.get('SHIPPING_EXPENSES') or item['SHIPPING_EXPENSES'] == '' or item['SHIPPING_EXPENSES'] == 'N/A' else float(item['SHIPPING_EXPENSES'])
            review_rating_value = None if not item.get('REVIEWS_RATING_VALUE') or item['REVIEWS_RATING_VALUE'] == '' or item['REVIEWS_RATING_VALUE'] == 'N/A' else float(item['REVIEWS_RATING_VALUE'])
            review_number = None if not item.get('REVIEWS_NUMBER') or item['REVIEWS_NUMBER'] == '' or item['REVIEWS_NUMBER'] == 'N/A' else int(item['REVIEWS_NUMBER'])
            
            SPECIAL_PRICES = None if not item.get('SPECIAL_PRICES') or item['SPECIAL_PRICES'] == '' or item['SPECIAL_PRICES'] == 'N/A' else int(item['SPECIAL_PRICES'])
            NEW_IN = None if not item.get('NEW_I N') or item['NEW_IN'] == '' or item['NEW_IN'] == 'N/A' else int(item['NEW_IN'])
            BEST_SELLERS = None if not item.get('BEST_SELLERS') or item['BEST_SELLERS'] == '' or item['BEST_SELLERS'] == 'N/A' else int(item['BEST_SELLERS'])

            new_data.append((
                item.get('DATE'),
                item.get('DOMAIN'),
                item.get('DOMAIN_URL'),
                item.get('DOMAIN_COUNTRY_CODE'),
                item.get('COLLECTION_NAME'),
                item.get('SEASON'),
                item.get('BRAND'),
                item.get('PRODUCT_BADGE'),
                item.get('MANUFACTURER'),
                item.get('MPN'),
                item.get('SKU'),
                item.get('SKU_TITLE'),
                item.get('SKU_SHORT_DESCRIPTION'),
                item.get('SKU_LONG_DESCRIPTION'),
                item.get('SKU_LINK'),
                item.get('GTIN8'),
                item.get('GTIN12'),
                item.get('GTIN13'),
                item.get('GTIN14'),
                base_price,
                sale_price,
                active_price,
                item.get('CURRENCY'),
                item.get('AVAILABILITY'),
                item.get('AVAILABILITY_MESSAGE'),
                item.get('SHIPPING_LEAD_TIME'),
                shipping_expense,
                item.get('MARKETPLACE_RETAILER_NAME'),
                item.get('CONDITION_PRODUCT'),
                item.get('SKU_COLOR'),
                review_rating_value,
                review_number,
                item.get('IMAGE_URL_1'),
                item.get('IMAGE_URL_2'),
                item.get('IMAGE_URL_3'),
                item.get('IMAGE_URL_4'),
                item.get('IMAGE_URL_5'),
                item.get('IMAGE_URL_6'),
                item.get('IMAGE_URL_7'),
                item.get('IMAGE_URL_8'),
                item.get('SIZE_DEFINITION'),
                item.get('SIZE_CODE'),
                item.get('AUDIENCE_GENDER'),
                item.get('SIZE_AGEGROUP'),
                item.get('SIZE_SUGGESTED_AGE'),
                item.get('SIZE_DIMENSIONS'),
                item.get('PRODUCT_HEIGHT'),
                item.get('PRODUCT_WIDTH'),
                item.get('PRODUCT_LENGTH'),
                item.get('PRODUCT_THICKNESS'),
                item.get('PRODUCT_OUTTER_DIAMETER'),
                item.get('PRODUCT_INNER_DIAMETER'),
                item.get('PRODUCT_WEIGHT'),
                item.get('MAIN_MATERIAL'),
                item.get('SECONDARY_MATERIAL'),
                SPECIAL_PRICES,
                NEW_IN,
                BEST_SELLERS,
            ))
        print(len(new_data[0]),len(cols))
        self.cursor.executemany(query,new_data)
        self.conn.commit()
    
    def closed(self,reason):
        file = sys.argv[-1]
        df = pd.read_csv(file).fillna('N/A')
        rows = [list(df.iloc[x]) for x in range(len(df))]
        for i,row_i in enumerate(rows):
            for j,row_j in enumerate(rows):
                if i==j:
                    continue
                if row_i[1:-3] == row_j[1:-3]:
                    if row_i[-2] == 0 and row_j[-2] == 1:
                        rows[i][-2] = 1
                    if row_i[-1] == 0 and row_j[-1] == 1:
                        rows[i][-1] = 1
                    print('Match found')



        with open(file, 'w', newline='',encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(list(list(df.columns)))
            writer.writerows(rows)
        # df = pd.read_csv(file).fillna('')
        # x.drop_duplicates(subset=list(df.columns)[1:],inplace=True)
        # x.to_csv(file,index=False)
        # file = './monica_feed_sb_5.csv'
        df = pd.read_csv(file)
        cols = list(df.columns)[1:]
        df.drop_duplicates(subset=cols,inplace=True)
        print(df)
        df.to_csv(file,index=False)
        items = load_csv_to_dict(file)
        self.bulk_upload(list(df.columns), items)

        self.conn.commit()
        self.cursor.close()
        self.conn.close()
    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.conn = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DBNAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        ssl={
            'ca': 'DigiCertGlobalRootCA.crt.pem',
        }
    )
        self.cursor = self.conn.cursor()
    def start_requests(self):
        yield Request(self.start_urls[0],
                        callback=self.main_page,
                        headers=self.headers)

    def main_page(self,response):
        men_jew = 'https://www.monicavinader.com/shop/mens-jewellery'
        categories_url = response.xpath('//ul[@class="header-nav__links"]//a')

        for itr,category in enumerate(categories_url):
            if itr == len(categories_url)-1:
                break
            category_text = category.xpath('./text()').get().strip()
            category_link = category.xpath('./@href').get()
            newLink = self.base_url+category_link
            if 'get-the-look/mens-jewellery' in category_link:
                newLink = men_jew
            if 'https://www.monicavinader.com/es/shop/by-collection/new-in/sort-by/new-in' in newLink:
                print(newLink)
                
                yield Request(newLink,
                                callback=self.category_page,
                                headers=self.headers,
                                meta = {'category_text':category_text,"NEW_IN":1},
                                dont_filter=True)
            elif 'https://www.monicavinader.com/es/shop/best-sellers' in newLink:
                print(newLink)
                yield Request(newLink,
                                callback=self.category_page,
                                headers=self.headers,
                                meta = {'category_text':category_text,"BEST_SELLERS":1},
                                dont_filter=True)
            else:
                print(newLink)
                yield Request(newLink,
                                callback=self.category_page,
                                headers=self.headers,
                                meta = {'category_text':category_text},
                                dont_filter=True)
        #     #break
    
    def category_page(self,response):

        meta = response.meta
        productLinks = response.xpath('//article[contains(@class,"product-preview")]')
        for product in productLinks:
            if product.xpath('.//button[contains(@class,"js-swatch swatch ")]'):
                group = product.xpath('.//button[contains(@class,"js-swatch swatch ")]')
                for item in group:
                    badge = item.xpath('./@data-badge').get(default='NA')
                    url = item.xpath('./@data-url').get()
                    if url is None:
                        continue
                    # print(f'{url} ----- {badge}')
                    new_url = self.base_url+url
                    yield Request(
                        new_url,
                        callback=self.product_page,
                        headers=self.headers,
                        dont_filter=True,
                        meta = {
                            'category_text':meta.get("category_text"),
                            'product_badge':badge,
                            "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                            "NEW_IN":response.meta.get('NEW_IN')
                            },
                        )
            else:
                url = product.xpath('./a/@href').get()
                if url is None:
                    continue
                # print(f'{url}')
                new_url = self.base_url+url
                yield Request(
                    new_url,
                    callback=self.product_page,
                    headers=self.headers,
                    dont_filter=True,
                    meta = {
                        'category_text':meta.get("category_text"),
                        "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                        "NEW_IN":response.meta.get('NEW_IN')
                        },
                    )
        
        nextUrl = response.xpath('//div[@class="pagination__links"]/a[text()="Next"]/@href').get()   
        if nextUrl:
            nextUrl = self.base_url + nextUrl
            yield Request(nextUrl,
                            callback=self.category_page,
                            headers=self.headers,
                            meta = {
                                'category_text':meta.get("category_text"),
                                "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                                "NEW_IN":response.meta.get('NEW_IN')},
                                dont_filter=True)
        
    
    def product_page(self,response):
        meta = response.meta
        #Check if a gift:
        combine = response.xpath('//div[@class="set-product-detail"]/a/@href').extract()
        if len(combine)>0:
            for com in combine:
                newSingle = self.base_url+com
                yield Request(
                    newSingle,
                    callback=self.product_page,
                    headers=self.headers,
                    meta = {
                        'category_text':meta.get("category_text"),
                        'product_badge':response.meta.get('product_badge'),
                        "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                        "NEW_IN":response.meta.get('NEW_IN')
                        },

                )
            return

        # Extracting from Json Data
        badge = response.meta.get('product_badge','')
        script = response.xpath('//script[@type="application/ld+json"]/text()').get()
        jData = json.loads(script)
        category_text = meta.get("category_text")
        time_stamp = strftime('%Y-%m-%d',gmtime())
        domain_wo_tld = self.domain_without_TLD(response.url)
        sites_region = 'es'

        title = jData.get('name','NA')
        brand = jData['brand'].get('name','NA')

        price = jData['offers'].get('price','NA')
        currency = jData['offers'].get('priceCurrency','NA')
        
        description = jData.get('description','NA')
        inStock = jData['offers'].get('availability','NA')
        availability_message = inStock
        if 'InStock' not in inStock:
            inStock = 'No'
        else:
            inStock = 'Yes'
        availability_message = availability_message.replace('https://schema.org/','')
        images = jData.get('image')

        # Extracting from Xpath
        material_recycled = response.xpath('//div[@class="product-info"]//*[contains(text(),"Recycled")]/text()').get(default='')
        engraving = response.xpath('//article//button[contains(@class,"add-engraving-new")]')
        if engraving:
            engraving = 'Yes'
        else:
            engraving = ''
        finish = response.xpath('//p[text()="Finish:"]/../p/text()').extract()[-1]
        stone = ''
        stone_color =''
        productColor = ''
        sizes = response.xpath('//div[@class="size-selector__options"]/a')
        size = meta.get('size','')
        plating = ''
        if 'gold' in finish.lower():
            plating = 'Gold'
        elif 'platinum' in finish.lower():
            plating = 'Platinum'
        elif 'bronze' in finish.lower():
            plating = 'Bronze'
        elif 'silver' in finish.lower():
            plating = 'Silver'
        # Processing product color and stone
        if finish:
            if '&' in finish:
                stone = finish.split('&')[-1].strip()
                if 'ct' in finish:
                    productColor = finish.split('ct')[-1].split('&')[0].strip()
                    productColor = self.find_color_name(finish)
                else:
                    # productColor = finish.split('&')[0].strip().split()[-1]
                    productColor = self.find_color_name(finish)

            else:
                if 'ct' in finish:
                    productColor = finish.split('ct')[-1].strip()
                    productColor = self.find_color_name(finish)
                else:
                    productColor = finish.split()[-1]
                    productColor = self.find_color_name(finish)
        tempColor = [x.strip() for x in response.xpath('//p[text()="Colour:"]/..//a[@class="swatch swatch--active"]/span/text()').extract() if x.strip()!='']
        if len(tempColor)!=0:
            productColor = tempColor[0]
        # Processing to find color of the stone
        if stone!='':
            if len(stone.split())>1:
                stone_color = self.find_color_name(stone)


        sku = response.xpath('//div[contains(text(),"Product code")]/text()').get(default='NA').replace('Product code -','').strip()
        lines = response.xpath('//div[contains(@class,"detail-callout__text")]/ul/li/text()').extract()

        # Data found for product details
        gemstone_1_name = stone
        gemstone_1_color = stone_color
        gemstone_1_lenght = ''
        gemstone_1_width = ''
        gemstone_1_weight = ''
        gemstone_1_clarity = ''
        gemstone_1_cut = ''
        chain_width = ''
        chain_adjust = ''
        productHeight =''
        productWidth = ''
        productThickness = ''
        earing_height = ''
        earing_width = ''
        internalDiameter = ''


        # Processing the data for product details
        for line in lines:
            gemstone_1_lenght = self.get_gemStone_length(line)
            if gemstone_1_lenght!='':
                break
        if gemstone_1_lenght == '':
            for line in lines:
                gemstone_1_lenght = self.get_getStone_length(stone,line)
                if gemstone_1_lenght!='':
                    break
        
        if gemstone_1_width == '':
            for line in lines:
                gemstone_1_width = self.get_gemStone_width(line)
                if gemstone_1_width!='':
                    break
        if gemstone_1_width == '':
            for line in lines:
                gemstone_1_width = self.get_getStone_width(stone,line)
                if gemstone_1_width!='':
                    break
        if gemstone_1_weight == '':
            for line in lines:
                gemstone_1_weight = self.get_getStone_weight(stone,line)
                if gemstone_1_weight!='':
                    break
        if gemstone_1_clarity == '':
            for line in lines:
                gemstone_1_clarity = self.get_getStone_quality(stone,line)
                if gemstone_1_clarity!='':
                    break
        if gemstone_1_cut == '':
            for line in lines:
                gemstone_1_cut = self.get_getStone_cut(stone,line)
                if gemstone_1_cut!='':
                    break
        if chain_width == '':
            for line in lines:
                chain_width = self.get_chain_width(line)
                if chain_width!='':
                    break
        if chain_adjust == '':
            for line in lines:
                chain_adjust = self.get_neclace_circumference(line)
                if chain_adjust!='':
                    break
        
        for line in lines:
            productHeight = self.get_productHeight(line)
            if productHeight!='':
                break

        for line in lines:
            productWidth = self.get_productWidth(line)
            if productWidth!='':
                break
        
        for line in lines:
            productThickness = self.get_productThickness(line)
            if productThickness!='':
                break

        for line in lines:
            earing_height = self.get_EaringHeight(line)
            if earing_height != '':
                break
        for line in lines:
            earing_width = self.get_EaringWidth(line)
            if earing_width != '':
                break
        for line in lines:
            internalDiameter = self.get_Internal_diameter(line)
            if internalDiameter != '':
                break
        
        chainLenData = response.xpath('//option[contains(@data-key,"Products_StockItem")]')
        

        items = {}
        if response.meta.get('NEW_IN'):
            items['NEW_IN'] = response.meta.get('NEW_IN')
        else:
            items['NEW_IN'] = '0'
        if response.meta.get('BEST_SELLERS'):
            items['BEST_SELLERS'] = response.meta.get('BEST_SELLERS')
        else:
            items['BEST_SELLERS'] = '0'
        imageName = 'IMAGE_URL_'
        if images != None:
            for itr,img in enumerate(images):
                if itr == 8:
                    break
                newName = imageName+str(itr+1)
                items[newName] = img

        fulldomain = self.base_url
        items['DATE'] = datetime.datetime.now()
        items['DOMAIN'] = domain_wo_tld
        items['DOMAIN_URL'] = fulldomain
        items['DOMAIN_COUNTRY_CODE'] = self.get_domain_country_code(response.url)
        items['BRAND'] = brand
        items['MANUFACTURER'] = 'Monica Vinader'
        items['SKU'] = sku
        items['SKU_TITLE'] = title
        items['SKU_LONG_DESCRIPTION'] = description
        items['SKU_LINK'] = response.url
        items['BASE_PRICE'] = float(price)
        items['ACTIVE_PRICE'] = float(price)
        items['CURRENCY'] = currency
        items['AVAILABILITY'] = inStock
        items['AVAILABILITY_MESSAGE'] = availability_message
        items['SKU_COLOR'] = productColor
        items['MAIN_MATERIAL'] = finish
        items['PRODUCT_BADGE'] = badge

        items['PRODUCT_INNER_DIAMETER'] = internalDiameter.replace('cmm','cm')
        items['PRODUCT_HEIGHT'] = self.cm_to_mm(productHeight.replace('cmm','cm'))
        items['PRODUCT_WIDTH'] = self.cm_to_mm(productWidth.replace('cmm','cm'))
        items['PRODUCT_THICKNESS'] = self.cm_to_mm(productThickness.replace('cmm','cm'))

        length = items.get('PRODUCT_LENGTH','')
        width = items['PRODUCT_WIDTH']
        height = items['PRODUCT_HEIGHT']

        # Join the dimensions using 'x', and exclude any empty strings
        items['SIZE_DIMENSIONS'] = 'x'.join([d for d in [str(length), str(width), str(height)] if d != ''])

        items["SIZE_DEFINITION"] = size
        items["SIZE_CODE"] = self.get_size_code(size)

        if len(sizes)>0:
            for size in sizes:
                if meta.get('category_text') == 'Rings':
                    items['SIZE_DEFINITION'] = size.xpath('./text()').get().strip()
                    items["SIZE_CODE"] = size.xpath('./text()').get().strip()
                    items["AVAILABILITY_MESSAGE"] = size.xpath('./@data-stock-message').get()

                
                    yield items
                else:
                    newUrl = self.base_url+size.xpath('./@href').get().strip()
                    items['SIZE_DEFINITION'] = size.xpath('./text()').get().strip()
                    items["SIZE_CODE"] = size.xpath('./text()').get().strip()
                    items["AVAILABILITY_MESSAGE"] = size.xpath('./@data-stock-message').get()
                    if newUrl == response.url:
                        yield items
                        continue
                    yield Request(newUrl,
                            callback=self.product_page,
                            headers=self.headers,
                            meta = {
                                'category_text':meta.get("category_text"),
                                'size':size.xpath('./text()').get().strip(),
                                "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                                "NEW_IN":response.meta.get('NEW_IN')
                                },
                            )
        elif len(chainLenData)>0:
            yield items
            for chainlen in chainLenData:
                newPrice = int(chainlen
                            .xpath('./text()')
                            .get().split()[-1]
                            .replace('(+€','')
                            .replace(')','')
                                )
                newSku = chainlen.xpath('./@data-code').get()
                new_title = chainlen.xpath('./text()').get().split('(')[0].strip()
                newChainLen = (chainlen
                            .xpath('./text()')
                            .get().split()[-2]
                            .replace('(','')
                            .replace(')','')
                                )
                newChainLen = newChainLen.replace('"',' in')
                items['SKU_TITLE'] = f'{title} & {newChainLen}'
                items['BASE_PRICE'] = price+newPrice
                items['ACTIVE_PRICE'] = price+newPrice
                items['SKU'] = newSku
                yield items

        
        else:
            yield items
        #input('here')
        variants = [self.base_url+var for var in response.xpath('//div[@class="swatch-group__options"]/a/@href').extract()]
        if len(variants)>0:
            for variant in variants:
                
                yield Request(variant,
                            callback=self.product_page,
                            headers=self.headers,
                            meta = {'category_text':meta.get("category_text"),
                            "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                            "NEW_IN":response.meta.get('NEW_IN')},
                            )
    def filter_nonColor(self,potentialColor):
        nonColors = ['leather','wax','ceramic','other']
        for nonColor in nonColors:
            if nonColor == potentialColor.lower():
                return ''
        return potentialColor
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
            #input(length.strip()[:-2].strip())
            return float(length.strip()[:-2].strip())
        elif "cm" in length:
            #input(length.strip()[:-2].strip())
            return float(length.strip()[:-2].strip()) * 10.0
        else:
            return ''
    def forLines(self,variable,lines):
        for line in lines:
            variable = self.get_EaringHeight(line)
            if variable != '':
                break
        return variable

    def get_size(self,url):
        if 'size=' in url:
            return url.split('size=')[-1]
        return ''
    def domain_without_TLD(self,value):
        return value.split('.')[1]
    def get_EaringHeight(self,line):
        earring = 'earing'
        height = 'height'
        if height in line.lower() and earring in line.lower():
            line = line.split()
            for itr in range(len(line)):
                c = itr+1
                while(c<len(line)):
                    if self.has_numbers(line[c]):
                        return line[c].replace(',','')
                    c +=1
        return ''
    def get_EaringWidth(self,line):
        earring = 'earing'
        width = 'width'
        if width in line.lower() and earring in line.lower():
            line = line.split()
            for itr in range(len(line)):
                c = itr+1
                while(c<len(line)):
                    if self.has_numbers(line[c]):
                        return line[c].replace(',','')
                    c +=1
        return ''
    def get_Internal_diameter(self,line):
        diameter = 'diameter'
        if diameter in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if diameter == line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
                        
        return ''

    def get_getStone_length(self,gem,line):
        length = 'length'

        if gem.lower() in line.lower() and length in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if length == line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
                        
        return ''
    
    def has_numbers(self,inputString):
        return any(char.isdigit() for char in inputString)
    def get_gemStone_width(self,line):
        gem = 'gemstone'
        width = 'width'

        if gem.lower() in line.lower() and width in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if width in line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
                        
        return ''
    def get_gemStone_length(self,line):
        gem = 'gemstone'
        length = 'length'

        if gem.lower() in line.lower() and length in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if length in line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
                        
        return ''
    def get_getStone_width(self,gem,line):
        width = 'width'

        if gem.lower() in line.lower() and width in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if width in line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1

        return ''
    def get_getStone_weight(self,gem,line):
        weight = 'weight'

        if gem.lower() in line.lower() and weight in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if weight in line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1

        return ''
    
    def get_getStone_quality(self,gem,line):
        quality = 'quality'

        if gem.lower() in line.lower() and quality in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if quality in line[itr].lower():
                    return line[itr+1].lower().replace(',','')
        return ''

    def get_getStone_cut(self,gem,line):
        cut = 'cut'

        if gem.lower() in line.lower() and cut in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if cut in line[itr].lower():
                    if line[itr+1].lower() == 'full':

                        return line[itr+1]+' '+cut
                    return line[itr+1]
        return ''

    def get_neclace_circumference(self,line):
        necklace = 'necklace'
        circumference = 'circumference'
        adjustable = 'adjustable'

        if (necklace in line.lower() 
            and circumference in line.lower()
            and adjustable in line.lower()
            ):
            line = line.split()
            for itr in range(len(line)):
                if adjustable == line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
        return ''

    def get_chain_width(self,line):
        chain = 'chain'
        width = 'width'
        if chain in line.lower() and width in line.lower():
            line = line.split()
            for itr in range(len(line)):
                if width == line[itr].lower():
                    c = itr+1
                    while(c<len(line)):
                        if self.has_numbers(line[c]):
                            return line[c].replace(',','')
                        c +=1
                        
        return ''
    
    def get_productHeight(self,line):
        products = ['pendant','band', 'candle']
        height = 'height'
        for product in products:
            if product in line.lower() and height in line.lower():
                line = line.split()
                for itr in range(len(line)):
                    if height == line[itr].lower():
                        c = itr+1
                        while(c<len(line)):
                            if self.has_numbers(line[c]):
                                return line[c].replace(',','')
                            c +=1
        return ''

    def get_productWidth(self,line):
        products = ['pendant','band']
        width = 'width'
        for product in products:
            if product in line.lower() and width in line.lower():
                line = line.split()
                for itr in range(len(line)):
                    if width == line[itr].lower():
                        c = itr+1
                        while(c<len(line)):
                            if self.has_numbers(line[c]):
                                try:
                                    if line[c+1].replace(',','') == 'mm' or line[c+1].replace(',','') == 'cm' or line[c+1].replace(',','') == 'cmm':
                                        return line[c].replace(',','').strip() + line[c+1].replace(',','')
                                except:
                                    pass
                                return line[c].replace(',','').strip()
                            c+=1
        return ''

    def get_productThickness(self,line):
        products = ['pendant']
        thickness = 'thickness'
        for product in products:
            if product in line.lower() and thickness in line.lower():
                line = line.split()
                for itr in range(len(line)):
                    if thickness == line[itr].lower():
                        c = itr+1
                        while(c<len(line)):
                            if self.has_numbers(line[c]):
                                return line[c].replace(',','')
                            c+=1
        return ''


    def find_color_name(self,string):
        """
        Find a color name in a string and return it.
        """
        words = string.split()
        for word in words:
            if word.lower() in CSS3_NAMES_TO_HEX or word.lower() in HTML4_NAMES_TO_HEX or word.lower() in CSS21_NAMES_TO_HEX:
                return word
        return None
    
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
    
    def process_item(self, item):
        try:
            base_price = None if not item.get('BASE_PRICE') or item['BASE_PRICE'] == '' or item['BASE_PRICE'] == 'N/A' else float(item['BASE_PRICE'])
            sale_price = None if not item.get('SALES_PRICE') or item['SALES_PRICE'] == '' or item['SALES_PRICE'] == 'N/A' else float(item['SALES_PRICE'])
            active_price = None if not item.get('ACTIVE_PRICE') or item['ACTIVE_PRICE'] == '' or item['ACTIVE_PRICE'] == 'N/A' else float(item['ACTIVE_PRICE'])
            shipping_expense = None if not item.get('SHIPPING_EXPENSES') or item['SHIPPING_EXPENSES'] == '' or item['SHIPPING_EXPENSES'] == 'N/A' else float(item['SHIPPING_EXPENSES'])
            review_rating_value = None if not item.get('REVIEWS_RATING_VALUE') or item['REVIEWS_RATING_VALUE'] == '' or item['REVIEWS_RATING_VALUE'] == 'N/A' else float(item['REVIEWS_RATING_VALUE'])
            review_number = None if not item.get('REVIEWS_NUMBER') or item['REVIEWS_NUMBER'] == '' or item['REVIEWS_NUMBER'] == 'N/A' else int(item['REVIEWS_NUMBER'])
            
            SPECIAL_PRICES = None if not item.get('SPECIAL_PRICES') or item['SPECIAL_PRICES'] == '' or item['SPECIAL_PRICES'] == 'N/A' else int(item['SPECIAL_PRICES'])
            NEW_IN = None if not item.get('NEW_IN') or item['NEW_IN'] == '' or item['NEW_IN'] == 'N/A' else int(item['NEW_IN'])
            BEST_SELLERS = None if not item.get('BEST_SELLERS') or item['BEST_SELLERS'] == '' or item['BEST_SELLERS'] == 'N/A' else int(item['BEST_SELLERS'])

            
            self.cursor.execute("""INSERT INTO {aristocrazy_table} (DATE, DOMAIN, DOMAIN_URL, DOMAIN_COUNTRY_CODE,
                            COLLECTION_NAME, SEASON, BRAND, PRODUCT_BADGE, MANUFACTURER, MPN, SKU, SKU_TITLE,
                            SKU_SHORT_DESCRIPTION, SKU_LONG_DESCRIPTION, SKU_LINK, GTIN8, GTIN12, GTIN13, GTIN14,
                            BASE_PRICE, SALES_PRICE, ACTIVE_PRICE, CURRENCY, AVAILABILITY, AVAILABILITY_MESSAGE,
                            SHIPPING_LEAD_TIME, SHIPPING_EXPENSES, MARKETPLACE_RETAILER_NAME, CONDITION_PRODUCT,
                            SKU_COLOR, REVIEWS_RATING_VALUE, REVIEWS_NUMBER, IMAGE_URL_1, IMAGE_URL_2,
                            IMAGE_URL_3, IMAGE_URL_4, IMAGE_URL_5, IMAGE_URL_6, IMAGE_URL_7, IMAGE_URL_8,
                            SIZE_DEFINITION, SIZE_CODE, AUDIENCE_GENDER, SIZE_AGEGROUP, SIZE_SUGGESTED_AGE,
                            SIZE_DIMENSIONS, PRODUCT_HEIGHT, PRODUCT_WIDTH, PRODUCT_LENGTH, PRODUCT_THICKNESS,
                            PRODUCT_OUTTER_DIAMETER, PRODUCT_INNER_DIAMETER, PRODUCT_WEIGHT, MAIN_MATERIAL,
                            SECONDARY_MATERIAL,SPECIAL_PRICES,NEW_IN,BEST_SELLERS)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(aristocrazy_table=self.name),
                            (
                                item.get('DATE'),
                                item.get('DOMAIN'),
                                item.get('DOMAIN_URL'),
                                item.get('DOMAIN_COUNTRY_CODE'),
                                item.get('COLLECTION_NAME'),
                                item.get('SEASON'),
                                item.get('BRAND'),
                                item.get('PRODUCT_BADGE'),
                                item.get('MANUFACTURER'),
                                item.get('MPN'),
                                item.get('SKU'),
                                item.get('SKU_TITLE'),
                                item.get('SKU_SHORT_DESCRIPTION'),
                                item.get('SKU_LONG_DESCRIPTION'),
                                item.get('SKU_LINK'),
                                item.get('GTIN8'),
                                item.get('GTIN12'),
                                item.get('GTIN13'),
                                item.get('GTIN14'),
                                base_price,
                                sale_price,
                                active_price,
                                item.get('CURRENCY'),
                                item.get('AVAILABILITY'),
                                item.get('AVAILABILITY_MESSAGE'),
                                item.get('SHIPPING_LEAD_TIME'),
                                shipping_expense,
                                item.get('MARKETPLACE_RETAILER_NAME'),
                                item.get('CONDITION_PRODUCT'),
                                item.get('SKU_COLOR'),
                                review_rating_value,
                                review_number,
                                item.get('IMAGE_URL_1'),
                                item.get('IMAGE_URL_2'),
                                item.get('IMAGE_URL_3'),
                                item.get('IMAGE_URL_4'),
                                item.get('IMAGE_URL_5'),
                                item.get('IMAGE_URL_6'),
                                item.get('IMAGE_URL_7'),
                                item.get('IMAGE_URL_8'),
                                item.get('SIZE_DEFINITION'),
                                item.get('SIZE_CODE'),
                                item.get('AUDIENCE_GENDER'),
                                item.get('SIZE_AGEGROUP'),
                                item.get('SIZE_SUGGESTED_AGE'),
                                item.get('SIZE_DIMENSIONS'),
                                item.get('PRODUCT_HEIGHT'),
                                item.get('PRODUCT_WIDTH'),
                                item.get('PRODUCT_LENGTH'),
                                item.get('PRODUCT_THICKNESS'),
                                item.get('PRODUCT_OUTTER_DIAMETER'),
                                item.get('PRODUCT_INNER_DIAMETER'),
                                item.get('PRODUCT_WEIGHT'),
                                item.get('MAIN_MATERIAL'),
                                item.get('SECONDARY_MATERIAL'),
                                SPECIAL_PRICES,
                                NEW_IN,
                                BEST_SELLERS,
                            )
            )
            self.conn.commit()
        except mysql.connector.Error as e:
            print(f"Error inserting data into MySQL: {e}")