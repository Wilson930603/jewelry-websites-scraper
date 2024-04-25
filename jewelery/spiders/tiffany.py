from scrapy import Request, Spider, Selector
from time import strftime, gmtime
import json
import sys, pandas as pd
from webcolors import CSS3_NAMES_TO_HEX, HTML4_NAMES_TO_HEX, CSS21_NAMES_TO_HEX
import tldextract
from random import randint
import datetime
from ..items import new_fields
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
def extract_domain_domainUrl(real_url):
    extracted = tldextract.extract(real_url)
    domain_without_tld = extracted.domain
    domain = domain_without_tld
    domain_url = extracted.registered_domain
    return domain, domain_url


class Tiffany(Spider):
    name = "tiffany"
    base_url = "https://www.tiffany.es"
    start_urls = ["https://www.tiffany.es/"]
    # handle_httpstatus_list = [400]
    download_delay = 0.8
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
    headers_api = {
        "x-ibm-client-id": "b9a8bfef128b495f8f17fb3cdeba5555",
        "x-ibm-client-secret": "73805214423d4AaebC96aD5581dbcf0b",
        "Content-Type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }
    api_url = "https://www.tiffany.es/ecomproductsearchprocessapi/api/process/v1/productsearch/ecomguidedsearch"
    tiffany_fields = [
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
    tiffany_fields2= [
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
        "FEED_EXPORT_FIELDS": tiffany_fields2, 
        "CONCURRENT_REQUESTS": 5,
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
            NEW_IN = None if not item.get('NEW_IN') or item['NEW_IN'] == '' or item['NEW_IN'] == 'N/A' else int(item['NEW_IN'])
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
        # file = './tiffany_feed_sb_5.csv'
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
        
        yield Request(self.start_urls[0], headers=self.headers, callback=self.main_page)

    def main_page(self, response):
        categories_url = response.xpath(
            '//div[@class="sub-nav__inner"]//li/a/@href'
        ).extract()
        print(f"Categories")
        for category in categories_url:
            if (
                "/fragrance/" in category
                or "/home-designs/" in category
                or "/gifts-for-the-home/" in category
                or "/stories/" in category
            ):
                continue
            # print(category)
            yield Request(category, callback=self.category_page, headers=self.headers)
            # break

    def category_page(self, response):
        try:
            data = response.xpath(
                '//script[contains(text(),"window.tiffany.authoredContent.searchConfig={}")]/text()'
            ).get()
            if data is None:
                return
            extracted_payload = (
                data.split("window.tiffany.authoredContent.searchConfig={}")[-1]
                .split("if(typeof window.tiffany.authoredContent.browseConfig")[0]
                .split("window.tiffany.authoredContent.browseConfig=")[-1]
                .strip()
            )
            data_json = json.loads(extracted_payload)
            # input(f'Payload {data_json["request"]["payload"]}')
            payload = json.dumps(
                {
                    "assortmentID": data_json["request"]["payload"]["assortmentID"],
                    "sortTypeID": data_json["request"]["payload"]["sortTypeID"],
                    "categoryid": data_json["request"]["payload"]["categoryid"],
                    "navigationFilters": data_json["request"]["payload"][
                        "navigationFilters"
                    ],
                    "recordsOffsetNumber": data_json["request"]["payload"][
                        "recordsOffsetNumber"
                    ],
                    "recordsCountPerPage": data_json["request"]["payload"][
                        "recordsCountPerPage"
                    ],
                    "priceMarketID": data_json["request"]["payload"]["priceMarketID"],
                    "searchModeID": data_json["request"]["payload"]["searchModeID"],
                    "siteid": data_json["request"]["payload"]["siteid"],
                }
            )
            NEW_IN = 0
            BEST_SELLERS = 0
            if 'https://www.tiffany.es/jewelry/shop/new-jewelry/' == response.url:
                print(response.url)
                yield Request(
                    self.api_url,
                    callback=self.pagination,
                    headers=self.headers_api,
                    method="POST",
                    body=payload,
                    meta={"offset": 0, "payload": data_json["request"]["payload"],"NEW_IN":1},
                )
            elif 'https://www.tiffany.es/jewelry/shop/most-popular-jewelry/' == response.url:
                print(response.url)
                yield Request(
                    self.api_url,
                    callback=self.pagination,
                    headers=self.headers_api,
                    method="POST",
                    body=payload,
                    meta={"offset": 0, "payload": data_json["request"]["payload"],"BEST_SELLERS":1},
                )
            else:
                print(response.url)
                yield Request(
                    self.api_url,
                    callback=self.pagination,
                    headers=self.headers_api,
                    method="POST",
                    body=payload,
                    meta={"offset": 0, "payload": data_json["request"]["payload"]},
                )
        except Exception as ex:
            print(ex)

    def pagination(self, response):
        try:
            offset = response.meta.get("offset")
            payload = response.meta.get("payload")
            offset = offset + payload["recordsCountPerPage"]
            payload["recordsOffsetNumber"] = offset
            payload_dump = json.dumps(payload)
            response_data = json.loads(response.text)
            products = response_data["resultDto"]["products"]

            for product in products:
  
                yield Request(
                    self.base_url + product.get("friendlyUrl"),
                    callback=self.product_page,
                    headers=self.headers,
                    dont_filter=True,
                    meta={
                        "productTag": product.get("productTag", ""),
                        "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                        "NEW_IN":response.meta.get('NEW_IN')
                        },
                )

            yield Request(
                self.api_url,
                callback=self.pagination,
                headers=self.headers_api,
                method="POST",
                body=payload_dump,
                meta={"offset": offset,
                "payload": payload,
                "BEST_SELLERS":response.meta.get('BEST_SELLERS'),
                "NEW_IN":response.meta.get('NEW_IN')},
            )
        except Exception as ex:
            print(f"################### EXCEPTIN {ex}")

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

    def product_page(self, response,isVariant=False):
        script = (
            response.xpath('//main//script[@type="application/ld+json"]/text()')
            .get(default='NA')
            .strip()
        )
        if script == "NA":
            return
        title = response.xpath('//h1[@class="product-description__content_title"]/span/text()').get(default='').strip()
        collection_name = response.xpath('//a[@class="product-description__content_eyebrow"]/span/span/text()').get(default='').strip()
        data = json.loads(script)
        breadCrumbs = response.xpath(
            '//li[@class="breadcrumb__container_link "]/a/text()'
        ).extract()
        data_images = response.xpath(
            '//script[contains(text(),"window.tiffany.authoredContent.productPreviewDetailsS7=")]'
        ).get()
        if data_images is None:
            data_images = response.xpath('//script[contains(text(),"window.tiffany.authoredContent.engagementProductPreviewDetails")]').get()
        variant_script = response.xpath(
            '//script[contains(text(),"variations")]/text()'
        ).get()

        if variant_script:
            try:
                variants_data = json.loads(
                    variant_script.split("window.tiffany.pdpConfig.modifiersConfig=")[
                        -1
                    ].split("window.tiffany.pdpConfig.dropHint")[0][:-1]
                )
            except:
                
                variants_data = json.loads(
                    variant_script.split('window.tiffany.authoredContent.engagementpdpConfig.modifiersConfig=')[
                        -1
                    ].split('window.tiffany.authoredContent.engagementCaratModifier')[0][:-1])
            
        else:
            variants_data = {}
        if data_images:
            try:

                img_data = json.loads(
                    data_images.split(
                        "window.tiffany.authoredContent.productPreviewDetailsS7="
                    )[-1].split(
                        "window.tiffany.authoredContent.iStatusInformationTextConfig"
                    )[
                        0
                    ][
                        :-1
                    ]
                )
            except:
                img_data = json.loads(
                    data_images.split('window.tiffany.authoredContent.engagementProductPreviewDetails=')[
                        -1
                    ].split(
                        'window.tiffany.authoredContent.engagementpdpConfig.dropHint'
                    )[
                        0
                    ][
                        :-1
                    ]
                )
        else:
            img_data = []
        size = ""
        available = response.xpath(
            '//tiffany-pdp-buttons'
        ).get()
        available_msg = ''
        if available:
            available = "Yes"
        else:
            available = response.xpath('//tiffany-product-action/@productconfig').get()
            if available:
                available = "No"
                available_msg = "Notificarme cuando esté disponible"

        variants = []
        if variants_data.get("variations"):
            for var in variants_data["variations"]:
                if var.get("isSelected") is False:
                    variants.append(self.base_url + var.get("URL"))
                else:
                    size = var.get("label")

        short_description = response.xpath(
            '//div[@class="product-description__content_title_extended"]/span/text()'
        ).get(default="")
        product_attributes = response.xpath(
            '//ul[@class="product-description__container_detail_list"]//span[@class="product-description__container_list-content"]/text()'
        ).extract()

        chain_adjustable = self.get_chain_adjustable(product_attributes)
        diameter = self.get_diameter(product_attributes)
        product_lenght = self.get_product_length(product_attributes)
        product_width = self.get_product_width(product_attributes)
        gem_weigth = self.get_carat_weight(product_attributes)
        items = {}
        images = [
            "https:" + img.get("defaultSrc")
            for img in img_data["images"]
            if img.get("defaultSrc") is not None
        ]
        imageName = "IMAGE_URL_"
        cat_name = "CATEGORY_"
        for itr, img in enumerate(images):
            if itr == 8:
                break
            newName =  imageName+str(itr + 1)
            items[newName] = img

        
        if response.meta.get('NEW_IN'):
            items['NEW_IN'] = response.meta.get('NEW_IN')
        else:
            items['NEW_IN'] = 0
        if response.meta.get('BEST_SELLERS'):
            items['BEST_SELLERS'] = response.meta.get('BEST_SELLERS')
        else:
            items['BEST_SELLERS'] = 0
        description = data.get("description", "") + " ".join(product_attributes)
        domain, domain_url = extract_domain_domainUrl(response.url)
        items["COLLECTION_NAME"] = collection_name
        items["DATE"] = datetime.datetime.now()
        items["DOMAIN"] = domain
        items["DOMAIN_URL"] = domain_url
        items['DOMAIN_COUNTRY_CODE'] = self.get_domain_country_code(response.url)
        items["BRAND"] = data.get("brand", "")
        items["MANUFACTURER"] = "tiffany"
        items["PRODUCT_BADGE"] = response.meta.get("productTag")
        items["SKU"] = data.get("sku", "")
        items["SKU_TITLE"] = title
        items["SKU_SHORT_DESCRIPTION"] = short_description
        items["SKU_LONG_DESCRIPTION"] = description
        items["SKU_LINK"] = response.url
        items["BASE_PRICE"] = data.get("offers", {}).get("price", "")
        items["ACTIVE_PRICE"] = data.get("offers", {}).get("price", "")
        items["CURRENCY"] = data.get("offers", {}).get("priceCurrency", "")
        items["CONDITION_PRODUCT"] = data.get("itemCondition", "")
        items["AVAILABILITY"] = available
        items["AVAILABILITY_MESSAGE"] = available_msg
        items["SIZE_DEFINITION"] = size
        items["SIZE_CODE"] = self.get_size_code(size)
        try:
            items["PRODUCT_INNER_DIAMETER"] = self.cm_to_mm(diameter)
        except:
            items["PRODUCT_INNER_DIAMETER"] = ''

        try:
            items["PRODUCT_LENGTH"] = self.cm_to_mm(product_lenght)
        except:
            items["PRODUCT_LENGTH"] = ''
        try:
            items["PRODUCT_WIDTH"] = self.cm_to_mm(product_width)
        except:
            items["PRODUCT_WIDTH"] = ''

        items["SKU_COLOR"] = "" if len(data.get("color")) == 0 else data.get("color")[0]
        items["MAIN_MATERIAL"] = (
            "" if len(data.get("material")) == 0 else data.get("material")[0]
        )
        length = items.get('PRODUCT_LENGTH','')
        width = items['PRODUCT_WIDTH']
        height = items.get('PRODUCT_WEIGHT','')

        # Join the dimensions using 'x', and exclude any empty strings
        items['SIZE_DIMENSIONS'] = 'x'.join([d for d in [str(length), str(width), str(height)] if d != ''])

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

        yield items

        if isVariant is False:
            for variant in variants:
                yield Request(
                    variant,
                    callback=self.product_page,
                    headers=self.headers,
                    meta={"productTag": "",
                    "NEW_IN":response.meta.get('NEW_IN'),
                    "BEST_SELLERS":response.meta.get('BEST_SELLERS')},
                    cb_kwargs={"isVariant":True}
                )

    def get_product_length(self, list_string):
        to_match = "de largo"
        for string in list_string:
            if to_match.lower() in string.lower():
                lines = string.split()
                for itr,word in enumerate(lines):
                    if "largo" in word.lower():
                        if "(" in lines[itr-2]:
                            return f"{lines[itr-4]}{lines[itr-3]}".replace(",",".")

                        return f"{lines[itr-3]}{lines[itr-2]}".replace(",",".")
        return ""
    def get_product_width(self, list_string):
        to_match = "de ancho"
        for string in list_string:
            if to_match.lower() in string.lower():
                lines = string.split()
                for itr,word in enumerate(lines):
                    if "ancho" in word.lower():
                        return f"{lines[itr-3]}{lines[itr-2]}".replace(",",".")
        return ""
    def get_diameter(self, list_string):
        to_match = "diámetro"
        for string in list_string:
            if to_match.lower() in string.lower():
                lines = string.split()
                for itr,word in enumerate(lines):
                    if "diámetro" in word.lower():
                        return f"{lines[itr-3]}{lines[itr-2]}".replace(",",".")
        return ""
    def get_carat_weight(self, list_string):
        to_match = ["Peso total en quilates de","Peso total en ct de"]
        for string in list_string:
            for match in to_match:
                if match.lower() in string.lower():
                    start_index = string.lower().find(match.lower())
                    if start_index != -1:
                        end_index = start_index + len(match) - 1
                        next_word_start_index = end_index + 2
                        next_word_end_index = next_word_start_index
                        while next_word_end_index < len(string) and not string[next_word_end_index].isspace():
                            next_word_end_index += 1
                        next_word = string[next_word_start_index:next_word_end_index]
                        if next_word.endswith("."):
                            next_word = next_word[:-1]
                        return next_word.replace(',','.')
        return ""
    def get_chain_adjustable(self, list_string):
        to_match = "Cadena ajustable de"
        remove_items = ["En una"]
        for string in list_string:
            if to_match.lower() in string.lower():
                lines = string.split()
                for itr,word in enumerate(lines):
                    if "Cadena".lower() in word.lower():
                        return f"{lines[itr+3]}{lines[itr+4]}".replace(",",".")
        return ""
    def get_clasp_type(self, list_string):
        to_match = "oculto"
        for string in list_string:
            if to_match.lower() in string.lower():
                return string.lower().replace(to_match.lower(), "").strip()
        return ""
    def cm_to_mm(self, length):
        """
        Converts a length value from centimeters (cm) or millimeters (mm) to millimeters (mm).

        Parameters:
        length (str): the length value as a string, with units denoted as "mm" or "cm"

        Returns:
        float or tuple: the length value(s) converted to millimeters, as a float or tuple of floats
        """

        if "-" in length:
            start, end = length.split("-")
            if "cm" in start or "mm" in start:
                start_mm = self.cm_to_mm(start)
            else:
                start_mm = self.cm_to_mm(
                    start + length.strip()[len(length) - 2 : len(length)]
                )
            end_mm = self.cm_to_mm(end)
            return f"{start_mm} - {end_mm}"
        elif "mm" in length:
            return float(length.strip()[:-2].strip())
        elif "cm" in length:
            return float(length.strip()[:-2].strip()) * 10.0
        else:
            return ""

    def get_material_stamp(self,data):
        match_words = ["18 carat","18 karat", "18-karat", "18k", "18kt","18-carat","18 quilates"]
        for word in match_words:
            if word in data.lower():
                return "18kt"

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