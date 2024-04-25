# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from scrapy.exceptions import DropItem
import pymysql.cursors
import os
from scrapy.utils.project import get_project_settings
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
import requests
from io import BytesIO
class JeweleryPipeline:
    def __init__(self, hostname, username, password, database_name,sql_ssl):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database_name = database_name
        self.sql_ssl = sql_ssl
        self.account_name = get_project_settings().get("AZURE_BLOB_ACCOUNT_NAME")
        self.account_key = get_project_settings().get("AZURE_BLOB_ACCOUNT_KEY")
        self.container_name = get_project_settings().get("AZURE_BLOB_CONTAINER_NAME")
        self.max_chunk_size = get_project_settings().get("AZURE_BLOB_MAX_CHUNK_SIZE")
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            hostname=crawler.settings.get('MYSQL_HOST'),
            username=crawler.settings.get('MYSQL_USER'),
            password=crawler.settings.get('MYSQL_PASSWORD'),
            database_name=crawler.settings.get('MYSQL_DBNAME'),
            sql_ssl = crawler.settings.get('MYSQL_SSL'),
        )
    
    def open_spider(self, spider):
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net/",
            credential=self.account_key
        )
        # Get a container client
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        self.conn = pymysql.connect(
            host=self.hostname,
            user=self.username,
            password=self.password,
            db=self.database_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            ssl={
                'ca': 'DigiCertGlobalRootCA.crt.pem',
            }
        )
        self.cursor = self.conn.cursor()
        table_name = spider.name
        if spider.name == "aristocrazy":
            sku = "SKU VARCHAR(255) PRIMARY KEY"
            end_keys = ""
        else:
            sku = "SKU VARCHAR(255)"
            end_keys = """,
            SPECIAL_PRICES INT,
            NEW_IN INT,
            BEST_SELLERS INT"""
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                DATE TIMESTAMP,
                DOMAIN VARCHAR(255),
                DOMAIN_URL VARCHAR(255),
                DOMAIN_COUNTRY_CODE VARCHAR(255),
                COLLECTION_NAME VARCHAR(255),
                SEASON VARCHAR(255),
                BRAND VARCHAR(255),
                PRODUCT_BADGE VARCHAR(255),
                MANUFACTURER VARCHAR(255),
                MPN VARCHAR(255),
                {sku},
                SKU_TITLE VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                SKU_SHORT_DESCRIPTION TEXT COLLATE utf8mb4_unicode_ci,
                SKU_LONG_DESCRIPTION TEXT COLLATE utf8mb4_unicode_ci,
                SKU_LINK VARCHAR(255),
                GTIN8 VARCHAR(255),
                GTIN12 VARCHAR(255),
                GTIN13 VARCHAR(255),
                GTIN14 VARCHAR(255),
                BASE_PRICE FLOAT,
                SALES_PRICE FLOAT,
                ACTIVE_PRICE FLOAT,
                CURRENCY VARCHAR(255),
                AVAILABILITY VARCHAR(255),
                AVAILABILITY_MESSAGE TEXT,
                SHIPPING_LEAD_TIME VARCHAR(255),
                SHIPPING_EXPENSES FLOAT,
                MARKETPLACE_RETAILER_NAME VARCHAR(255),
                CONDITION_PRODUCT VARCHAR(255),
                SKU_COLOR VARCHAR(255),
                REVIEWS_RATING_VALUE FLOAT,
                REVIEWS_NUMBER INT,
                IMAGE_URL_1 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_2 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_3 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_4 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_5 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_6 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_7 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_URL_8 TEXT COLLATE utf8mb4_unicode_ci,
                SIZE_DEFINITION VARCHAR(255),
                SIZE_CODE VARCHAR(255),
                AUDIENCE_GENDER VARCHAR(255),
                SIZE_AGEGROUP VARCHAR(255),
                SIZE_SUGGESTED_AGE VARCHAR(255),
                SIZE_DIMENSIONS VARCHAR(255),
                PRODUCT_HEIGHT VARCHAR(255),
                PRODUCT_WIDTH VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                PRODUCT_LENGTH VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                PRODUCT_THICKNESS VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                PRODUCT_OUTTER_DIAMETER VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                PRODUCT_INNER_DIAMETER VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                PRODUCT_WEIGHT VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                MAIN_MATERIAL VARCHAR(255) COLLATE utf8mb4_unicode_ci,
                SECONDARY_MATERIAL VARCHAR(255),
                IMAGE_SAS_URL_1 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_2 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_3 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_4 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_5 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_6 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_7 TEXT COLLATE utf8mb4_unicode_ci,
                IMAGE_SAS_URL_8 TEXT COLLATE utf8mb4_unicode_ci{end_keys}
            );
        """
        self.cursor.execute(create_table_query)
    
    def close_spider(self, spider):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
    
    def process_item(self, item, spider):
        for itr in range(1,9):
            if item.get(f"IMAGE_URL_{itr}"):
                #item[f"IMAGE_SAS_URL_{itr}"] = item.get(f"IMAGE_URL_{itr}")
                if item.get(f'IMAGE_URL_{itr}') == 'N/A' or item.get(f'IMAGE_URL_{itr}') == "":
                    continue
                response = requests.get(item[f'IMAGE_URL_{itr}'])
                image_data = BytesIO(response.content)
                image_name = os.path.basename(item[f'IMAGE_URL_{itr}']).split('?')[0]
                # Create a blob client
                blob_client = self.container_client.get_blob_client(blob=image_name)

                # Upload the file in chunks
                if not blob_client.exists():
                    file_size = len(image_data.getbuffer())
                    upload_offset = 0
                    while upload_offset < file_size:
                        chunk_data = image_data.read(self.max_chunk_size)
                        chunk_size = len(chunk_data)
                        blob_client.upload_blob(chunk_data, length=chunk_size, max_concurrency=4)
                        upload_offset += chunk_size
                else:
                    spider.log(f"Resource by blob {image_name} Already exists. Moving to next...")
                    
                content_settings = ContentSettings(content_type=response.headers.get('Content-Type'))
                blob_client.set_http_headers(content_settings=content_settings)
                if blob_client.exists():
                    spider.log(f"The blob {image_name} was uploaded successfully to Azure Blob Storage!")
                    sas_expiry = datetime.utcnow() + timedelta(days=365)  # Expiry time for the SAS URL
                    sas_permissions = BlobSasPermissions(read=True)
                    sas_token = generate_blob_sas(
                        account_name=self.account_name,
                        account_key=self.account_key,
                        container_name=self.container_name,
                        blob_name=image_name,
                        permission=sas_permissions,
                        expiry=sas_expiry,
                    )
                    sas_url = blob_client.url #+ '?' + sas_token # SAS url
                    item[f"IMAGE_SAS_URL_{itr}"] = sas_url
                else:
                    spider.log(f"There was an error uploading the blob {image_name} to Azure Blob Storage.")

            else:
                break
        if spider.name == "monica" or spider.name == "tiffany":
            return item
        try:
            base_price = None if not item.get('BASE_PRICE') or item['BASE_PRICE'] == '' or item['BASE_PRICE'] == 'N/A' else float(item['BASE_PRICE'])
            sale_price = None if not item.get('SALES_PRICE') or item['SALES_PRICE'] == '' or item['SALES_PRICE'] == 'N/A' else float(item['SALES_PRICE'])
            active_price = None if not item.get('ACTIVE_PRICE') or item['ACTIVE_PRICE'] == '' or item['ACTIVE_PRICE'] == 'N/A' else float(item['ACTIVE_PRICE'])
            shipping_expense = None if not item.get('SHIPPING_EXPENSES') or item['SHIPPING_EXPENSES'] == '' or item['SHIPPING_EXPENSES'] == 'N/A' else float(item['SHIPPING_EXPENSES'])
            review_rating_value = None if not item.get('REVIEWS_RATING_VALUE') or item['REVIEWS_RATING_VALUE'] == '' or item['REVIEWS_RATING_VALUE'] == 'N/A' else float(item['REVIEWS_RATING_VALUE'])
            review_number = None if not item.get('REVIEWS_NUMBER') or item['REVIEWS_NUMBER'] == '' or item['REVIEWS_NUMBER'] == 'N/A' else int(item['REVIEWS_NUMBER'])
            
            
           
            if spider.name == "aristocrazy":
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
                                    SECONDARY_MATERIAL, IMAGE_SAS_URL_1,IMAGE_SAS_URL_2,IMAGE_SAS_URL_3,IMAGE_SAS_URL_4,
                                    IMAGE_SAS_URL_5,IMAGE_SAS_URL_6,IMAGE_SAS_URL_7,IMAGE_SAS_URL_8)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""".format(aristocrazy_table=spider.name),
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
                                        item.get('IMAGE_SAS_URL_1'),
                                        item.get('IMAGE_SAS_URL_2'),
                                        item.get('IMAGE_SAS_URL_3'),
                                        item.get('IMAGE_SAS_URL_4'),
                                        item.get('IMAGE_SAS_URL_5'),
                                        item.get('IMAGE_SAS_URL_6'),
                                        item.get('IMAGE_SAS_URL_7'),
                                        item.get('IMAGE_SAS_URL_8'),
                                )
                )
            else:
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
                                SECONDARY_MATERIAL,SPECIAL_PRICES,NEW_IN,BEST_SELLERS, IMAGE_SAS_URL_1,IMAGE_SAS_URL_2,
                                IMAGE_SAS_URL_3,IMAGE_SAS_URL_4,IMAGE_SAS_URL_5,IMAGE_SAS_URL_6,IMAGE_SAS_URL_7,IMAGE_SAS_URL_8)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s)""".format(aristocrazy_table=spider.name),
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
                                    item.get('SPECIAL_PRICES'),
                                    item.get('NEW_IN'),
                                    item.get('BEST_SELLERS'),
                                    item.get('IMAGE_SAS_URL_1'),
                                    item.get('IMAGE_SAS_URL_2'),
                                    item.get('IMAGE_SAS_URL_3'),
                                    item.get('IMAGE_SAS_URL_4'),
                                    item.get('IMAGE_SAS_URL_5'),
                                    item.get('IMAGE_SAS_URL_6'),
                                    item.get('IMAGE_SAS_URL_7'),
                                    item.get('IMAGE_SAS_URL_8'),
                               )
                )

            self.conn.commit()
            return item
        except mysql.connector.Error as e:
            spider.logger.error(f"Error inserting data into MySQL: {e}")
            raise DropItem(f"Error inserting data into MySQL: {e}")
