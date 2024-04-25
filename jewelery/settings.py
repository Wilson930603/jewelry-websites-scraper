
BOT_NAME = 'jewelery'
SPIDER_MODULES = ['jewelery.spiders']
NEWSPIDER_MODULE = 'jewelery.spiders'
# LOG_STDOUT = True
# LOG_FILE = './log.log'

AZURE_BLOB_ACCOUNT_NAME = 'xxxxxx'
AZURE_BLOB_ACCOUNT_KEY = 'xxxxxx'
AZURE_BLOB_CONTAINER_NAME = 'crawlingimages'
AZURE_BLOB_MAX_CHUNK_SIZE =  4 * 1024 * 1024
MYSQL_HOST = 'xxxxxx'
MYSQL_USER = 'xxxxx'
MYSQL_PASSWORD = 'xxxxxx'
MYSQL_DBNAME = 'xxxxx'
MYSQL_SSL = {
    'ca': 'DigiCertGlobalRootCA.crt.pem',
}
# Obey robots.txt rules
ROBOTSTXT_OBEY = False
# LOG_LEVEL = 'INFO'
FEED_EXPORT_FIELDS =[
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

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'jewelery.middlewares.JewelerySpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'jewelery.middlewares.JeweleryDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'jewelery.pipelines.JeweleryPipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
