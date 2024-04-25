# jewelry-scraping-project

# Description

The Jewelry project is created to scrape data from jewelry site(s) like aristocrazy. This project structure allows to integrate more scrapers and use the same file formats, proxies or custom-defined paramters. Our goal is to achieve to generate a dataset with consistency from different sites

# Folder Structure

1. All spiders are located in ./spiders/ directory
2. Main director of the project is /jewelry/jewelry/ {where settings.py, items.py files are located}
3. Datasets will be generated and saved in /datafolder/ directory with the sitename_date.csv.


 To install the same packages by running:

```bash
pip install -r requirements.txt
```
# Run script
```bash
scrapy crawl aristocrazy -o ./datafolder/aristocrazy.csv

```
