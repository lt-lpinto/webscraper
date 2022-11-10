from prefect import flow, tasks

import requests, sys
from prefect import flow, task
from bs4 import BeautifulSoup
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_query
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud.bigquery import SchemaField
from prefect_gcp.bigquery import bigquery_insert_stream

sys.setrecursionlimit(100000)

ROOT_URL = "https://webscraper.io"
ENDPOINT_URL = "/test-sites/e-commerce/allinone-popup-links"
DATASET_NAME = "webscraper"
TABLE_NAME = "raw__allinone_popup_links"

@task
def get_request(url):
    response = requests.get(url)
    if (response.status_code == 200):
        return BeautifulSoup(str(response.text), "html.parser")
    return None 

@task
def parse_request(doc, selector):
   return doc.select(selector)

@task
def parse_items(doc):
    products = list()
    items = doc.select('div.thumbnail')
    for item in items:
        records = {}
        records['title'] = item.select_one('a.title').text
        records['price'] = item.select_one('h4').text
        records['description'] = item.select_one('p.description').text
        records['reviews'] = item.select_one('div.ratings p.pull-right').text
        records['product_url'] = ROOT_URL + item.select_one('a.title')['onclick'].replace("window.open(", "").split(",")[0].replace("'", "")
        products.append(records)

    return products

@task 
def create_bigquery_schema(gcp_credentials):
    schema = [
        SchemaField("title", field_type="STRING", mode="REQUIRED"),
        SchemaField("price", field_type="STRING", mode="REQUIRED"),
        SchemaField("description", field_type="STRING"),
        SchemaField("reviews", field_type="STRING"),
        SchemaField("product_url", field_type="STRING")
    ]

    return schema
    

@flow
def flow_get__allinone_popup_links():
    gcp_credentials = GcpCredentials(service_account_file="./tf-test-365219-e68b028a905b.json")
    schema = create_bigquery_schema(gcp_credentials)
    result = bigquery_create_table(
        dataset=DATASET_NAME,
        table=TABLE_NAME,
        schema=schema,
        gcp_credentials=gcp_credentials
    )

    #result = bigquery_query(
    #    "DELETE FROM `{}` WHERE true".format("tf-test-365219.webscraper.raw__allinone"), gcp_credentials, query_params=None
    #)

    products = list()
    doc = get_request(ROOT_URL + ENDPOINT_URL)
    categories = parse_request(doc, "a[class='category-link']")
    for cat in categories:
        category_url = ROOT_URL + cat['href']
        doc1 = get_request(category_url)
        sub_categories = parse_request(doc1, "a[class='subcategory-link']")
        for sub_cat in sub_categories:
            sub_categories_url = ROOT_URL + sub_cat['href']
            print(sub_categories_url)
            doc2 = get_request(sub_categories_url)
            products.extend(parse_items(doc2))

    result = bigquery_insert_stream(
        dataset=DATASET_NAME,
        table=TABLE_NAME,
        records=products,
        gcp_credentials=gcp_credentials
    )