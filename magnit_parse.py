import os
import requests
import bs4
from urllib.parse import urljoin
from dotenv import load_dotenv
import pymongo

class MagnitParser:
    def __init__(self, start_url, data_base):
        self.start_url = start_url
        self.database = data_base["gb_parse_13_01_2021"]

    @staticmethod
    def _get_response(url, *args, **kwargs):
        response = requests.get(url, *args, **kwargs)
        return response

    @staticmethod
    def _get_soup(response):
        return bs4.BeautifulSoup(response.text, "lxml")

    def run(self):
        for product in self.parse(self.start_url):
            self.save(product)

    def parse(self, url) -> dict:
        soup = self._get_soup(self._get_response(url))
        catalog_main = soup.find('div', attrs={"class": "сatalogue__main"})
        for product_tag in catalog_main.find_all("a", attrs={"class": "card-sale"}):
            yield self._get_product_data(product_tag)

    @property
    def data_template(self):
        return {
        "url": lambda tag: urljoin(self.start_url, tag.attrs.get("href")),
        "promo_name": lambda tag: tag.find('div', attrs={"class": "card-sale__header"}).text,
        "product_name": lambda tag: tag.find('div', attrs={"class": "card-sale__title"}).text,
        "old_price": lambda tag: float(".".join(price for price in tag.find("div", attrs={"class": "label__price_old"}).text.split())),
        "new_price": lambda tag: float(".".join(price for price in tag.find("div", attrs={"class": "label__price_new"}).text.split())),
        "image_url": lambda tag: urljoin(self.start_url, tag.find("img").attrs.get("data-src")),
        # "date_from": lambda tag: tag.find('div', class_='card-sale__date').contents[1].text[2:],
        # "date_to": lambda tag: tag.find('div', class_='card-sale__date').contents[3].text[3:]
}


    def _get_product_data(self, product_tag:bs4.Tag) -> dict:
        data = {}
        for key, pattern in self.data_template.items():
            try:
                data[key] = pattern(product_tag)
            except (AttributeError, TypeError, ValueError):
                data[key] = None
        return data

    def save(self, data):
        collection = self.database["magnit_product"]
        collection.insert_one(data)

if __name__ == '__main__':
    load_dotenv(".env")
    data_base = pymongo.MongoClient(os.getenv("DATA_BASE_URL"))
    parser = MagnitParser("https://magnit.ru/promo/?geo=moskva", data_base)
    parser.run()