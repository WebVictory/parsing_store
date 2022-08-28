import datetime
import requests
from bs4 import BeautifulSoup
import csv
import json
import logging
from urllib.parse import urlparse
from urllib.parse import parse_qs
from pathlib import Path
from random import randint
from time import sleep


class Parser:
    def __init__(self):
        self.__get_config()
        self.__url_core = 'https://zootovary.ru'
        # ссылка на каталог с отображением по 50 товаров на странице
        self.__url_catalog = 'https://zootovary.ru/catalog/?pc=60'
        # папка по умолчанию результатов работы парсера
        self.__result_dir_default = 'out'
        self.__log_name_default = 'log.txt'
        self.__product_header = ['price_datetime', 'price', 'price_promo', 'sku_status', 'sku_barcode', 'sku_article',
                                 'sku_name', 'sku_category', 'sku_country', 'sku_weight_min', 'sku_volume_min',
                                 'sku_quantity_min', 'sku_link', 'sku_images']
        self.__products_file_name = "products.csv"
        self.__log_record()

    def __get_config(self, config_file="config.json"):
        with open(config_file, 'r') as f:
            config = json.load(f)
        self.__output_directory = config["config"]["output_directory"]
        self.__categories = config["config"]["categories"]
        self.__delay_range_s = config["config"]["delay_range_s"]
        self.__max_retries = config["config"]["max_retries"]
        self.__headers = config["config"]["headers"]
        self.__logs_dir = config["config"]["logs_dir"]
        self.__restart_count = config["config"]["restart"]["restart_count"]
        self.__interval_m = config["config"]["restart"]["interval_m"]

    # получаем все категории с сайта
    def get_categories(self):
        # получаем сначала разделы с главного сайта
        main_categories = self.__get_main_categories()
        # затем на страницах разделов получаем все категории
        categories = self.__get_all_categories(main_categories)
        # записываем полученную информацию в файл
        header = ['name', 'id']
        file_name = "categories.csv"
        info = "Приступаю к записи файла"
        logging.info(info)
        print(info)
        self.__record_csv(categories, header, file_name)
        return "Информация сохранена в файл"

    # получаем все продукты с сайта
    def get_products(self):
        all_links = self.__cheked_categories()
        all_products = self.__get_products_from_link(all_links)
        info = "Приступаю к записи файла"
        logging.info(info)
        print(info)
        self.__record_csv(all_products, self.__product_header, self.__products_file_name)
        info = "Файл успешно сохранен"
        logging.info(info)
        print(info)

    def __cheked_categories(self):
        all_links = []
        # если категории выбраны проходимся по всем
        if self.__categories:
            for category in self.__categories:
                catalog_url = self.__url_core +"/catalog"+ category + "?pc=60"
                category_links = self.__get_all_links(catalog_url)
                all_links += category_links
            else:
                info = f"Найдено товаров: {len(all_links)}"
        # иначе идем по всему каталогу
        else:
            all_links = self.__get_all_links(self.__url_catalog)
            info = f"Найдено товаров: {len(all_links)}"
            all_links = all_links
        print(info)
        logging.info(info)
        return all_links


    #получаем продукты по ссылкам
    def __get_products_from_link(self, all_links):
        all_products = []
        for iter, link in enumerate(all_links):
            # получаем данные со страницы товара

            products = self.__get_data_one_product(link)
            # если продуктов со страницы получено несколько тогда добавляем каждый из них
            for product in products:
                validate = self.__checking_duplicate(product, all_products)
                if validate:
                    info = f'{iter + 1} из {len(all_links)} загружен {product["sku_name"]} '
                    logging.info(info)
                    print(info)
                    all_products.append(product)
        return all_products

    # функция проверки совпадаения артикула, и штрихкод с уже найденными товарами.
    def __checking_duplicate(self, product_cheked, all_products):
        for product in all_products:
            if product["sku_article"] == product_cheked["sku_article"] and product["sku_barcode"] == product_cheked["sku_barcode"]:
                info = f'{product["sku_name"]}  не был добавлен из-за совпадаения артикула, и штрихкод с уже найденными товарами'
                print(info)
                logging.info(info)
                return True
        return True

    def __delay(self):
        # если 0 зпдержки нет
        if self.__delay_range_s == 0:
            pass
        # если не задано значение зпдержка между 1 и 3
        elif not self.__delay_range_s:
            delay_min = 1
            delay_max = 3
            sleep(randint(delay_min, delay_max))
        # если задано значение берем из переменой
        else:
            delay_min = self.__delay_range_s[0]
            delay_max = self.__delay_range_s[1]
            sleep(randint(delay_min, delay_max))

    # # проверка на совпадение артикула и штрихкода
    # def __limit_categories(self, category_item):
    #     # категория не проходит если  она есть списке категорий либо этот список пуст
    #     if category_item["id"] in self.__url_core + "/" + self.__categories or not self.__categories:
    #         return category_item

    def __get_main_categories(self):
        main_categories = []
        # получаем каталог
        page_source = self.__get_page(self.__url_core)
        catlog_source = page_source.find('div', {'id': 'catalog-menu'}).find('ul')
        catalog = catlog_source.findAll('li', recursive=False)
        # работаем с отдельными категориями
        for category in catalog:
            category_name = category.findChild().text
            category_id = category.findChild()['href']
            category_item = {"name": category_name, "id": category_id}
            main_categories.append(category_item)

        return main_categories

    def __get_all_categories(self, main_categories):
        categories = []
        categories.extend(main_categories)
        count_main_categories = str(len(main_categories))
        info = f"Найдено разделов:  {count_main_categories}"
        print(info)
        logging.info(info)
        for iter, category in enumerate(main_categories):
            # Получаем страницу с категориями внтури одного раздела
            page_source = self.__get_page(self.__url_core + category["id"])
            category_source = page_source.find('div', {'class': 'catalog-menu-left'}).findAll('li')
            count_categories = str(len(category_source))
            print(f"\nВ {iter + 1} разделe {category['name']} найдено {count_categories} категорий.\n")

            # Добавлем категории из этого раздела в список
            for iter, category in enumerate(category_source):
                count_categories = str(len(category_source))
                category_name = category.findChildren("a", recursive=False)[-1].text
                category_id = category.findChildren("a", recursive=False)[-1]['href']
                category_item = {"name": category_name, "id": category_id}
                categories.append(category_item)
                info = f'{iter + 1} из {count_categories} категория получена с названием - "{category_name}" и ID "{category_id}" '
                print(info)
                logging.info(info)
        else:
            info = f"Получено {len(categories)} категорий в {count_main_categories} разделах"
            print(info)
            logging.info(info)
        return categories

    def __restart_request(self, func):
        if self.__max_retries:
            for retrie in range(self.__max_retries):
                try:
                    # задеркжа перед следующим вызовом  функции
                    self.__delay()
                    return func
                except:
                    self.__delay()
            else:
                logging.error("Ошибка получения товара")
        else:
            return func

    def __restart_parsing(self, func):
        if self.__restart_count:
            for retrie in range(self.__restart_count):
                try:
                    return func
                except:
                    # задеркжа перед слеюдующей  вызовом слеюдующей функции
                    sleep(self.__interval_m)
            else:
                logging.error("Ошибка получения товара")
        else:
            return func

    # метод записи в файл
    def __record_csv(self, data, header, file_name):
        dir = self.__create_result_dir()
        file_name_full = dir + "/" + file_name

        with open(file_name_full, 'w', encoding='UTF8', newline='', ) as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter=";")
            writer.writeheader()
            writer.writerows(data)
        print("Файл записан")

    # настриваем работу логгирования
    def __log_record(self):
        log_dir = self.__create_log_dir()
        log_filename = self.__log_name_default
        if log_dir:
            log_filename = log_dir + "/" + self.__log_name_default

        logging.basicConfig(
            level=logging.DEBUG,
            filename=log_filename,
            format="%(asctime)s - %(levelname)s - %(funcName)s:  %(message)s",
            datefmt='%H:%M:%S',
        )

    # если значение конфига logs_dir не пустое создаем директорию
    def __create_log_dir(self):
        if self.__logs_dir:
            Path(self.__logs_dir).mkdir(parents=True, exist_ok=True)
        return self.__logs_dir

    # по умолчанию папка с резульататом работы self.__result_dir_default
    # если задано значение конфига output_directory берем название оттуда
    # и создаем папку
    def __create_result_dir(self):
        if self.__output_directory:
            dir_name = self.__output_directory
        else:
            dir_name = self.__result_dir_default
        Path(dir_name).mkdir(parents=True, exist_ok=True)
        return dir_name

    # получение одной страницы по ссылке
    def __get_page(self, url):
        # получаем страницу в случае ошибки потвторяем запрос столько сколько указано в конфиге
        r = self.__restart_request(requests.get(url, headers=self.__headers))
        soup = BeautifulSoup(r.text, features="html.parser")
        return soup

    # получаем ссылки на все товары на странице списка
    def __get_links_from_page(self, url):
        data_url = set()  # ссылки на товар, сохраняем все данные во множество чтобы избежать дубликатов
        page_source = self.__get_page(url)
        items_list = page_source.find_all('div', {'class': 'catalog-item'})
        for item in items_list:
            product_link = item.find('div', {'class': 'catalog-content-info'}).find('a').get('href')
            # добавляем во множество полные адреса ссылок
            data_url.add(self.__url_core + product_link)
        return data_url

    # получаем количество страниц
    def __get_page_count(self, url):
        page = self.__get_page(url)
        try:
            source = page.find('div', {'class': 'navigation'}).findChildren("a", recursive=False)[-1]['href']
            parsed_url = urlparse(source)
            page_count = parse_qs(parsed_url.query)['PAGEN_1'][0]
        except:
            page_count = 1

        return int(page_count)

    # получаем все ссылки на товары каталога
    def __get_all_links(self, catalog_url):
        all_links = set()
        page_count = self.__get_page_count(catalog_url)
        # получаем ссылки на товар со всех страниц каталога
        info = f"Найдено {page_count} страниц со списком товара"
        logging.info(info)
        print(info)
        for i in range(page_count):
            url_page = catalog_url + "&PAGEN_1=" + str(i + 1)
            links_from_page = self.__get_links_from_page(url_page)
            all_links.update(links_from_page)
            info = f"Обработано {i + 1} страниц из {page_count}"
            print(info)
            logging.info(info)
        info = f"Получено {len(all_links)} ссылок на товары "
        logging.info(info)
        print(info)

        return all_links

    # получаем всю информацию со страницы товара
    def __get_data_one_product(self, url_page):
        product_list = []
        page = self.__get_page(url_page)
        coutnt_items = len(page.findAll('tr', {'class': 'b-catalog-element-offer'}))


        for item in range(coutnt_items):
            product = {}
            product["price_datetime"] = f"{datetime.datetime.now():%H:%M:%S %d-%m-%Y}"
            product["sku_link"] = url_page

            self.__one_product_get_status(page, product)
            self.__one_product_get_name(page, product)
            self.__one_product_get_country(page, product)
            self.__one_product_get_categories(page, product)
            self.__one_product_get_images(page, product)

            self.__one_product_get_price(page, product, item)
            self.__one_product_get_price_promo(page, product, item)
            self.__one_product_get_barcode(page, product, item)
            self.__one_product_get_article(page, product, item)
            self.__one_product_get_dimensions(page, product, item),
            product_list.append(product)
        return product_list

    # получаем стоимость
    def __one_product_get_price(self, page, product, item):
        product["price"] = ""
        try:
            price = page.findAll('tr', {'class': 'b-catalog-element-offer'})[item].findChildren("td", recursive=False)[
                4].find('s').text
            if price:
                product["price"] = price
        except:
            info = "Ошибка загрузки стоимости"
            print(info)
            logging.warning(info)


    # получаем промо стоимость
    def __one_product_get_price_promo(self, page, product, item):
        product["price_promo"] = ""
        try:
            price_promo = page.findAll('tr', {'class': 'b-catalog-element-offer'})[item].findChildren("td", recursive=False)[4].find('span').text
            if price_promo:
                product["price_promo"] = price_promo
        except:
            info = "Ошибка загрузки акционной цены"
            print(info)
            logging.warning(info)

    # получаем штрихкод
    def __one_product_get_barcode(self, page, product, item):
        product["sku_barcode"] = ""
        try:
            sku_barcode_list = page.findAll('tr', {'class': 'b-catalog-element-offer'})[item].findChildren("td", recursive=False)[
                1].findAll('b')
            if sku_barcode_list:
                for barcode in sku_barcode_list[1:]:
                    product["sku_barcode"] += barcode.text + '\n'
                else:
                    # удалеям последний элемент \n строки в конце цикла
                    product["sku_barcode"] = product["sku_barcode"][:-1]
        except:
            info = "Ошибка загрузки штрихкода"
            print(info)
            logging.warning(info)

    # получаем артикул
    def __one_product_get_article(self, page, product, item):
        product["sku_article"] = ""
        try:
            sku_article = \
                page.findAll('tr', {'class': 'b-catalog-element-offer'})[item].findChildren("td", recursive=False)[
                    0].findAll('b')[
                    1].text
            if sku_article:
                product["sku_article"] = sku_article
        except:
            info = "Ошибка загрузки артикула"
            print(info)
            logging.warning(info)


    # определяем габариты заполняем нужные поля, ненужные оставлем пустыми
    def __one_product_get_dimensions(self, page, product, item):
        product["sku_volume_min"] = ""
        product["sku_quantity_min"] = ""
        product["sku_weight_min"] = ""
        try:
            dimensions = page.findAll('tr', {'class': 'b-catalog-element-offer'})[item].findChildren("td", recursive=False)[
                2].findAll('b')[1].text

            if dimensions and "л" in dimensions:
                product["sku_volume_min"] = dimensions
            elif dimensions and "шт" in dimensions:
                product["sku_quantity_min"] = dimensions
            elif dimensions and "г" in dimensions:
                product["sku_weight_min"] = dimensions
        except:
            info = "Ошибка загрузки габаритов товара"
            print(info)
            logging.warning(info)

    # получаем статус товара
    def __one_product_get_status(self, page, product):
        product["sku_status"] = ""
        try:
            sku_status = page.find('tr', {'class': 'b-catalog-element-offer'}).find('notavailbuybuttonarea')
            if sku_status:
                sku_status = 0
            else:
                sku_status = 1
            product["sku_status"] = sku_status
        except:
            info = "Ошибка загрузки статуса товара"
            print(info)
            logging.warning(info)

    # получаем название товарв
    def __one_product_get_name(self, page, product):
        product["sku_name"] = ""
        try:
            sku_name = page.find('div', {'class': 'catalog-element-right'}).find('h1').text
            if sku_name:
                product["sku_name"] = sku_name
        except:
            info = "Ошибка загрузки наименования товара"
            print(info)
            logging.warning(info)

    # получаем страну
    def __one_product_get_country(self, page, product):
        product["sku_country"] = ""
        try:
            sku_country = page.find('div', {'class': 'catalog-element-offer-left'}).find('p').text
            if sku_country:
                product["sku_country"] = sku_country
        except:
            info = "Ошибка загрузки страны товара"
            print(info)
            logging.warning(info)

    # получаем ктегории товара
    def __one_product_get_categories(self, page, product):
        product["sku_category"] = ""
        try:
            sku_category_list = page.find('ul', {'class': 'breadcrumb-navigation'}).findChildren("li", recursive=False)
            if sku_category_list:
                for category in sku_category_list[3:]:
                    if category.find('a'):
                        product["sku_category"] += category.find('a').text + '|'
                else:
                    # удалеям последний элемент | строки в конце цикла
                    product["sku_category"] = product["sku_category"][:-1]
        except:
            info = "Ошибка загрузки категории товара"
            print(info)
            logging.warning(info)

    # получаем изображения
    def __one_product_get_images(self, page, product):
        product["sku_images"] = ""
        try:
            sku_images = page.find('div', {'class': 'catalog-element-pictures'}).findAll("a", )
            if sku_images:
                for image in sku_images:
                    product["sku_images"] += self.__url_core + image.find('img')["src"] + ', '
                else:
                    # удаляем последний элемент , строки
                    product["sku_images"] = product["sku_images"][:-2]
        except:
            info = "Ошибка загрузки изображений товара"
            print(info)
            logging.warning(info)


if __name__ == '__main__':
    parser_categories = Parser()
    x = 0
    while x != "3":
        info = "Чтобы получить категории нажмите 1, чтобы получить товар нажимте 2, для выхода нажмите 3"
        print(info)
        logging.info(info)
        x = input()
        if x == "1":
            info = "Начинаем загрузку категорий"
            logging.info(info)
            print(info)
            parser_categories.get_categories()
        elif x == "2":
            info = "Начинаем загрузку товара"
            print(info)
            logging.info(info)
            parser_categories.get_products()
