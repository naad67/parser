import time
import asyncio
import aiohttp
from bs4 import BeautifulSoup

import json
import re
from random import randint
import csv
import requests
from selenium.webdriver.common.by import By
from seleniumwire import webdriver


start_time = time.time()

MIN_INTERVAL = 3
MAX_INTERVAL = 7

data_set = []

def loading_page(value):
    is_done = False
    while not is_done:
        try:
            value = value
            is_done = True
        except:
            continue
    return value


def GetProductData(url):
    try:
        high = "Не указано"
        phone = "Нет"
        balcony = "Не указано"
        max_floor = floor = "Не указано"

        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        driver = webdriver.Chrome(options=options)
        driver.get(url)

        loading_page(driver.find_element(By.CLASS_NAME, "fi-close-big")).click()
        loading_page(driver.find_element(By.CLASS_NAME, "show-phones")).click()
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        script_tag = soup.find('script', id='jsdata')
        script_content = script_tag.string
        pattern = r'var data = ({.*?});'
        match = re.search(pattern, script_content, re.DOTALL)
        json_data = match.group(1)
        data = json.loads(json_data)

        id = data['advert']['id']
        price = data['advert']['price']
        square = data['advert']['square']
        owner_name = data['advert']['ownerName']
        user_type = data['advert']['userType']

        # Phone section
        phone_obj = soup.find('div', class_='offer__contacts-phones')
        if phone_obj:
            phone = phone_obj.text.strip()

        # Address section
        try:
            address = data['advert']['address']['street'] + " " + data['advert']['address']['house_num']
        except:
            address = data['advert']['addressTitle']

        # Title section
        title_obj = soup.find('div', class_='offer__info-item', attrs={'data-name': 'map.complex'})
        if title_obj:
            title = "ЖК " + title_obj.find('div', class_='offer__advert-short-info').text
        else:
            title = "Жилой Дом по Улице " + address

        # Balcony section
        balcony_dt = soup.find('dt', {'data-name': 'flat.balcony'})
        if balcony_dt:
            balcony_dd = balcony_dt.find_next('dd')
            balcony = balcony_dd.text.strip()
            balcony_g_dt = soup.find('dt', {'data-name': 'flat.balcony_g'})
            if balcony_g_dt:
                balcony_g_dd = balcony_dt.find_next('dd')
                balcony_g = balcony_g_dd.text.strip()
                if balcony != balcony_g:
                    balcony = balcony + " " + balcony_g

        # High section
        high_dt = soup.find('dt', {'data-name': 'ceiling'})
        if high_dt:
            high_dd = high_dt.find_next('dd')
            ceiling_height = high_dd.text.strip()
            ceiling_height = ceiling_height.split()
            high = ceiling_height[0]

        floor_dt = soup.find('div', {'data-name': 'flat.floor'})
        if floor_dt:
            floor_dt = floor_dt.find('div', class_="offer__advert-short-info").text
            floor_dt = floor_dt.split()

            max_floor = floor_dt[2]
            floor = floor_dt[0]
            print(floor_dt)

        return {"id": id, "title": title, "price": price, "square": square, "high": high, "floor": floor, "max_floor": max_floor,"address": address, "city": data['advert']['address']['city'], "balcony": balcony, "owner_name": owner_name, "phone": phone, "user_type": user_type}
    except:
        return GetProductData(url)

async def GetPageData(url):
    async with aiohttp.ClientSession() as session:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'user-agent': 'Mozilla/5.0 (Windows  NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
        }
        response = await session.get(url=url, headers=headers)
        print_page = url.split("?")[-1].split("=")[-1]
        print(f"[INFO] Parse {print_page} page")
        soup = BeautifulSoup(await response.text(), "html.parser")
        script_tag = soup.find('script', id='jsdata')

        script_content = script_tag.string
        pattern = r'var data = ({.*?});'
        match = re.search(pattern, script_content, re.DOTALL)
        json_data = match.group(1)
        data = json.loads(json_data)
        data = data['search']['ids']
        for item in data:
            link = f"https://krisha.kz/a/show/{item}"
            result =GetProductData(link)
            data_set.append(result)
            fields = data_set[0].keys()

            file_path = 'krisha_astana.csv'

            # Writing to the CSV file
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                csvwriter = csv.DictWriter(csvfile, fieldnames=fields)
                csvwriter.writeheader()
                csvwriter.writerows(data_set)
            await asyncio.sleep(randint(MIN_INTERVAL, MAX_INTERVAL))



async def GetAllPageData(url, total_links):
    current_link = 1
    while current_link < total_links:
        link = url + '/?page=' + f"{current_link}"
        current_link += 1
        task = asyncio.create_task(GetPageData(link))
        await asyncio.gather(task)
        await asyncio.sleep(randint(MIN_INTERVAL, MAX_INTERVAL))



def main():
    headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'user-agent': 'Mozilla/5.0 (Windows  NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
        }
    url = "https://krisha.kz/prodazha/kvartiry/astana/"
    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    next_page_element = soup.find('a', {'class': 'paginator__btn--next'})
    total_links = next_page_element.find_previous_sibling('a').text
    total_links = int(total_links)

    title_element = soup.find('div', {'class': 'page-title'})

    asyncio.run(GetAllPageData(url, total_links))

    fields = data_set[0].keys()

    file_path = 'krisha_astana.csv'

    # Writing to the CSV file
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        # Creating a CSV writer object
        csvwriter = csv.DictWriter(csvfile, fieldnames=fields)

        # Writing the header
        csvwriter.writeheader()

        # Writing the data
        csvwriter.writerows(data_set)

    finish_time = time.time() - start_time
    print(f"Затраченное на работу скрипта время: {finish_time}")


if __name__ == "__main__":
    main()