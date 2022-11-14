import datetime
import logging
from io import BytesIO, StringIO

import aiohttp
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime

import settings


class Register:

    @staticmethod
    def parser_bs4(html_doc: str) -> BeautifulSoup:
        soup = BeautifulSoup(html_doc, 'html.parser')
        return soup

    @staticmethod
    def transformed_str_date(text: str) -> datetime.date:
        str_date = text.split(" de ")
        transformed_str_date = str_date[1][:3]
        result = list(filter(lambda x: list(x.keys())[0] == transformed_str_date, settings.MONTHS))[0]
        date_time_obj = datetime.strptime(f"{str_date[0]}-{result.get(transformed_str_date)}-{str_date[2]}", '%d-%m-%Y')
        return date_time_obj.date()

    @staticmethod
    async def request_page(session: aiohttp.ClientSession, url_base: str, url_path: str) -> str:
        async with session.get(f"{url_base}{url_path}") as response:
            html = await response.text()
            return html

    @staticmethod
    def filter_url_files(soup_file: BeautifulSoup) -> dict:
        result = dict()
        html_section = soup_file.find("section", {"id": "dataset-resources"})
        html_a = html_section.find_all("a", {"class": "heading"})
        for item in html_a:
            if item.get_text().find("Conjunto CompletoCSV") > 0:
                result = {
                    "path": item['href'],
                    "name": item.get_text().strip()
                }

        return result

    @staticmethod
    async def download_csv(session: aiohttp.ClientSession, soup_file: BeautifulSoup) -> None:
        html_a = soup_file.find("a", {"class": "resource-url-analytics"})
        async with session.get(html_a["href"]) as response:
            file = BytesIO(await response.content.read())
            df = pd.read_csv(file, sep=";", encoding="iso-8859-1", low_memory=False)
            df.to_csv("cad_fi.csv", index=False)

    def check_update_file(self, soup_file: BeautifulSoup) -> bool:
        html_tbody = soup_file.find("tbody")
        html_td = html_tbody.find_all("td")
        last_update = self.transformed_str_date(text=html_td[0].get_text())
        date_now = datetime.now().date().isoformat()
        if last_update != date_now:
            return True
        return False

    async def worker(self):
        try:
            async with aiohttp.ClientSession(raise_for_status=True) as session:
                page_html = await self.request_page(session=session, url_base=settings.URL_CVM, url_path="/dataset/fi-cad")
                soup = self.parser_bs4(html_doc=page_html)
                link_detail_data = self.filter_url_files(soup_file=soup)

                page_html_detail = await self.request_page(
                    session=session,
                    url_base=settings.URL_CVM,
                    url_path=link_detail_data.get("path")
                )
                soup = self.parser_bs4(html_doc=page_html_detail)
                if self.check_update_file(soup_file=soup):
                    await self.download_csv(session=session, soup_file=soup)
                    logging.info("Download file was successfully")

        except Exception as e:
            logging.exception(f"Download file error --> {e}")




