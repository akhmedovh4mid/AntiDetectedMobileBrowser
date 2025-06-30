import json
import os
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import requests
from openpyxl import load_workbook
from dataclasses import dataclass

from utils import get_country_info
from browser import GeoConfig, MobileBrowserConfig, ProxyConfig, UndetectBrowser


@dataclass
class AdInfo:
    url: str
    description: str
    ad_img: str


class ExcelDataHandler:
    def __init__(self):
        self.data: Dict[str, List[AdInfo]] = {}

    def get_valid_excel_path(self) -> str:
        """Запрашивает путь к файлу, пока не будет введен корректный."""
        parser = argparse.ArgumentParser(description='Process Excel file with advertisement data')
        parser.add_argument('file_path', type=str, help='Path to Excel file (.xlsx)')
        args = parser.parse_args()

        path = args.file_path.strip()

        if not path:
            raise ValueError("❌ Путь не может быть пустым")

        if not path.lower().endswith('.xlsx'):
            raise ValueError("❌ Файл должен быть в формате .xlsx!")

        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ Файл '{path}' не найден!")

        return path

    def load_data(self, file_path: str) -> Dict[str, List[AdInfo]]:
        try:
            wb = load_workbook(file_path, data_only=True)
            sheet = wb.active

            print(f"\nФайл '{file_path}' успешно загружен. Данные:")

            for row in sheet.iter_rows(values_only=True):
                url = row[2]
                description = row[8] if row[8] != "null" else row[3]
                lang = row[4] if row[4] is not None else "undefined"
                ad_img = row[7]

                ad_info = AdInfo(url=url, description=description, ad_img=ad_img)

                if lang in self.data:
                    self.data[lang].append(ad_info)
                else:
                    self.data[lang] = [ad_info]

            return self.data

        except Exception as e:
            print(f"⚠️ Ошибка при загрузке файла: {e}")
            raise


class AdBrowserProcessor:
    def __init__(self, data: Dict[str, List[AdInfo]]):
        self.data = data
        self.proxy_list_path = Path("proxy.json")
        self.proxy_list = None

        if self.proxy_list_path.exists():
            with self.proxy_list_path.open(mode="r", encoding='utf-8') as file:
                try:
                    self.proxy_list = json.load(file)  # Загружаем JSON в словарь
                except json.JSONDecodeError as e:
                    raise ValueError(f"Ошибка парсинга proxy.json: {e}")
        else:
            raise FileNotFoundError("Файл proxy.json не найден")

    def process_ads(self):
        for lang, info_list in self.data.items():
            country_data = get_country_info(country_code=lang)

            proxy = self._get_proxy_config(lang)
            if proxy == None and lang != "ru":
                continue

            mobile_config = self._get_mobile_config(country_data)
            geo_config = self._get_geo_config(country_data)

            self._process_with_browser(info_list, proxy, mobile_config, geo_config)

    def _get_proxy_config(self, lang: str) -> Optional[ProxyConfig]:
        if lang == "ru":
            return None

        if proxy := self.proxy_list.get(lang):
            return ProxyConfig(
                host=proxy["host"],
                port=proxy["port"],
                username=proxy["username"],
                password=proxy["password"]
            )

        return None

    def _get_mobile_config(self, country_data: dict) -> MobileBrowserConfig:
        return MobileBrowserConfig(
            user_agent="Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.1234.56 Mobile Safari/537.36",
            width=360,
            height=640,
            pixel_ratio=3.0,
            language=country_data["language"],
            headless=False
        )

    def _get_geo_config(self, country_data: dict) -> GeoConfig:
        return GeoConfig(
            timezone=country_data["timezone"],
            latitude=country_data["latitude"],
            longitude=country_data["longitude"]
        )

    def _process_with_browser(self, info_list: List[AdInfo], proxy: Optional[ProxyConfig],
                            mobile_config: MobileBrowserConfig, geo_config: GeoConfig):
        with UndetectBrowser(
            proxy_config=proxy,
            browser_config=mobile_config,
            geo_config=geo_config
        ) as browser:
            for info in info_list:
                if info.url is None:
                    break

                count = 0
                while count < 3:
                    try:
                        result = browser.build(
                            url=info.url if info.url.startswith("http") else f"https://{info.url}",
                            ad_image_url=info.ad_img,
                            description=info.description
                        )

                        if result:
                            break

                        count += 1

                    except Exception as e:
                        print(info.url, e)
                        break


if __name__ == "__main__":
    try:
        handler = ExcelDataHandler()
        file_path = handler.get_valid_excel_path()
        data = handler.load_data(file_path)

        processor = AdBrowserProcessor(data)
        processor.process_ads()

    except Exception as e:
        print(f"Произошла ошибка: {e}")
