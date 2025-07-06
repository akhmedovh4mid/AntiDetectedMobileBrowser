import json
from pathlib import Path
import time
from datetime import datetime
from typing import Optional, Dict

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from proxy.nekoray import Proxy
from browser import MobileBrowser
from manager import FileManager, DirManager


class WebsiteProcessor:
    def __init__(self, excel_path: str):
        """
        Инициализация процессора веб-сайтов.

        Args:
            excel_path: Путь к Excel-файлу с данными
        """
        self.excel_path = Path(excel_path)
        self.proxy_list = self._load_proxies()
        self.wb = load_workbook(self.excel_path)
        self.sheet = self.wb.active

    @staticmethod
    def _load_proxies() -> Dict:
        """Загружает прокси из JSON-файла."""
        with open("proxy.json", "r") as file:
            return json.load(file)

    def process_row(self, row: int) -> Optional[Path]:
        """
        Обрабатывает одну строку из Excel-файла.

        Args:
            row: Номер строки для обработки

        Returns:
            Path: Путь к сохраненной директории или None если обработка не удалась
        """
        link = self.sheet.cell(row=row, column=3).value
        if not link:
            return None

        title = self.sheet.cell(row=row, column=4).value
        lang = str(self.sheet.cell(row=row, column=5).value).lower()
        description = self.sheet.cell(row=row, column=9).value

        # Пропускаем если язык не поддерживается
        if lang not in self.proxy_list.keys() and lang != "ru":
            return None

        # Обработка с прокси или без
        if lang != "ru":
            return self._process_with_proxy(link, lang, title, description)
        else:
            return self._process_without_proxy(link, title, description)

    def _process_with_proxy(self, link: str, lang: str, title: str, description: str) -> Optional[Path]:
        """Обработка строки с использованием прокси."""
        proxy = self.proxy_list[lang]
        with Proxy(proxy["host"], proxy["port"], proxy["username"], proxy["password"]):
            time.sleep(5)
            return self._process_browser(link, lang, title, description)

    def _process_without_proxy(self, link: str, title: str, description: str) -> Optional[Path]:
        """Обработка строки без прокси."""
        return self._process_browser(link, "ru", title, description)

    def _process_browser(self, link: str, lang: str, title: str, description: str) -> Optional[Path]:
        """Общая логика работы с браузером."""
        with MobileBrowser() as browser:
            browser.goto(link, delay=3)

            if not browser.download_website("website"):
                DirManager.clear_directory(Path("temp"))
                return None

            try:
                browser.screenshot("temp/screenshot.png")
            except Exception:
                browser.pdf("temp/screenshot.pdf")

        self._save_info_file(link, title, description)
        return DirManager.move_to_numbered_dir(Path("temp"), Path(f"websites/{lang}"))

    def _save_info_file(self, link: str, title: str, description: str) -> None:
        """Создает файл с информацией о сайте."""
        data = {
            "url": link,
            "description": description if description != "null" else title,
            "time": datetime.now().isoformat()
        }
        FileManager.write_file(data, "temp/info.txt")

    def process_all(self) -> None:
        """Обрабатывает все строки в Excel-файле."""
        result_column = self.sheet.max_column
        row = 1

        while True:
            result_dir = self.process_row(row)
            if result_dir is None:
                break

            self.sheet.cell(row=row, column=result_column + 1, value=str(result_dir.absolute()))
            self.wb.save(f"{self.excel_path}")
            row += 1


if __name__ == "__main__":
    processor = WebsiteProcessor("ads.xlsx")
    processor.process_all()
