import json
from pathlib import Path

from openpyxl import load_workbook
from datetime import datetime

from proxy.nekoray import Proxy
from browser import MobileBrowser
from manager import FileManager, DirManager

with open("proxy.json", "r") as file:
    proxy_list: dict = json.load(file)

wb = load_workbook("ads.xlsx")
sheet = wb.active
result_column = sheet.max_column

row = 1
while True:
    link = sheet.cell(row=row, column=3).value
    if link == None:
        break

    title = sheet.cell(row=row, column=4).value
    lang = sheet.cell(row=row, column=5).value.lower()
    if lang not in proxy_list.keys() and lang != "ru":
        continue

    image = sheet.cell(row=row, column=8).value
    description = sheet.cell(row=row, column=9).value

    if lang != "ru":
        proxy = proxy_list[lang]
        with Proxy(proxy["host"], proxy["port"], proxy["username"], proxy["password"]):
            with MobileBrowser(device="Pixel 7", headless=False) as browser:
                browser.goto(link, delay=3)
                is_downloaded = browser.download_website("website")

                if not is_downloaded:
                    DirManager.clear_directory(Path("temp"))
                    continue

                # browser.screenshot("temp/screenshot.png")

        if description != "null":
            FileManager.write_file({"url": link, "description": description, "time": datetime.now().isoformat()}, file_path="temp/info.txt")
        else:
            FileManager.write_file({"url": link, "description": title, "time": datetime.now().isoformat()}, file_path="temp/info.txt")

    else:
        with MobileBrowser(device="Pixel 7", headless=False) as browser:
            browser.goto(link, delay=3)
            is_downloaded = browser.download_website("website")

            if not is_downloaded:
                    DirManager.clear_directory("temp")
                    continue

            # browser.screenshot("temp/screenshot.png")

        if description != "null":
            FileManager.write_file({"url": link, "description": description, "time": datetime.now().isoformat()}, file_path="temp/info.txt")
        else:
            FileManager.write_file({"url": link, "description": title, "time": datetime.now().isoformat()}, file_path="temp/info.txt")

    dirname = DirManager.move_to_numbered_dir(Path("temp"), Path("websites"))

    sheet.cell(row=row, column=result_column + 1, value=str(dirname.absolute()))
    wb.save("ads.xlsx")

    row += 1
