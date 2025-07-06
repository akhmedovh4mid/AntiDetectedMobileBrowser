import json
import time
from proxy.nekoray import Proxy
from browser import MobileBrowser

with open("proxy.json") as file:
    proxyies = json.load(file)

proxy = proxyies["de"]
with Proxy(proxy["host"], proxy["port"], proxy["username"], proxy["password"]):
    with MobileBrowser() as browser:
        browser.goto("https://bot.sannysoft.com")
        time.sleep(100)
