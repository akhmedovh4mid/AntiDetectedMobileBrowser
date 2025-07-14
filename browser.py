import os
import re
import time
import shutil
import logging
import requests

from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, Request, Response

from proxy.nekoray import Proxy


logging.basicConfig(
    level=logging.INFO,
    format='[%(name)s] [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(name="browser")


class Browser:
    def __init__(
        self,
        locale: Optional[str] = None,
        longitude: Optional[float] = None,
        lantitude: Optional[float] = None,
        zipcode: Optional[str] = None,
        device: str = 'Desktop Chrome',
        proxy: str = 'socks5://127.0.0.1:2080',
        headless: bool = False
    ) -> None:
        """
        Инициализация браузера с настройками геолокации и устройства

        :param locale: Локаль браузера (например, 'en-US')
        :param longitude: Долгота для геолокации
        :param lantitude: Широта для геолокации
        :param zipcode: Почтовый индекс
        :param device: Название устройства из списка Playwright
        :param proxy: Прокси-сервер в формате 'socks5://ip:port'
        :param headless: Режим без графического интерфейса
        """
        logger.info(f"Инициализация браузера с устройством: {device}, прокси: {proxy}, headless: {headless}")

        self.device = device
        self.proxy = proxy
        self.headless = headless
        self.locale = locale
        self.longitude = longitude
        self.lantitude = lantitude
        self.zipcode = zipcode

        self.requests: Optional[Set[Request]] = set()
        self.responses: Optional[Set[Response]] = set()

        self.browser = None
        self.context = None
        self.page = None

    def __enter__(self) -> 'MobileBrowser':
        """Поддержка контекстного менеджера (with) - автоматический запуск браузера"""
        logger.debug("Вход в контекстный менеджер - запуск браузера")

        self.launch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Автоматическое закрытие при выходе из контекста"""
        logger.debug("Выход из контекстного менеджера - закрытие браузера")

        self.close()

    def launch(self) -> None:
        """Запуск браузера с настройками устройства и прокси"""
        logger.info(f"Запуск браузера с устройством: {self.device}")

        try:
            self.playwright = sync_playwright().start()
            device = self.playwright.devices[self.device]

            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                proxy={'server': self.proxy} if self.proxy else None,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-automation',
                ]
            )
            if self.locale != None:
                logger.debug(f"Создание контекста с локалью: {self.locale}, геолокацией и почтовым индексом")

                self.context = self.browser.new_context(
                    **device,
                    locale=self.locale,
                    geolocation={"latitude": self.lantitude, "longitude": self.lantitude},
                    permissions=["geolocation"],
                    extra_http_headers={
                        "X-Postal-Code": self.zipcode
                    }
                )
            else:
                logger.debug("Создание контекста без локали/геолокации")
                self.context = self.browser.new_context(**device)

            self.page = self.context.new_page()

            self.page.on("request", lambda request: self.requests.add(request))
            self.page.on("response", lambda response: self.responses.add(response))

            logger.info("Браузер успешно запущен")

        except Exception as e:
            logger.error(f"Ошибка при запуске браузера: {str(e)}")
            raise


    def goto(self, url: str, delay: float = 0) -> None:
        """
        Переход по указанному URL с возможной задержкой

        :param url: Адрес для перехода
        :param delay: Задержка после загрузки (в секундах)
        """
        logger.info(f"Переход по URL: {url}")
        try:
            self.page.goto(url, wait_until="domcontentloaded", timeout=30 * 1000)

            if delay > 0:
                logger.debug(f"Ожидание {delay} секунд после загрузки страницы")
                time.sleep(delay)
        except Exception as e:
            logger.error(f"Ошибка при переходе по URL {url}: {str(e)}")
            raise

    def close(self) -> None:
        """Закрытие браузера и освобождение ресурсов"""
        logger.info("Закрытие браузера и освобождение ресурсов")
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            logger.info("Браузер успешно закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии браузера: {str(e)}")
            raise


class MobileBrowser:
    def __init__(
        self,
        timezone: Optional[str] = None,
        locale: Optional[str] = None,
        longitude: Optional[float] = None,
        lantitude: Optional[float] = None,
        zipcode: Optional[str] = None,
        device: str = 'iPhone 13',
        proxy: str = 'socks5://127.0.0.1:2080',
        headless: bool = False
    ) -> None:
        """
        Инициализация мобильного браузера с расширенными настройками

        :param locale: Локаль браузера (например, 'en-US')
        :param longitude: Долгота для геолокации
        :param lantitude: Широта для геолокации
        :param zipcode: Почтовый индекс
        :param device: Название мобильного устройства из списка Playwright
        :param proxy: Прокси-сервер в формате 'socks5://ip:port'
        :param headless: Режим без графического интерфейса
        """
        self.device = device
        self.proxy = proxy
        self.headless = headless
        self.timezone = timezone
        self.locale = locale
        self.longitude = longitude
        self.lantitude = lantitude
        self.zipcode = zipcode

        self.requests: Optional[Set[Request]] = set()
        self.responses: Optional[Set[Response]] = set()

        self.browser = None
        self.context = None
        self.page = None

        self.output_dir = Path("temp")
        self.max_workers = 3

    def __enter__(self) -> 'MobileBrowser':
        """Поддержка контекстного менеджера (with) - автоматический запуск браузера"""
        logger.debug("Вход в контекстный менеджер - запуск мобильного браузера")
        self.launch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Автоматическое закрытие при выходе из контекста"""
        logger.debug("Выход из контекстного менеджера - закрытие мобильного браузера")
        self.close()

    def _add_context_stcripts(self) -> None:
        """Добавление скриптов для эмуляции мобильного устройства и обхода детекции"""
        logger.debug("Добавление скриптов контекста для эмуляции мобильного устройства")
        scripts = [
            # Эмуляция характеристик iPhone
            """
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 6,  // iPhone 13: 6 ядер
            });
            Object.defineProperty(navigator, 'deviceMemory', {
                get: () => 4,  // iPhone 13: 4 ГБ RAM
            });
            """,
            # Обход детекции автоматизации
            """
            delete Object.getPrototypeOf(navigator).webdriver;
            window.navigator.chrome = { runtime: {}, };
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            """,
            # Эмуляция платформы
            """
            Object.defineProperty(navigator, 'platform', {
                get: () => 'iPhone'
            });
            """,
            # Эмуляция вендора
            """
            Object.defineProperty(navigator, 'vendor', {
                value: 'Apple Computer, Inc.',
                configurable: false,
                enumerable: true,
                writable: false
            });
            """,
            # Эмуляция WebGL
            """
            const originalGetParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Apple Inc.';       // VENDOR
                if (parameter === 37446) return 'Apple GPU';        // RENDERER
                return originalGetParameter.call(this, parameter);  // Остальные параметры без изменений
            };

            const originalGetParameterWebGL2 = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Apple Inc.';
                if (parameter === 37446) return 'Apple GPU';
                return originalGetParameterWebGL2.call(this, parameter);
            };
            """,
            # Эмуляция Battery API
            """
            Object.defineProperty(navigator, 'getBattery', {
                value: () => Promise.resolve({
                    charging: false,
                    level: 0.77,
                    chargingTime: Infinity,
                    dischargingTime: 8940,
                    addEventListener: () => {}
                }),
                configurable: false,
                enumerable: true,
                writable: false
            });
            """,
            # Эмуляция Touch API
            """
            Object.defineProperty(navigator, 'maxTouchPoints', {
                get: () => 5
            });
            """,
            # Эмуляция Connection API
            """
            Object.defineProperty(navigator, 'connection', {
                get: () => ({
                    downlink: 10,
                    effectiveType: "4g",
                    rtt: 50,
                    saveData: false,
                    type: "cellular"
                })
            });
            """,
            # Эмуляция Screen Orientation
            """
            Object.defineProperty(screen, 'orientation', {
                get: () => ({
                    angle: 0,
                    type: "portrait-primary",
                    onchange: null
                })
            });
            """,
            # Эмуляция Device Pixel Ratio
            """
            Object.defineProperty(window, 'devicePixelRatio', {
                get: () => 3  // Типичное значение для современных смартфонов
            });
            """,
        ]

        for script in scripts:
            self.context.add_init_script(script)

    def launch(self) -> None:
        """Запуск мобильного браузера с настройками устройства и прокси"""
        logger.info(f"Запуск мобильного браузера с устройством: {self.device}")
        try:
            self.playwright = sync_playwright().start()
            mobile = self.playwright.devices[self.device]

            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                ignore_default_args=['--enable-automation'],
                chromium_sandbox=False,
                proxy={'server': self.proxy} if self.proxy else None,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-automation',
                    '--enable-touch-events',
                    '--simulate-touch-screen-with-mouse',
                ]
            )

            if self.locale != None:
                logger.debug(f"Создание мобильного контекста с локалью: {self.locale}, геолокацией и почтовым индексом")
                self.context = self.browser.new_context(
                    **mobile,
                    color_scheme="light",
                    locale=self.locale,
                    geolocation={"latitude": self.lantitude, "longitude": self.lantitude},
                    permissions=["geolocation"],
                    extra_http_headers={
                        "X-Postal-Code": self.zipcode
                    },
                    timezone_id=self.timezone
                )
            else:
                logger.debug("Создание мобильного контекста без локали/геолокации")
                self.context = self.browser.new_context(**mobile)

            self._add_context_stcripts()
            self.page = self.context.new_page()
            self.page.emulate_media(color_scheme='light')

            # Подписка на события запросов и ответов
            self.page.on("request", lambda request: self.requests.add(request))
            self.page.on("response", lambda response: self.responses.add(response))

            logger.info("Мобильный браузер успешно запущен")
        except Exception as e:
            logger.error(f"Ошибка при запуске мобильного браузера: {str(e)}")
            raise

    def goto(self, url: str, delay: float = 0) -> None:
        """
        Переход по указанному URL с возможной задержкой

        :param url: Адрес для перехода
        :param delay: Задержка после загрузки (в секундах)
        """
        logger.info(f"Переход по URL: {url}")
        try:
            self.page.goto(url)

            if delay > 0:
                logger.debug(f"Ожидание {delay} секунд после загрузки страницы")
                time.sleep(delay)
        except Exception as e:
            logger.error(f"Ошибка при переходе по URL {url}: {str(e)}")
            raise

    def screenshot(self, screenshot_path: str) -> None:
        """
        Сохранение скриншота текущей страницы

        :param screenshot_path: Путь для сохранения файла
        """
        logger.info(f"Создание скриншота и сохранение в: {screenshot_path}")
        if not self.page:
            logger.error("Страница не инициализирована. Сначала вызовите launch()")
            raise RuntimeError("Страница не инициализирована. Сначала вызовите launch()")

        try:
            self.page.screenshot(
                path=screenshot_path,
                full_page=True,
                type="png",
                timeout=5000
            )
            logger.info("Скриншот успешно сохранен")
        except Exception as e:
            logger.error(f"Ошибка при создании скриншота: {str(e)}")
            raise

    def pdf(self, pdf_path: str) -> None:
        """
        Сохранение текущей страницы в PDF

        :param pdf_path: Путь для сохранения PDF файла
        """
        logger.info(f"Сохранение страницы в PDF: {pdf_path}")
        if not self.page:
            logger.error("Страница не инициализирована. Сначала вызовите launch()")
            raise RuntimeError("Страница не инициализирована. Сначала вызовите launch()")

        try:
            self.page.pdf(
                path=pdf_path,
                print_background=True,
                scale=1.0,
                margin={
                    "top": "0px",
                    "right": "0px",
                    "bottom": "0px",
                    "left": "0px"
                },
                prefer_css_page_size=True,
                display_header_footer=False,
                )
            logger.info("PDF успешно сохранен")
        except Exception as e:
            logger.error(f"Ошибка при сохранении PDF: {str(e)}")
            raise

    def close(self) -> None:
        """Закрытие браузера и освобождение ресурсов"""
        logger.info("Закрытие мобильного браузера и освобождение ресурсов")
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            logger.info("Мобильный браузер успешно закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии мобильного браузера: {str(e)}")
            raise

    @staticmethod
    def get_available_devices() -> List[str]:
        """Получение списка доступных устройств из Playwright"""
        logger.info("Получение списка доступных устройств")
        try:
            with sync_playwright() as playwright:
                devices = list(playwright.devices.keys())
                logger.debug(f"Доступные устройства: {devices}")
                return devices
        except Exception as e:
            logger.error(f"Ошибка при получении списка устройств: {str(e)}")
            raise

    def _wait_load_full_page(
        self,
        timeout: int = 30,
        max_scroll_attempts: int = 10,
        request_timeout: float = 2.0
    ) -> None:
        """
        Ожидание полной загрузки страницы с прокруткой и проверкой активных запросов

        :param timeout: Максимальное время ожидания в секундах
        :param max_scroll_attempts: Максимальное количество попыток прокрутки
        :param request_timeout: Таймаут ожидания завершения запросов
        """
        logger.debug("Ожидание полной загрузки страницы с прокруткой")

        # 1. Ждем загрузки DOM и сети
        try:
            self.page.wait_for_load_state("domcontentloaded", timeout=timeout * 1000)
            self.page.wait_for_load_state("networkidle", timeout=timeout * 1000)
        except Exception as e:
            logger.warning(f"Превышено время ожидания загрузки страницы: {str(e)}")

        # 2. Прокручиваем страницу до конца (для ленивой загрузки)
        last_height = self.page.evaluate("""() => {
            const bodyHeight = document.body.scrollHeight;
            return bodyHeight > 0 ? bodyHeight : document.documentElement.scrollHeight;
        }""")
        scroll_attempts = 0

        while scroll_attempts < max_scroll_attempts:
            self.page.evaluate("""() => {
                const scrollHeight = document.body.scrollHeight > 0
                    ? document.body.scrollHeight
                    : document.documentElement.scrollHeight;
                window.scrollTo(0, scrollHeight);
            }""")
            logger.debug(f"Попытка прокрутки {scroll_attempts + 1}/{max_scroll_attempts}")

            try:
                self.page.wait_for_function("""(prevHeight) => {
                    const currentHeight = document.body.scrollHeight > 0
                        ? document.body.scrollHeight
                        : document.documentElement.scrollHeight;
                    return currentHeight > prevHeight;
                }""", arg=last_height, timeout=2000)
            except Exception:
                break

            new_height = self.page.evaluate("""() => {
                const bodyHeight = document.body.scrollHeight;
                return bodyHeight > 0 ? bodyHeight : document.documentElement.scrollHeight;
            }""")
            if new_height == last_height:
                logger.debug("Высота страницы не изменилась после прокрутки")
                break

            last_height = new_height
            scroll_attempts += 1

        # 3. Ждем завершения всех активных запросов
        start_time = time.time()
        while time.time() - start_time < timeout:
            current_requests = len(self.requests)
            time.sleep(0.2)

            if current_requests == len(self.requests):
                time.sleep(request_timeout)
                if current_requests == len(self.requests):
                    logger.debug("Новых запросов не обнаружено")
                    break
            else:
                logger.debug("Обнаружены новые запросы, сброс таймера")
                start_time = time.time()

    def _download_file(self, request: Tuple[str, str], download_dir: Path) -> Optional[Tuple[str, str]]:
        """
        Загрузка файла с обработкой ошибок и повторами

        :param request: Кортеж (URL, Referer) для загрузки
        :param download_dir: Директория для сохранения
        :return: Кортеж (оригинальный URL, имя файла, Referer) или None при ошибке
        """
        max_retries = 3
        retry_delay = 1
        url = request[0]
        referer = request[1]

        logger.info(f"Загрузка файла: {url}")

        for attempt in range(max_retries):
            try:
                session = requests.Session()
                response = session.get(url, timeout=10)
                response.raise_for_status()

                # Определение имени файла
                filename = os.path.basename(url).split("?")[0]
                if not filename:
                    logger.warning(f"Не удалось определить имя файла из URL: {url}")
                    continue

                filename = f"_{filename}"
                filepath = download_dir / filename

                # Загрузка файла по частям
                with filepath.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)


                logger.info(f"Файл {url} успешно загружен в {filepath}")
                return (url, filename, referer)

            except requests.exceptions.RequestException as e:
                logger.warning(f"Попытка {attempt + 1} не удалась для {url}: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Не удалось загрузить {url} после {max_retries} попыток")
                    return None
                time.sleep(retry_delay * (attempt + 1))

    def download_resources(self, requests: Set[Request], download_dir: str = "temp") -> Dict[str, str]:
        """
        Многопоточная загрузка всех ресурсов страницы

        :param requests: Множество запросов для загрузки
        :param download_dir: Поддиректория для сохранения
        :return: Словарь сопоставления URL с локальными путями
        """
        logger.info(f"Загрузка {len(requests)} ресурсов в {download_dir}")

        download_path = self.output_dir / download_dir
        download_path.mkdir(parents=True, exist_ok=True)

        result = {}
        data = [(request.url, request.header_value("Referer")) for request in requests]

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_request = {
                executor.submit(self._download_file, request=request, download_dir=download_path): request
                for request in data
            }

            for future in as_completed(future_to_request):
                ans = future.result()
                if ans:
                    result[ans[0]] = (ans[1], ans[2])

        logger.info(f"Успешно загружено {len(result)} ресурсов")
        return result

    def replace_urls_in_html(self, html_content: str, url_mapping: Dict[str, str]) -> str:
        """
        Замена URL в HTML на локальные пути

        :param html_content: Исходный HTML
        :param url_mapping: Словарь замены URL
        :return: Модифицированный HTML
        """
        logger.debug("Замена URL в HTML на локальные пути")

        for url in url_mapping.keys():
            links = [url]

            if url_mapping[url][1]:
                replace_url = url.replace(url_mapping[url][1], "")

                url_items = replace_url.split("/")
                for i in range(len(url_items) - 1):
                    temp_url = "/".join(url_items[i:])
                    links.append(f"./{temp_url}")
                    links.append(f"/{temp_url}")
                    links.append(temp_url)

            else:
                replace_url = url.replace("https://", "").replace("http://", "")

                url_items = replace_url.split("/")
                for i in range(len(url_items) - 1):
                    temp_url = "/".join(url_items[i:])
                    links.append(f"./{temp_url}")
                    links.append(f"/{temp_url}")
                    links.append(temp_url)

            for item in links:
                html_content = html_content.replace(item, url_mapping[url][0])

        # Удаление тега base
        soup = BeautifulSoup(html_content, 'html.parser')
        for base_tag in soup.find_all('base'):
            base_tag.decompose()

        return soup.prettify()


    def download_website(self, output_subdir: Optional[str] = None, make_zip: bool = True, remove_source: bool = True) -> bool:
        """
        Полное скачивание веб-сайта с возможностью архивации

        :param output_subdir: Имя поддиректории для сохранения
        :param make_zip: Создавать ZIP-архив
        :param remove_source: Удалять исходные файлы после архивации
        :return: True при успешном выполнении, None при ошибке
        """
        logger.info(f"Начало загрузки веб-сайта: {self.page.url}")

        try:
            # Извлекаем домен из URL
            parsed_url = urlparse(self.page.url)
            domain = parsed_url.netloc.replace('www.', '').split(':')[0]
            folder_name = output_subdir if output_subdir else domain

            # Создаем директорию
            website_dir = self.output_dir / folder_name
            website_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Создана выходная директория: {website_dir}")

            # Получаем ресурсы
            self._wait_load_full_page()
            html = self.page.content()
            url_mapping = self.download_resources(self.requests, folder_name)

            output_html_path = website_dir / 'index.html'
            with open(output_html_path, 'w', encoding='utf-8') as f:
                f.write(self.replace_urls_in_html(html, url_mapping))
            logger.info(f"Модифицированный HTML сохранен в: {output_html_path}")

            # Создаем архив при необходимости
            archive_path = None
            if make_zip:
                archive_path = str(website_dir) + ".zip"
                logger.info(f"Создание архива: {archive_path}")

                shutil.make_archive(
                    base_name=str(website_dir),
                    format='zip',
                    root_dir=website_dir
                )
                logger.info(f"Архив создан: {archive_path}")

                # Удаляем исходную папку, если требуется
                if remove_source:
                    try:
                        shutil.rmtree(website_dir)
                        logger.info(f"Исходная директория удалена: {website_dir}")
                    except Exception as e:
                        logger.error(f"Ошибка при удалении директории {website_dir}: {str(e)}")

            logger.info(f"Обработка веб-сайта завершена. Архив: {archive_path}")
            return True

        except Exception as e:
            logger.error(f"Ошибка при загрузке веб-сайта {self.page.url}: {str(e)}")
            return None

# Пример использования
if __name__ == "__main__":
    with Proxy(host="proxy.soax.com", port=23319, username="k4y7tvrLJrU1dl2M", password="wifi;ca;;;"):
        with MobileBrowser() as browser:
            browser.goto('https://melkravaagency.com/?utm_source=google&utm_medium=cpc&utm_campaign=example_campaign&utm_term=example_keyword&utm_content=example_ad_content&gclid=EAIaIQobChMI2pS8sP_8_QIVFMd3Ch0xLAIyEAAYASAAEgK1CvD_AzB', delay=3)
            # time.sleep(10000)
            browser.download_website()
