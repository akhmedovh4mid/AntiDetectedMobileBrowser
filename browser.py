import base64
import math
import os
import time
import shutil
import requests

from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, List, Optional, Set, Tuple
from playwright.sync_api import sync_playwright, Request, Response
from playwright.sync_api import Error as PlaywrightError



class MobileBrowser:
    def __init__(
        self,
        device: str = 'iPhone 13',
        proxy: str = 'socks5://127.0.0.1:2080',
        headless: bool = False
    ) -> None:
        """
        Инициализация мобильного браузера

        :param device: Название устройства из списка Playwright
        :param proxy: Прокси-сервер в формате 'socks5://ip:port'
        :param headless: Режим без графического интерфейса
        """
        self.device = device
        self.proxy = proxy
        self.headless = headless
        self.requests: Optional[Set[Request]] = set()
        self.responses: Optional[Set[Response]] = set()
        self.browser = None
        self.context = None
        self.page = None

        self.output_dir = Path("temp")
        self.max_workers = 3

    def __enter__(self) -> 'MobileBrowser':
        """Поддержка контекстного менеджера (with)"""
        self.launch()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Автоматическое закрытие при выходе из контекста"""
        self.close()

    def _add_context_stcripts(self) -> None:
        self.context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
            configurable: true
        });
        delete navigator.__proto__.webdriver;
        window.cdc_adoQpoasnfa76pfcZLmcfl_Array = undefined;
        """)
        self.context.add_init_script("""
        Object.defineProperty(navigator, 'platform', {
            get: () => 'iPhone'
        });
        """)
        self.context.add_init_script("""
        Object.defineProperty(navigator, 'vendor', {
            value: 'Apple Computer, Inc.',
            configurable: false,
            enumerable: true,
            writable: false
        });
        """)
        self.context.add_init_script("""
        // Переопределяем WebGLRenderingContext
        const originalGetParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Apple Inc.';       // VENDOR
            if (parameter === 37446) return 'Apple GPU';        // RENDERER
            return originalGetParameter.call(this, parameter);  // Остальные параметры без изменений
        };

        // Переопределяем WebGL2RenderingContext
        const originalGetParameterWebGL2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Apple Inc.';
            if (parameter === 37446) return 'Apple GPU';
            return originalGetParameterWebGL2.call(this, parameter);
        };
        """)
        self.context.add_init_script("""
        // Переопределяем BatteryManager API
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
        """)

    def launch(self) -> None:
        """Запуск браузера с настройками"""
        self.playwright = sync_playwright().start()
        mobile = self.playwright.devices[self.device]

        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            proxy={'server': self.proxy} if self.proxy else None,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-automation'
            ]
        )

        self.context = self.browser.new_context(**mobile)
        self._add_context_stcripts()

        self.page = self.context.new_page()

        self.page.on("request", lambda request: self.requests.add(request))
        self.page.on("response", lambda response: self.responses.add(response))

    def goto(self, url: str, delay: float = 0) -> None:
        """
        Переход по URL

        :param url: Адрес для перехода
        :param delay: Задержка после загрузки (секунды)
        """
        self.page.goto(url)
        if delay > 0:
            time.sleep(delay)

    def screenshot(self, screenshot_path: str) -> None:
        """
        Сохраняет скриншот текущей страницы

        :param screenshot_path: Путь для сохранения файла
        :param full_page: Если True - сохраняет всю страницу с прокруткой
        """
        if not self.page:
            raise RuntimeError("Страница не инициализирована. Сначала вызовите launch()")

        self.page.screenshot(
            path=screenshot_path,
            full_page=True,
            type="png",
            timeout=5000
        )

    def pdf(self, pdf_path: str) -> None:
        if not self.page:
            raise RuntimeError("Страница не инициализирована. Сначала вызовите launch()")

        self.page.pdf(path=pdf_path)

    def close(self) -> None:
        """Закрытие браузера и освобождение ресурсов"""
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    @staticmethod
    def get_available_devices() -> List[str]:
        """Возвращает список доступных устройств"""
        with sync_playwright() as playwright:
            return list(playwright.devices.keys())

    def _wait_load_full_page(
        self, timeout: int = 30,
        max_scroll_attempts: int = 10,
        request_timeout: float = 2.0
    ) -> None:
        # 1. Ждем загрузки DOM и сети
        self.page.wait_for_load_state("domcontentloaded", timeout=timeout * 1000)
        self.page.wait_for_load_state("networkidle", timeout=timeout * 1000)

        # 2. Прокручиваем страницу до конца (для ленивой загрузки)
        last_height = self.page.evaluate("document.body.scrollHeight")
        scroll_attempts = 0

        while scroll_attempts < max_scroll_attempts:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            try:
                self.page.wait_for_function(
                    "prevHeight => document.body.scrollHeight > prevHeight",
                    arg=last_height,
                    timeout=2000,
                )
            except Exception:
                break

            new_height = self.page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
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
                    break
            else:
                start_time = time.time()

    def _download_file(self, request: Request, download_dir: Path) -> Optional[Tuple[str, str]]:
        """
        Загружает файл с улучшенной обработкой ошибок и повторами
        :param url: URL для загрузки
        :param download_dir: Директория для сохранения
        :return: Кортеж (оригинальный URL, локальный путь) или None при ошибке
        """
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                session = requests.Session()
                response = session.get(request.url, timeout=10)
                response.raise_for_status()

                # Определение имени файла
                filename = os.path.basename(request.url).split("?")[0]
                if not filename:
                    continue

                filepath = download_dir / filename

                # Загрузка файла по частям
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)


                print(f"Downloaded {request.url} to {filepath}")
                return (request.url, filename, request.header_value("Referer"))

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    print(f"Failed to download {request.url} after {max_retries} attempts: {e}")
                    return None
                time.sleep(retry_delay * (attempt + 1))

    def download_resources(self, requests: Set[Request], download_dir: str = "temp") -> Dict[str, str]:
        download_path = self.output_dir / download_dir
        download_path.mkdir(parents=True, exist_ok=True)

        result = {}

        for request in list(requests):
            ans = self._download_file(request=request, download_dir=download_path)
            if ans:
                result[ans[0]] = (ans[1], ans[2])

        return result

    def replace_urls_in_html(self, html_content: str, url_mapping: Dict[str, str]) -> str:
        """
        Заменяет URL в HTML на локальные пути
        :param html_content: Исходный HTML
        :param url_mapping: Словарь замены URL
        :return: Модифицированный HTML
        """
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

        return html_content

    def download_website(self, output_subdir: Optional[str] = None, make_zip: bool = True, remove_source: bool = True) -> Tuple[str, Optional[str]]:
        """
        Скачивает веб-сайт и создает ZIP-архив
        :param output_subdir: Опциональное имя поддиректории
        :param make_zip: Создавать ZIP-архив (по умолчанию True)
        :param remove_source: Удалять исходную папку после архивации (по умолчанию True)
        :return: Кортеж (путь к папке, путь к архиву или None)
        """
        try:
            print(f"Starting website download: {self.page.url}")

            # Извлекаем домен из URL
            parsed_url = urlparse(self.page.url)
            domain = parsed_url.netloc.replace('www.', '').split(':')[0]
            folder_name = output_subdir if output_subdir else domain

            # Создаем директорию
            website_dir = self.output_dir / folder_name
            website_dir.mkdir(parents=True, exist_ok=True)

            # Получаем ресурсы
            self._wait_load_full_page()
            html = self.page.content()
            url_mapping = self.download_resources(self.requests, folder_name)

            # Сохраняем HTML
            output_html_path = website_dir / 'index.html'
            with open(output_html_path, 'w', encoding='utf-8') as f:
                f.write(self.replace_urls_in_html(html, url_mapping))

            # Создаем архив при необходимости
            archive_path = None
            if make_zip:
                archive_path = str(website_dir) + ".zip"

                # Создаем архив
                shutil.make_archive(
                    base_name=str(website_dir),
                    format='zip',
                    root_dir=website_dir
                )
                print(f"Created archive: {archive_path}")

                # Удаляем исходную папку, если требуется
                if remove_source:
                    try:
                        shutil.rmtree(website_dir)
                        print(f"Removed source directory: {website_dir}")
                    except Exception as e:
                        print(f"Failed to remove directory {website_dir}: {e}")

            print(f"Website processing complete. Archive: {archive_path}")
            return str(website_dir), archive_path

        except Exception as e:
            print(f"Failed to download website {self.page.url}: {e}")
            return