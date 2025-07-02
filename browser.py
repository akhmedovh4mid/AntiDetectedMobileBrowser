import os
import re
import time
import base64
import shutil
import logging
import hashlib
import requests
import concurrent

import warnings
warnings.filterwarnings("ignore")

from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from seleniumwire import webdriver
from seleniumwire.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from typing import Dict, Optional, Set, Tuple, Union


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("browser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProxyConfig:
    """Класс для конфигурации прокси с валидацией"""
    def __init__(self, host: str, port: int, username: str, password: str) -> None:
        if not all([host, port, username, password]):
            raise ValueError("All proxy parameters must be provided")

        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @property
    def seleniumwire_options(self) -> Dict[str, Dict[str, str]]:
        """Возвращает настройки прокси для Selenium Wire"""
        proxy_url = f"socks5://{self.username}:{self.password}@{self.host}:{self.port}"
        return {
            "proxy": {
                "http": proxy_url,
                "https": proxy_url,
                "no_proxy": "localhost,127.0.0.1"
            }
        }


class BrowserConfig:
    """Базовый класс конфигурации браузера"""
    def __init__(self, headless: bool = False) -> None:
        self._options = Options()
        self._setup_basic_options(headless)

    def _setup_basic_options(self, headless: bool) -> None:
        """Настройка базовых опций браузера"""
        if headless:
            self._options.add_argument("--headless=new")

        self._options.add_argument("--disable-blink-features=AutomationControlled")
        self._options.add_argument("--disable-infobars")
        self._options.add_argument("--disable-dev-shm-usage")
        self._options.add_argument("--no-sandbox")
        self._options.add_argument("--disable-gpu")
        self._options.add_argument("--disable-extensions")
        self._options.add_argument("--disable-popup-blocking")
        self._options.add_argument("--ignore-certificate-errors")
        self._options.add_argument("--disable-web-security")
        self._options.add_argument("--disable-notifications")

        self._options.add_experimental_option("useAutomationExtension", False)
        self._options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])

    @property
    def options(self) -> Options:
        """Возвращает объект настроек браузера"""
        return self._options


class MobileBrowserConfig(BrowserConfig):
    def __init__(
        self,
        user_agent: str,
        width: int,
        height: int,
        pixel_ratio: float,
        language: str = "de-DE",
        headless: bool = False
    ):
        super().__init__(headless)
        self.user_agent = user_agent
        self.width = width
        self.height = height
        self.pixel_ratio = pixel_ratio
        self.language = language
        self._setup_mobile_options()

    def _setup_mobile_options(self):
        mobile_emulation = {
            "deviceMetrics": {
                "width": self.width,
                "height": self.height,
                "pixelRatio": self.pixel_ratio,
                "touch": True
            }
        }

        self._options.add_experimental_option("mobileEmulation", mobile_emulation)
        self._options.add_argument(f"--lang={self.language}")
        self._options.add_argument("--use-mobile-user-agent")
        self._options.add_argument("--touch-events=enabled")


class GeoConfig:
    """Класс для гео-конфигурации с валидацией"""
    def __init__(
        self,
        timezone: str,
        latitude: float,
        longitude: float,
        accuracy: int = 100
    ) -> None:
        if not (-90 <= latitude <= 90):
            raise ValueError("Latitude must be between -90 and 90")
        if not (-180 <= longitude <= 180):
            raise ValueError("Longitude must be between -180 and 180")
        if accuracy <= 0:
            raise ValueError("Accuracy must be positive")

        self.timezone = timezone
        self.latitude = latitude
        self.longitude = longitude
        self.accuracy = accuracy


class UndetectBrowser:
    """Основной класс для работы с undetect browser"""
    def __init__(
        self,
        proxy_config: Optional[ProxyConfig] = None,
        browser_config: Optional[BrowserConfig | MobileBrowserConfig] = None,
        geo_config: Optional[GeoConfig] = None,
        driver_path: str = "chromedriver/chromedriver.exe",
        temp_dir: str = "temp",
        max_workers: int = 10
    ) -> None:
        self.proxy_config = proxy_config
        self.browser_config = browser_config or BrowserConfig()
        self.geo_config = geo_config
        self.driver_path = driver_path
        self.max_workers = max_workers

        self.temp = Path(temp_dir)
        self.temp.mkdir(parents=True, exist_ok=True)

        self.driver = self._create_driver()

        if self.geo_config:
            self._apply_geo_settings()
        self._mask_browser_features()

        self.page_load_count = 0
        self.start_time = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_type is not None:
            logger.error(f"Exception occurred: {exc_val}", exc_info=True)

    def _create_driver(self) -> Chrome:
        """Создает и возвращает экземпляр веб-драйвера"""
        service = Service(executable_path=self.driver_path)

        seleniumwire_options = {}
        if self.proxy_config:
            seleniumwire_options = self.proxy_config.seleniumwire_options
            seleniumwire_options.update({
                "connection_timeout": 30,  # Таймаут подключения
                "verify_ssl": False,  # Отключение проверки SSL
            })

        driver = webdriver.Chrome(
            service=service,
            seleniumwire_options=seleniumwire_options or None,
            options=self.browser_config.options
        )

        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)

        return driver

    def _apply_geo_settings(self) -> None:
        """Применяет гео-настройки с обработкой ошибок"""
        try:
            # Установка часового пояса
            self.driver.execute_cdp_cmd(
                "Emulation.setTimezoneOverride",
                {"timezoneId": self.geo_config.timezone}
            )

            # Установка геолокации
            self.driver.execute_cdp_cmd(
                "Emulation.setGeolocationOverride",
                {
                    "latitude": self.geo_config.latitude,
                    "longitude": self.geo_config.longitude,
                    "accuracy": self.geo_config.accuracy
                }
            )

            # Установка языка и локали
            self.driver.execute_cdp_cmd(
                "Emulation.setLocaleOverride",
                {"locale": self.geo_config.timezone.split("/")[-1].replace("_", "-")}
            )
        except Exception as e:
            logger.error(f"Failed to apply geo settings: {e}")

    def _mask_browser_features(self) -> None:
        """Маскирует особенности браузера для предотвращения детекта"""
        scripts = [
            # Маскировка плагинов
            """
            Object.defineProperty(navigator, 'plugins', {
                get: () => [{
                    name: 'Chrome PDF Viewer',
                    filename: 'internal-pdf-viewer',
                    description: 'Portable Document Format',
                    length: 1
                }]
            });
            """,

            # Маскировка WebGL
            """
            const getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) {
                    return 'Intel Open Source Technology Center';
                }
                if (parameter === 37446) {
                    return 'Mesa DRI Intel(R) Ivybridge Mobile ';
                }
                return getParameter.call(this, parameter);
            };
            """,

            # Маскировка WebDriver
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
            });
            """,

            # Маскировка permissions
            """
            const originalQuery = navigator.permissions.query;
            navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """
        ]

        for script in scripts:
            try:
                self.driver.execute_cdp_cmd(
                    "Page.addScriptToEvaluateOnNewDocument",
                    {"source": script}
                )
            except Exception as e:
                logger.warning(f"Failed to inject script: {e}")

    def _safe_quit(self) -> None:
        """Безопасное закрытие драйвера с расширенной обработкой ошибок"""
        if not hasattr(self, 'driver') or not self.driver:
            return

        try:
            # Попытка стандартного закрытия
            self.driver.quit()
            logger.info("Browser closed gracefully")
        except ConnectionResetError:
            # Обработка ошибки разрыва соединения
            logger.warning("Connection was reset during browser shutdown (normal in some cases)")
        except Exception as e:
            logger.error(f"Error during browser shutdown: {str(e)}")
            try:
                # Аварийное закрытие через системные команды
                if self.driver.service:
                    self.driver.service.process.kill()
            except Exception as kill_error:
                logger.error(f"Failed to kill browser process: {kill_error}")
        finally:
            self.driver = None

    def close(self) -> None:
        """Закрывает браузер и выводит статистику"""
        self._safe_quit()

        duration = time.time() - self.start_time
        logger.info(
            f"Session stats: {self.page_load_count} pages loaded "
            f"in {duration:.2f} seconds"
        )

    def save_full_page_screenshot(self, filename: str = "screenshot.png") -> str:
        """
        Сохраняет полноразмерный скриншот страницы с улучшенной обработкой
        :param filename: Имя файла для сохранения
        :return: Абсолютный путь к сохраненному файлу
        """
        try:
            # Получаем размеры всей страницы с обработкой ошибок
            total_width = self.driver.execute_script(
                "return Math.max("
                "document.body.scrollWidth, "
                "document.documentElement.scrollWidth, "
                "document.body.offsetWidth, "
                "document.documentElement.offsetWidth, "
                "document.documentElement.clientWidth"
                ")"
            )
            total_height = self.driver.execute_script(
                "return Math.max("
                "document.body.scrollHeight, "
                "document.documentElement.scrollHeight, "
                "document.body.offsetHeight, "
                "document.documentElement.offsetHeight, "
                "document.documentElement.clientHeight"
                ")"
            )

            # Устанавливаем размер окна под всю страницу
            self.driver.set_window_size(total_width, total_height)

            # Делаем скриншот через CDP (полноразмерный)
            screenshot = self.driver.execute_cdp_cmd("Page.captureScreenshot", {
                "format": "png",
                "clip": {
                    "x": 0,
                    "y": 0,
                    "width": total_width,
                    "height": total_height,
                    "scale": 1
                },
                "captureBeyondViewport": True,
                "fromSurface": True
            })

            # Сохраняем в файл
            file_path = self.temp.joinpath(filename)
            with open(file_path, "wb") as f:
                f.write(base64.b64decode(screenshot["data"]))

            logger.info(f"Screenshot saved to {file_path}")
            return str(file_path.resolve())

        except Exception as e:
            logger.error(f"Failed to take screenshot: {e}")
            raise

    def _get_page_resources(self, timeout: int = 30) -> Set[str]:
        """
        Собирает все ресурсы страницы с улучшенной обработкой
        :param timeout: Таймаут ожидания загрузки страницы
        :return: Множество URL ресурсов
        """
        resource_urls = set()

        try:
            # Ожидание полной загрузки страницы
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            last_height = self.driver.execute_script("return document.body.scrollHeight")
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Сбор всех запросов
            requests = self.driver.requests[:]
            while True:
                if len(requests) != len(self.driver.requests):
                    requests = self.driver.requests
                    time.sleep(2)
                else:
                    break

            for request in requests:
                if request.response and request.url.startswith(('http', 'https')):
                    resource_urls.add(request.url)

            return resource_urls

        except Exception as e:
            logger.error(f"Error while collecting resources: {e}")
            return resource_urls

    def _download_file(self, url: str, download_dir: Path) -> Optional[Tuple[str, str]]:
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
                headers = {
                    'User-Agent': getattr(self.browser_config, 'user_agent', 'Mozilla/5.0'),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': getattr(self.browser_config, 'language', 'en-US,en;q=0.5'),
                    'Referer': self.driver.current_url,
                }
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=10,
                    stream=True
                )
                response.raise_for_status()

                # Определение имени файла
                filename = self._generate_filename(url, response.headers.get('content-type'))
                filepath = download_dir / filename

                if not filename:
                    filename = hashlib.md5(url.encode()).hexdigest()
                    content_type = response.headers.get('content-type', '').split(';')[0]
                    if content_type == 'text/javascript':
                        filename += '.js'
                    elif content_type == 'text/css':
                        filename += '.css'
                    elif content_type == 'image/png':
                        filename += '.png'
                    elif content_type == 'image/jpeg':
                        filename += '.jpg'
                    elif content_type == 'image/webp':
                        filename += '.webp'
                    elif content_type == 'font/woff2':
                        filename += '.woff2'

                # Загрузка файла по частям
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                logger.debug(f"Downloaded {url} to {filepath}")
                return (url, str(filepath))

            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    logger.warning(f"Failed to download {url} after {max_retries} attempts: {e}")
                    return None
                time.sleep(retry_delay * (attempt + 1))

    def _generate_filename(self, url: str, content_type: Optional[str] = None) -> str:
        """
        Генерирует имя файла на основе URL и content-type
        :param url: URL ресурса
        :param content_type: Content-Type из заголовков
        :return: Сгенерированное имя файла
        """
        parsed = urlparse(url)
        path = parsed.path.lstrip('/')
        filename = os.path.basename(path) or hashlib.md5(url.encode()).hexdigest()

        # Добавление расширения по content-type
        if content_type:
            content_type = content_type.split(';')[0].strip()
            extensions = {
                'text/javascript': '.js',
                'text/css': '.css',
                'image/png': '.png',
                'image/jpeg': '.jpeg',
                'image/webp': '.webp',
                'font/woff2': '.woff2',
                'application/json': '.json',
                'text/html': '.html',
                'application/xml': '.xml',
            }
            ext = extensions.get(content_type, '')
            if ext and not filename.endswith(ext):
                filename += ext

        return filename

    def download_resources(self, urls: Set[str], download_dir: str = "downloaded_files") -> Dict[str, str]:
        """
        Загружает все ресурсы параллельно с улучшенным управлением потоками
        :param urls: Множество URL для загрузки
        :param download_dir: Директория для сохранения
        :return: Словарь {оригинальный URL: локальный путь}
        """
        download_path = self.temp / download_dir
        download_path.mkdir(parents=True, exist_ok=True)

        url_to_local = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._download_file, url, download_path): url
                for url in urls
            }

            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                    if result:
                        original_url, local_path = result
                        url_to_local[original_url] = local_path
                except Exception as e:
                    logger.error(f"Error downloading {url}: {e}")

        logger.info(f"Downloaded {len(url_to_local)}/{len(urls)} resources")
        return url_to_local


    def replace_urls_in_html(self, html_content: str, url_mapping: Dict[str, str]) -> str:
        for target_value, local_path in url_mapping.items():
            filename = ((target_value.split("?"))[0].split("/")[-1]).rsplit('.', 1)[0]

            if not filename:
                continue

            pattern = re.compile(
                rf'''(["'])([^"']*?{re.escape(filename)}[^"']*?\.[a-zA-Z0-9]+)\1''',
                re.VERBOSE | re.IGNORECASE
            )

            matches = pattern.findall(html_content)

            if len(matches) != 0:
                for match in matches[0][1:]:
                    html_content = html_content.replace(match, os.path.basename(local_path))

        return html_content


    def download_website(self, url: str, output_dir: Optional[str] = None, make_zip: bool = True) -> Tuple[str, Optional[str]]:
        """
        Скачивает веб-сайт и создает ZIP-архив
        :param url: URL сайта для скачивания
        :param output_dir: Опциональное имя поддиректории
        :param make_zip: Создавать ZIP-архив (по умолчанию True)
        :return: Кортеж (путь к папке, путь к архиву или None)
        """
        try:
            logger.info(f"Starting website download: {url}")

            # Извлекаем домен из URL
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.replace('www.', '').split(':')[0]
            folder_name = output_dir if output_dir else domain

            # Создаем директорию
            website_dir = self.temp / folder_name
            website_dir.mkdir(parents=True, exist_ok=True)

            html = self.driver.page_source
            resources = self._get_page_resources()
            url_mapping = self.download_resources(resources, folder_name)

            # Сохраняем HTML
            output_html_path = website_dir / 'index.html'
            with open(output_html_path, 'w', encoding='utf-8') as f:
                link = url
                if "?" in url:
                    link = url.split("?")[0]
                f.write(self.replace_urls_in_html(html, url_mapping))

            # Создаем архив при необходимости
            archive_path = None
            if make_zip:
                archive_path = f"{website_dir}.zip"
                self.make_archive(website_dir, archive_path)
                logger.info(f"Created archive: {archive_path}")

            logger.info(f"Website downloaded to: {website_dir}")
            return str(website_dir), archive_path

        except Exception as e:
            logger.error(f"Failed to download website {url}: {e}")
            raise

    def make_archive(self, folder_to_zip: Union[str, Path], output_archive: Union[str, Path], remove_source: bool = True) -> str:
        """
        Создает ZIP-архив из папки и (опционально) удаляет исходную папку

        :param folder_to_zip: Путь к папке для архивации
        :param output_archive: Путь к архиву (без .zip)
        :param remove_source: Удалять ли исходную папку после архивации (по умолчанию True)
        :return: Путь к созданному архиву
        :raises: RuntimeError при ошибках архивации или удаления
        """
        try:
            # Конвертация путей
            folder_path = Path(folder_to_zip) if isinstance(folder_to_zip, str) else folder_to_zip
            archive_path = Path(output_archive) if isinstance(output_archive, str) else output_archive

            # Валидация
            if not folder_path.exists():
                raise FileNotFoundError(f"Source folder not found: {folder_path}")
            if not folder_path.is_dir():
                raise NotADirectoryError(f"Source is not a directory: {folder_path}")

            # Создание родительских директорий
            archive_path.parent.mkdir(parents=True, exist_ok=True)

            # Создание архива
            archive_full_path = shutil.make_archive(
                base_name=str(archive_path.with_suffix('')),
                format='zip',
                root_dir=folder_path
            )

            # Проверка успешности создания архива
            if not Path(archive_full_path).exists():
                raise RuntimeError(f"Archive creation failed: {archive_full_path}")

            logger.info(f"Archive created successfully: {archive_full_path}")

            # Удаление исходной папки при необходимости
            if remove_source:
                try:
                    shutil.rmtree(folder_path)
                    logger.info(f"Source folder removed: {folder_path}")
                except Exception as remove_error:
                    logger.error(f"Failed to remove source folder {folder_path}: {remove_error}")
                    raise RuntimeError(f"Failed to remove source folder: {remove_error}") from remove_error

            return archive_full_path

        except Exception as e:
            logger.error(f"Archive creation failed: {str(e)}")
            raise RuntimeError(f"Failed to create archive: {str(e)}") from e

    def create_info_file(self, url: str, description: str) -> Path:
        """
        Создает файл с информацией о скачанном сайте в указанной директории

        :param url: URL сайта
        :param description: Описание сайта
        :return: Путь к созданному файлу
        :raises: OSError при проблемах с созданием файла
        """
        try:
            # Полный путь к файлу
            info_file = self.temp.joinpath("info.txt")

            # Формируем содержимое файла
            content = f"URL: {url}\nDescription: {description}\nDate: {datetime.now().isoformat()}"

            # Записываем данные в файл (в кодировке UTF-8)
            with open(info_file, 'w', encoding='utf-8') as f:
                f.write(content)

            logger.info(f"Info file created at: {info_file}")
            return info_file

        except Exception as e:
            logger.error(f"Failed to create info file: {e}")
            raise OSError(f"Could not create info file: {e}") from e

    def move_temp_to_numbered_folder(self, target_dir: Union[str, Path]) -> Path:
        """
        Переносит все содержимое папки temp в новую пронумерованную папку внутри target_dir

        :param target_dir: Путь к целевой директории где создаются нумерованные папки
        :return: Путь к созданной нумерованной папке
        :raises: OSError при проблемах с файловыми операциями
        """
        try:
            # Конвертируем в Path объекты
            target_path = Path(target_dir) if isinstance(target_dir, str) else target_dir

            # Проверяем существование директорий
            if not self.temp.exists():
                raise FileNotFoundError(f"Temp directory not found: {self.temp}")
            if not target_path.exists():
                target_path.mkdir(parents=True, exist_ok=True)

            # Проверяем, есть ли что-то в temp
            if not any(self.temp.iterdir()):
                logger.warning("Temp directory is empty, nothing to move")
                return target_path / "0"  # или можно вернуть None/вызвать исключение

            # Находим последнюю существующую нумерованную папку
            existing_folders = [
                int(f.name) for f in target_path.iterdir()
                if f.is_dir() and f.name.isdigit()
            ]

            # Определяем номер новой папки
            new_folder_num = max(existing_folders) + 1 if existing_folders else 1
            new_folder = target_path / str(new_folder_num)

            # Переносим всю папку temp в новую папку (вместо создания пустой папки)
            shutil.move(str(self.temp), str(new_folder))
            logger.info(f"Moved {self.temp} to {new_folder}")

            # Создаем новую пустую папку temp
            self.temp.mkdir(exist_ok=False)

            logger.info(f"Successfully moved all files to {new_folder}")
            return new_folder

        except Exception as e:
            logger.error(f"Failed to move files: {e}")
            raise OSError(f"File transfer failed: {e}") from e

    def clear_requests(self) -> None:
        """
        Очищает историю запросов в Selenium Wire.
        Уменьшает потребление памяти при долгой работе браузера.
        """
        if hasattr(self.driver, 'requests'):
            try:
                del self.driver.requests
                logger.debug("Successfully cleared requests history")
            except Exception as e:
                logger.warning(f"Failed to clear requests: {e}")

    def build(self, url: str, ad_image_url: str, description: str) -> None:
        self.driver.get(url)

        time.sleep(3)

        body = self.driver.find_element(By.TAG_NAME, "body")
        try:
            cloudflare = self.driver.find_element(By.CSS_SELECTOR, "body > div.main-wrapper > div > h1")
        except:
            cloudflare = None
        if "neterror" in body.get_attribute("class"):
            logger.info("Ошибка загрузки страницы (neterror)!")
            return False

        elif "502 Bad Gateway" in self.driver.title:
            logger.info("Ошибка загрузки страницы (502 Bad Gateway)!")
            return False

        elif "500 Internal Server Error" in self.driver.title:
            logger.info("Ошибка загрузки страницы (500 Internal Server Error)!")
            return False

        elif cloudflare:
            if urlparse(url).netloc in cloudflare.text:
                logger.info("Ошибка загрузки страницы (CloudFlare)!")
                return False

        self.page_load_count += 1
        self.download_website(url=url)
        self.save_full_page_screenshot()
        self._download_file(url=ad_image_url, download_dir=self.temp)
        self.create_info_file(url=url, description=description)
        self.move_temp_to_numbered_folder(target_dir=f"websites/{self.browser_config.language}")
        self.clear_requests()

        return True
