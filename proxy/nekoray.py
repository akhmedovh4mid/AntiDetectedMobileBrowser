import time
import json
import psutil
import logging
import subprocess

from pathlib import Path
from typing import Any, Dict


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(name)s] [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(name="proxy")


class Proxy:
    def __init__(
        self, host: str, port: int,
        username: str, password: str,
    ) -> None:
        """
        Инициализация прокси-клиента NekoBox Core

        :param host: Хост прокси-сервера
        :param port: Порт прокси-сервера
        :param username: Имя пользователя для аутентификации
        :param password: Пароль для аутентификации
        """
        logger.info(f"Инициализация прокси для {host}:{port} с пользователем {username}")
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.exe_path = Path("proxy/nekoray/nekobox_core.exe")
        self.config_path = Path("proxy/config.json")
        self.process = None

    def _generate_config(self) -> Dict[str, Any]:
        """
        Генерирует конфигурационный файл для NekoBox Core

        :return: Словарь с конфигурацией
        """
        logger.debug("Генерация конфигурационного файла")
        return {
            "log": {
                "level": "info"
            },
            "inbounds": [
                {
                "type": "socks",
                "tag": "socks-in",
                "listen": "127.0.0.1",
                "listen_port": 2080,
                "sniff": True
                }
            ],
            "outbounds": [
                {
                "type": "socks",
                "tag": "socks-out",
                "server": self.host,
                "server_port": self.port,
                "username": self.username,
                "password": self.password
                }
            ]
        }

    def _write_config(self) -> None:
        """Записывает конфигурационный файл на диск в формате JSON"""
        logger.debug(f"Запись конфигурации в файл {self.config_path}")
        try:
            config = self._generate_config()
            with self.config_path.open("w", encoding="utf-8") as file:
                json.dump(config, file, indent=2)
            logger.debug("Конфигурационный файл успешно записан")
        except Exception as e:
            logger.error(f"Ошибка записи конфигурации: {str(e)}")
            raise

    def start(self) -> bool:
        """
        Запускает NekoBox Core процесс

        :return: True если запуск успешен, False в случае ошибки
        """
        logger.info("Попытка запуска NekoBox Core")
        if self.is_running():
            logger.warning("NekoBox уже запущен!")
            return False

        try:
            self._write_config()
            time.sleep(0.1)

            self.process = subprocess.Popen(
                [str(self.exe_path.absolute()), "run", "-c", str(self.config_path.absolute())],
                stdout=None,
                stderr=None,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            logger.info(f"NekoBox запущен (PID: {self.process.pid})")
            return True
        except Exception as e:
            logger.error(f"Ошибка при запуске NekoBox: {str(e)}")
            return False


    def stop(self) -> bool:
        """
        Останавливает NekoBox Core процесс

        :return: True если остановка успешна, False в случае ошибки
        """
        logger.info("Попытка остановки NekoBox Core")
        if not self.is_running():
            logger.warning("NekoBox не запущен!")
            return False

        try:
            self.process.terminate()
            logger.debug("Отправлен сигнал terminate процессу")

            try:
                self.process.wait(timeout=5)
                logger.debug("Процесс завершился после terminate")
            except subprocess.TimeoutExpired:
                logger.warning("Процесс не завершился, отправка kill")
                self.process.kill()

            self.process = None
            logger.info("NekoBox успешно остановлен")
            return True
        except Exception as e:
            logger.error(f"Ошибка при остановке NekoBox: {str(e)}")
            return False


    def is_running(self) -> bool:
        """
        Проверяет, запущен ли процесс NekoBox Core

        :return: True если процесс активен, False если нет
        """
        if self.process is None:
            logger.debug("Процесс не инициализирован")
            return False

        try:
            running = psutil.Process(self.process.pid).is_running()
            logger.debug(f"Состояние процесса: {'запущен' if running else 'остановлен'}")
            return running
        except psutil.NoSuchProcess:
            logger.debug("Процесс не найден")
            return False

    def __enter__(self):
        """Поддержка контекстного менеджера - автоматический запуск при входе"""
        logger.debug("Вход в контекстный менеджер - запуск прокси")
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическая остановка при выходе из контекста"""
        logger.debug("Выход из контекстного менеджера - остановка прокси")
        self.stop()
