import json

from pathlib import Path
import subprocess
import time
from typing import Any, Dict
import psutil


class Proxy:
    def __init__(
        self, host: str, port: int,
        username: str, password: str,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.exe_path = Path("proxy/nekoray/nekobox_core.exe")
        self.config_path = Path("proxy/config.json")
        self.process = None

    def _generate_config(self) -> Dict[str, Any]:
        """Генерирует конфигурационный файл для NekoBox."""
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
        """Записывает конфигурационный файл на диск."""
        config = self._generate_config()
        with self.config_path.open("w", encoding="utf-8") as file:
            json.dump(config, file, indent=2)

    def start(self) -> bool:
        """
        Запускает NekoBox Core процесс.

        :return: True если запуск успешен, False в случае ошибки
        """
        if self.is_running():
            print("NekoBox уже запущен!")
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
            print(f"NekoBox запущен (PID: {self.process.pid})")
            return True
        except Exception as e:
            print(f"Ошибка при запуске NekoBox: {e}")
            return False


    def stop(self) -> bool:
        """
        Останавливает NekoBox Core процесс.

        :return: True если остановка успешна, False в случае ошибки
        """
        if not self.is_running():
            print("NekoBox не запущен!")
            return False

        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()

            self.process = None
            print("NekoBox остановлен")
            return True
        except Exception as e:
            print(f"Ошибка при остановке NekoBox: {e}")
            return False


    def is_running(self) -> bool:
        """
        Проверяет, запущен ли процесс NekoBox.

        :return: True если процесс активен, False если нет
        """
        if self.process is None:
            return False

        try:
            return psutil.Process(self.process.pid).is_running()
        except psutil.NoSuchProcess:
            return False

    def __enter__(self):
        """Поддержка контекстного менеджера."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Гарантированное завершение процесса при выходе из контекста."""
        self.stop()
