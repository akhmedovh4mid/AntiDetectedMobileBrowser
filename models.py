from pathlib import Path
from typing import Optional
from pydantic import BaseModel
from datetime import datetime


class WorkUnit(BaseModel):
    link: str
    title: str
    lang: str
    image_url: str
    description: Optional[str]
    is_downloaded: bool


class ProxyUnit(BaseModel):
    host: str
    port: int
    username: str
    password: str
    timezone: str
    locale: str
    longitude: float
    lantitude: float
    zipcode: str


class WaitWorkUnit(BaseModel):
    work: WorkUnit
    proxy: Optional[ProxyUnit]
    timestamp: datetime
    attempts: int


class ResultWorkUnit(BaseModel):
    status: str
    unit: WorkUnit
    timestamp: datetime
    path: Optional[Path] = None
    context: Optional[str] = None


class ProxyManager:
    count = 0
    regions = []
    proxies = {}

    def add_proxy(self, country_name: str, value: ProxyUnit) -> None:
        self.proxies[country_name] = value
        self.regions.append(country_name)
        self.count += 1

    def get_proxy(self, country_name: str) -> ProxyUnit:
        return self.proxies.get(country_name)
