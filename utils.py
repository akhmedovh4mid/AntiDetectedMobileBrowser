import pycountry
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="country_info_app")
tf = TimezoneFinder()

def get_country_info(country_code: str) -> dict:
    country_code = country_code.upper()

    try:
        country = pycountry.countries.get(alpha_2=country_code)
        if not country:
            raise ValueError("Unknown country code")

        # Получаем полное название страны
        country_name = country.name

        # Получаем язык (условно по стране, это приближенно)
        language = f"{country_code.lower()}-{country_code.upper()}"

        # Получаем координаты страны через geopy
        location = geolocator.geocode(country_name)

        if not location:
            raise ValueError("Location not found")

        latitude = location.latitude
        longitude = location.longitude

        # Получаем временную зону по координатам
        timezone = tf.timezone_at(lng=longitude, lat=latitude)

        return {
            "language": language,
            "latitude": latitude,
            "longitude": longitude,
            "timezone": timezone or "UTC"
        }

    except Exception as e:
        return {
            "language": "en-US",
            "latitude": 0.0,
            "longitude": 0.0,
            "timezone": "UTC",
            "error": str(e)
        }
