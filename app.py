from __future__ import annotations

import csv
import io
import json
import re
import shutil
import subprocess
import threading
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from math import asin, atan2, cos, degrees, radians, sin, sqrt
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify, render_template, request


app = Flask(__name__)

AIRPORTS: dict[str, dict[str, Any]] = {
    "CYAV": {
        "name": "St. Andrews Airport",
        "lat": 50.0564,
        "lon": -97.0325,
        "elevation_ft": 760,
        "liveatc_url": None,
        "liveatc_available": False,
        "runways": [
            {"name": "04", "heading": 40},
            {"name": "22", "heading": 220},
            {"name": "13", "heading": 130},
            {"name": "31", "heading": 310},
            {"name": "18", "heading": 180},
            {"name": "36", "heading": 360},
        ],
        "frequencies": [
            {"service": "ATIS", "frequency": "125.80"},
            {"service": "Tower", "frequency": "118.50"},
            {"service": "Ground", "frequency": "121.80"},
        ],
    },
    "CYWG": {
        "name": "Winnipeg Richardson International",
        "lat": 49.9100,
        "lon": -97.2399,
        "liveatc_url": "https://www.liveatc.net/search/?icao=CYWG",
        "liveatc_available": True,
        "runways": [
            {"name": "13", "heading": 130},
            {"name": "31", "heading": 310},
            {"name": "18", "heading": 180},
            {"name": "36", "heading": 360},
        ],
        "frequencies": [
            {"service": "ATIS", "frequency": "118.30"},
            {"service": "Tower", "frequency": "118.30"},
            {"service": "Ground", "frequency": "121.90"},
        ],
    },
    "CYQK": {
        "name": "Kenora Airport",
        "lat": 49.7883,
        "lon": -94.3631,
        "liveatc_url": None,
        "liveatc_available": False,
        "runways": [
            {"name": "08", "heading": 80},
            {"name": "26", "heading": 260},
            {"name": "13", "heading": 130},
            {"name": "31", "heading": 310},
        ],
        "frequencies": [
            {"service": "ATF", "frequency": "122.80"},
            {"service": "FSS / RCO", "frequency": "123.475"},
        ],
    },
    "CYGM": {
        "name": "Gimli Industrial Park Airport",
        "lat": 50.6281,
        "lon": -97.0433,
        "liveatc_url": None,
        "liveatc_available": False,
        "runways": [
            {"name": "15", "heading": 150},
            {"name": "33", "heading": 330},
            {"name": "09", "heading": 90},
            {"name": "27", "heading": 270},
        ],
        "frequencies": [
            {"service": "ATF", "frequency": "122.80"},
            {"service": "RCO", "frequency": "122.10"},
        ],
    },
}

API_URL = "https://aviationweather.gov/api/data/metar"
NAVCAN_METAR_URL = "https://plan.navcanada.ca/weather/api/alpha/"
AIRPORTS_URL = "https://ourairports.com/data/airports.csv"
RUNWAYS_URL = "https://ourairports.com/data/runways.csv"
FREQUENCIES_URL = "https://ourairports.com/data/airport-frequencies.csv"
LOCAL_TZ = ZoneInfo("America/Winnipeg")
INHG_PER_HPA = 0.0295299830714

AIRPORT_CACHE_TTL_SECONDS = 60 * 60 * 24
AIRPORT_CACHE_FILE = Path(__file__).with_name(".airports_cache.json")

_airport_cache_lock = threading.Lock()
_airport_cache_data: dict[str, dict[str, Any]] | None = None
_airport_cache_timestamp: float = 0.0
_airport_cache_refreshing = False
_taf_fallback_cache_lock = threading.Lock()
_taf_fallback_cache: dict[str, str] = {}
_taf_fallback_cache_loaded = False
TAF_FALLBACK_CACHE_FILE = Path(__file__).with_name(".taf_fallback_cache.json")
METAR_CACHE_MAX_AGE_HOURS = 6.0
LOCAL_METAR_RECENT_MAX_AGE_HOURS = 2.0
FALLBACK_METAR_MAX_AGE_HOURS = 6.0
_metar_cache_lock = threading.Lock()
_metar_cache: dict[str, dict[str, Any]] = {}

METAR_STATIONS: dict[str, dict[str, Any]] = {
    "CYAV": {"lat": 50.0564, "lon": -97.0325},
    "CYWG": {"lat": 49.9100, "lon": -97.2399},
    "CYQK": {"lat": 49.7883, "lon": -94.3631},
    "CYGM": {"lat": 50.6281, "lon": -97.0433},
    "CYBR": {"lat": 49.9100, "lon": -99.9519},
    "CYNE": {"lat": 49.3850, "lon": -97.7890},
    "CYXL": {"lat": 50.1139, "lon": -91.9053},
}


@dataclass
class Wind:
    direction: int | None
    speed_kt: int
    gust_kt: int | None
    variable: bool = False


def normalize_angle_difference(a: float, b: float) -> float:
    diff = (a - b) % 360
    if diff > 180:
        diff -= 360
    return diff


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return radius_km * c


def _observation_epoch(observed: Any) -> float | None:
    if isinstance(observed, (int, float)):
        epoch_value = float(observed)
        if epoch_value > 1e12:
            epoch_value /= 1000.0
        return epoch_value

    if isinstance(observed, str):
        text = observed.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    return None


def _cache_metar_entry(station_code: str, metar: dict[str, Any]) -> None:
    code = station_code.strip().upper()
    if not code:
        return
    with _metar_cache_lock:
        _metar_cache[code] = dict(metar)


def _get_cached_metar_entry(station_code: str, max_age_hours: float = METAR_CACHE_MAX_AGE_HOURS) -> dict[str, Any] | None:
    code = station_code.strip().upper()
    if not code:
        return None

    with _metar_cache_lock:
        cached = dict(_metar_cache.get(code) or {})

    if not cached:
        return None

    observed = cached.get("obsTime") or cached.get("observation_time")
    observed_epoch = _observation_epoch(observed)
    if observed_epoch is None:
        return None

    age_seconds = time.time() - observed_epoch
    if age_seconds < 0:
        age_seconds = 0
    if age_seconds > max_age_hours * 3600.0:
        return None

    provider = str(cached.get("_provider") or "Unknown")
    if "cached" not in provider.lower():
        cached["_provider"] = f"{provider} (cached)"
    return cached


def _get_cached_metar_entry_any_age(station_code: str) -> dict[str, Any] | None:
    code = station_code.strip().upper()
    if not code:
        return None

    with _metar_cache_lock:
        cached = dict(_metar_cache.get(code) or {})

    if not cached:
        return None

    provider = str(cached.get("_provider") or "Unknown")
    if "cached" not in provider.lower():
        cached["_provider"] = f"{provider} (cached)"
    return cached


def metar_age_hours(observed: Any) -> float | None:
    observed_utc = parse_observed_utc(observed)
    if observed_utc is None:
        return None
    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    return max(0.0, (now_utc - observed_utc).total_seconds() / 3600.0)


def _is_metar_recent(metar: dict[str, Any], max_age_hours: float) -> bool:
    age_hours = metar_age_hours(metar.get("obsTime") or metar.get("observation_time"))
    if age_hours is None:
        return False
    return age_hours <= max_age_hours


def fetch_metar(airport: str) -> dict[str, Any] | None:
    def observation_sort_key(entry: dict[str, Any]) -> tuple[float, str]:
        observed = entry.get("obsTime") or entry.get("observation_time") or ""

        if isinstance(observed, (int, float)):
            epoch_value = float(observed)
            if epoch_value > 1e12:
                epoch_value /= 1000.0
            return epoch_value, str(observed)

        if isinstance(observed, str):
            try:
                parsed = datetime.fromisoformat(observed.replace("Z", "+00:00"))
                return parsed.timestamp(), observed
            except ValueError:
                return 0.0, observed

        return 0.0, ""

    for hours in (3, 24):
        params = {
            "ids": airport,
            "format": "json",
            "taf": "false",
            "hours": hours,
        }

        try:
            response = requests.get(API_URL, params=params, timeout=8)
            response.raise_for_status()
            if not response.text or not response.text.strip():
                continue
            data = response.json()
        except (requests.RequestException, ValueError):
            continue

        if not isinstance(data, list) or not data:
            continue

        reports = [entry for entry in data if isinstance(entry, dict)]
        if not reports:
            continue

        airport_code = airport.upper()
        exact_matches = [
            entry
            for entry in reports
            if str(entry.get("icaoId") or entry.get("station_id") or "").strip().upper() == airport_code
        ]
        candidates = exact_matches or reports

        latest = max(candidates, key=observation_sort_key)
        result = {
            **latest,
            "_provider": "AviationWeather",
        }
        station_code = str(result.get("icaoId") or result.get("station_id") or airport).strip().upper() or airport.upper()
        _cache_metar_entry(station_code, result)
        return result

    navcanada_metar = fetch_navcanada_metar(airport)
    if navcanada_metar is not None:
        station_code = str(navcanada_metar.get("icaoId") or airport).strip().upper() or airport.upper()
        _cache_metar_entry(station_code, navcanada_metar)
        return navcanada_metar

    cached = _get_cached_metar_entry(airport)
    if cached is not None:
        return cached

    return None


def _parse_signed_temperature(value: str) -> int:
    return -int(value[1:]) if value.startswith("M") else int(value)


def _parse_altimeter_from_raw(raw_text: str) -> float | None:
    match = re.search(r"\bA(\d{4})\b", raw_text)
    if not match:
        return None
    return int(match.group(1)) / 100.0


def _parse_visibility_from_raw(raw_text: str) -> str | None:
    match = re.search(r"\b(P?\d{1,2}(?:/\d)?|\d/\d)SM\b", raw_text)
    if not match:
        return None
    return match.group(1)


def _parse_wind_from_raw(raw_text: str) -> tuple[str | int | None, int | None, int | None]:
    match = re.search(r"\b(VRB|\d{3})(\d{2,3})(?:G(\d{2,3}))?KT\b", raw_text)
    if not match:
        return None, None, None

    direction_token = match.group(1)
    direction: str | int
    if direction_token == "VRB":
        direction = "VRB"
    else:
        direction = int(direction_token)

    speed = int(match.group(2))
    gust = int(match.group(3)) if match.group(3) else None
    return direction, speed, gust


def _parse_temp_dew_from_raw(raw_text: str) -> tuple[int | None, int | None]:
    match = re.search(r"\b(M?\d{2})/(M?\d{2})\b", raw_text)
    if not match:
        return None, None
    return _parse_signed_temperature(match.group(1)), _parse_signed_temperature(match.group(2))


def fetch_navcanada_metar(airport: str) -> dict[str, Any] | None:
    params = {
        "site": airport,
        "alpha": "metar",
    }

    try:
        response = requests.get(NAVCAN_METAR_URL, params=params, timeout=8)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError):
        return None

    if not isinstance(data, dict):
        return None

    reports = data.get("data")
    if not isinstance(reports, list):
        return None

    airport_code = airport.upper()
    candidates: list[dict[str, Any]] = []
    for entry in reports:
        if not isinstance(entry, dict):
            continue
        location = str(entry.get("location") or "").strip().upper()
        if location != airport_code:
            continue
        text = str(entry.get("text") or "").strip()
        if not text:
            continue
        candidates.append(entry)

    if not candidates:
        return None

    latest = max(candidates, key=lambda item: str(item.get("startValidity") or ""))
    raw_text = str(latest.get("text") or "").strip()
    observed = str(latest.get("startValidity") or "").strip()

    wind_dir, wind_speed, wind_gust = _parse_wind_from_raw(raw_text)
    temperature, dewpoint = _parse_temp_dew_from_raw(raw_text)
    visibility = _parse_visibility_from_raw(raw_text)
    altimeter = _parse_altimeter_from_raw(raw_text)

    return {
        "icaoId": airport_code,
        "rawOb": raw_text,
        "obsTime": observed if observed else None,
        "wdir": wind_dir,
        "wspd": wind_speed,
        "wgst": wind_gust,
        "temp": temperature,
        "dewp": dewpoint,
        "visib": visibility,
        "altim": altimeter,
        "_provider": "NAV CANADA",
    }


def _extract_navcan_notam_text(raw_field: Any) -> str:
    if not isinstance(raw_field, str) or not raw_field.strip():
        return ""

    try:
        parsed = json.loads(raw_field)
    except json.JSONDecodeError:
        return raw_field.strip()

    if not isinstance(parsed, dict):
        return raw_field.strip()

    for key in ("raw", "english", "french"):
        value = parsed.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return raw_field.strip()


def fetch_navcan_alpha_records(airport: str, product: str, max_items: int = 30) -> list[dict[str, str]]:
    params = {
        "site": airport,
        "alpha": product,
    }

    try:
        response = requests.get(NAVCAN_METAR_URL, params=params, timeout=8)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []

    if not isinstance(payload, dict):
        return []

    raw_records = payload.get("data")
    if not isinstance(raw_records, list):
        return []

    normalized: list[dict[str, str]] = []
    for entry in raw_records:
        if not isinstance(entry, dict):
            continue

        location = str(entry.get("location") or airport).strip().upper() or airport.upper()
        start_time = str(entry.get("startValidity") or "").strip()
        end_time = str(entry.get("endValidity") or "").strip()
        start_local, start_zulu = format_local_and_zulu(start_time)
        end_local, end_zulu = format_local_and_zulu(end_time)

        text_value = entry.get("formattedBulletin") or entry.get("text") or ""
        if product == "notam":
            text = _extract_navcan_notam_text(text_value)
        else:
            text = str(text_value).strip()

        if not text:
            continue

        normalized.append(
            {
                "location": location,
                "start": start_time or "N/A",
                "end": end_time or "N/A",
                "start_local": start_local,
                "start_zulu": start_zulu,
                "end_local": end_local,
                "end_zulu": end_zulu,
                "text": text,
            }
        )

        if len(normalized) >= max_items:
            break

    return normalized


def _station_coords(code: str, airport_index: dict[str, dict[str, Any]]) -> tuple[float, float] | None:
    airport = airport_index.get(code)
    if airport:
        lat = airport.get("lat")
        lon = airport.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return float(lat), float(lon)

    station = METAR_STATIONS.get(code)
    if station:
        return station["lat"], station["lon"]

    return None


def _load_taf_fallback_cache_if_needed() -> None:
    global _taf_fallback_cache_loaded

    with _taf_fallback_cache_lock:
        if _taf_fallback_cache_loaded:
            return

        try:
            raw = TAF_FALLBACK_CACHE_FILE.read_text(encoding="utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for requested, station in parsed.items():
                    if isinstance(requested, str) and isinstance(station, str):
                        _taf_fallback_cache[requested.upper()] = station.upper()
        except (OSError, json.JSONDecodeError):
            pass

        _taf_fallback_cache_loaded = True


def _save_taf_fallback_cache() -> None:
    with _taf_fallback_cache_lock:
        payload = dict(sorted(_taf_fallback_cache.items(), key=lambda item: item[0]))

    try:
        TAF_FALLBACK_CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        return


def bearing_degrees(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_lon = radians(lon2 - lon1)

    y = sin(delta_lon) * cos(phi2)
    x = cos(phi1) * sin(phi2) - sin(phi1) * cos(phi2) * cos(delta_lon)
    return (degrees(atan2(y, x)) + 360.0) % 360.0


def cardinal_direction_from_bearing(bearing: float) -> str:
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    index = int((bearing + 22.5) // 45) % 8
    return directions[index]


def decode_taf_segments(raw_taf: str) -> list[dict[str, str]]:
    compact = " ".join(raw_taf.split())
    if not compact:
        return []

    header_match = re.match(
        r"^(TAF(?:\s+AMD|\s+COR)?\s+[A-Z]{4}\s+\d{6}Z\s+\d{4}/\d{4})\s*(.*)$",
        compact,
        flags=re.IGNORECASE,
    )
    if not header_match:
        return [{"point": "Forecast", "decoded": decode_taf_text(compact)}]

    remainder = header_match.group(2).strip()
    segment_pattern = re.compile(
        r"\b(FM\d{6}|TEMPO\s+\d{4}/\d{4}|BECMG\s+\d{4}/\d{4}|PROB(?:30|40)(?:\s+TEMPO)?\s+\d{4}/\d{4})\b",
        flags=re.IGNORECASE,
    )
    matches = list(segment_pattern.finditer(remainder))

    rows: list[dict[str, str]] = []
    if not matches:
        initial = remainder or "No forecast details"
        rows.append({"point": "Initial", "decoded": decode_taf_text(initial)})
        return rows

    initial_text = remainder[: matches[0].start()].strip()
    if initial_text:
        rows.append({"point": "Initial", "decoded": decode_taf_text(initial_text)})

    for index, match in enumerate(matches):
        token = match.group(1).upper()
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(remainder)
        segment_text = remainder[start:end].strip()
        point_label = token
        if token.startswith("FM") and len(token) == 8:
            point_label = f"From {token[2:4]} {token[4:6]}:{token[6:8]}Z"
        decoded_text = decode_taf_text(segment_text) if segment_text else "No additional details"
        rows.append({"point": point_label, "decoded": decoded_text})

    return rows


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    month_index = (year * 12 + (month - 1)) + delta
    shifted_year = month_index // 12
    shifted_month = (month_index % 12) + 1
    return shifted_year, shifted_month


def _resolve_day_hour_minute_utc(day: int, hour: int, minute: int, reference: datetime) -> datetime | None:
    candidates: list[datetime] = []
    for delta in (-1, 0, 1):
        year, month = _shift_month(reference.year, reference.month, delta)
        try:
            candidate = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("UTC"))
            candidates.append(candidate)
        except ValueError:
            continue

    if not candidates:
        return None
    return min(candidates, key=lambda candidate: abs((candidate - reference).total_seconds()))


def _format_friendly_local_time(local_dt: datetime) -> str:
    now_local = datetime.now(LOCAL_TZ)
    local_date = local_dt.date()
    if local_date == now_local.date():
        prefix = "Today"
    elif local_date == (now_local + timedelta(days=1)).date():
        prefix = "Tomorrow"
    else:
        prefix = local_dt.strftime("%b %d")
    time_part = local_dt.strftime("%I:%M %p %Z").lstrip("0")
    return f"{prefix} at {time_part}"


def _friendly_day_label(local_dt: datetime) -> str:
    now_local = datetime.now(LOCAL_TZ)
    local_date = local_dt.date()
    if local_date == now_local.date():
        return "Today"
    if local_date == (now_local + timedelta(days=1)).date():
        return "Tomorrow"
    return local_dt.strftime("%b %d")


def _compact_window_lines(start_local: datetime, end_local: datetime) -> tuple[str, str]:
    start_day = _friendly_day_label(start_local)
    end_day = _friendly_day_label(end_local)
    start_time = start_local.strftime("%I:%M%p %Z").lstrip("0")
    end_time = end_local.strftime("%I:%M%p").lstrip("0").lower()

    if start_local.date() == end_local.date():
        line = f"{start_time} until {end_time}"
    else:
        line = f"{start_time} until {end_day} {end_time}"

    return start_day, line


def _decode_weather_token(token: str) -> str | None:
    normalized = token.upper().replace("−", "-")
    if not normalized:
        return None

    intensity_text = ""
    if normalized[0] in {"-", "+"}:
        intensity_text = "Light" if normalized[0] == "-" else "Heavy"
        normalized = normalized[1:]

    in_vicinity = False
    if normalized.startswith("VC"):
        in_vicinity = True
        normalized = normalized[2:]

    descriptors = {
        "MI": "Shallow",
        "BC": "Patches",
        "PR": "Partial",
        "DR": "Low drifting",
        "BL": "Blowing",
        "SH": "Showers",
        "TS": "Thunderstorm",
        "FZ": "Freezing",
    }
    precipitation = {
        "DZ": "drizzle",
        "RA": "rain",
        "SN": "snow",
        "SG": "snow grains",
        "IC": "ice crystals",
        "PL": "ice pellets",
        "GR": "hail",
        "GS": "small hail",
        "UP": "unknown precipitation",
    }
    obscuration = {
        "BR": "mist",
        "FG": "fog",
        "FU": "smoke",
        "VA": "volcanic ash",
        "DU": "widespread dust",
        "SA": "sand",
        "HZ": "haze",
        "PY": "spray",
    }
    other = {
        "PO": "dust/sand whirls",
        "SQ": "squalls",
        "FC": "funnel cloud",
        "SS": "sandstorm",
        "DS": "duststorm",
    }

    descriptor_list: list[str] = []
    while len(normalized) >= 2 and normalized[:2] in descriptors:
        descriptor_list.append(normalized[:2])
        normalized = normalized[2:]

    phenomenon_list: list[str] = []
    while len(normalized) >= 2 and normalized[:2] in {**precipitation, **obscuration, **other}:
        phenomenon_list.append(normalized[:2])
        normalized = normalized[2:]

    if normalized:
        return None
    if not descriptor_list and not phenomenon_list:
        return None

    words: list[str] = []
    if intensity_text:
        words.append(intensity_text)

    if "TS" in descriptor_list and any(code in precipitation for code in phenomenon_list):
        precip_words = " and ".join(precipitation[code] for code in phenomenon_list if code in precipitation)
        words.append(f"Thunderstorm with {precip_words}")
    elif "SH" in descriptor_list and any(code in precipitation for code in phenomenon_list):
        precip_words = " and ".join(precipitation[code] for code in phenomenon_list if code in precipitation)
        words.append(f"Showers of {precip_words}")
    elif "SH" in descriptor_list and not phenomenon_list:
        words.append("Showers")
    else:
        for code in descriptor_list:
            if code not in {"TS", "SH"}:
                words.append(descriptors[code])

        for code in phenomenon_list:
            if code in precipitation:
                words.append(precipitation[code])
            elif code in obscuration:
                words.append(obscuration[code])
            elif code in other:
                words.append(other[code])

    if in_vicinity:
        words.append("in the vicinity")

    return " ".join(words).strip().capitalize()


def _extract_taf_components(text: str, inherited: dict[str, Any] | None = None) -> dict[str, Any]:
    tokens = text.split()

    wind = inherited["wind"] if inherited and inherited.get("wind") else "N/A"
    visibility = inherited["visibility"] if inherited and inherited.get("visibility") else "N/A"
    clouds = inherited["clouds"] if inherited and inherited.get("clouds") else "N/A"
    other_parts: list[str] = []
    ws_hazard = False
    ws_alert: str | None = None

    wind_pattern = re.compile(r"^(VRB|\d{3})(\d{2,3})(?:G(\d{2,3}))?KT$")
    cloud_pattern = re.compile(r"^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$")
    visibility_pattern = re.compile(r"^\d+(?:/\d+)?SM$")
    cloud_map = {
        "FEW": "Few",
        "SCT": "Scattered",
        "BKN": "Broken",
        "OVC": "Overcast",
    }

    cloud_parts: list[str] = []
    ceiling_candidates_ft: list[int] = []

    index = 0
    while index < len(tokens):
        token = tokens[index]
        upper = token.upper().replace("−", "-")

        ws_match = re.match(r"^WS(\d{3})/(\d{3})(\d{2,3})KT$", upper)
        if ws_match:
            ws_altitude_ft = int(ws_match.group(1)) * 100
            ws_direction = int(ws_match.group(2))
            ws_speed = int(ws_match.group(3))
            ws_alert = (
                f"{upper} — LOW LEVEL WINDSHEAR at {ws_altitude_ft} ft AGL, "
                f"{ws_direction:03d}° at {ws_speed} kt"
            )
            ws_hazard = True
            index += 1
            continue

        wind_match = wind_pattern.match(upper)
        if wind_match:
            direction = wind_match.group(1)
            speed = int(wind_match.group(2))
            gust = wind_match.group(3)
            if direction == "VRB":
                wind = f"Variable at {speed} kts"
            elif gust:
                wind = f"{int(direction):03d}° at {speed} - {int(gust)} kts"
            else:
                wind = f"{int(direction):03d}° at {speed} kts"
            index += 1
            continue

        if upper == "P6SM":
            visibility = "6+ sm"
            index += 1
            continue

        if upper.isdigit() and (index + 1) < len(tokens):
            next_upper = tokens[index + 1].upper().replace("−", "-")
            if re.match(r"^\d/\dSM$", next_upper):
                visibility = f"{upper} {next_upper[:-2]} sm"
                index += 2
                continue

        if upper.endswith("SM") and visibility_pattern.match(upper):
            visibility = f"{upper[:-2]} sm"
            index += 1
            continue

        cloud_match = cloud_pattern.match(upper)
        if cloud_match:
            layer = cloud_map[cloud_match.group(1)]
            altitude_ft = int(cloud_match.group(2)) * 100
            cloud_parts.append(f"{layer} {altitude_ft:,}'")
            if cloud_match.group(1) in {"BKN", "OVC"}:
                ceiling_candidates_ft.append(altitude_ft)
            index += 1
            continue

        if upper.startswith("VV") and len(upper) == 5 and upper[2:].isdigit():
            altitude_ft = int(upper[2:]) * 100
            cloud_parts.append(f"Vertical visibility {altitude_ft:,}'")
            ceiling_candidates_ft.append(altitude_ft)
            index += 1
            continue

        if upper in {"SKC", "CLR", "NSC", "NCD"}:
            cloud_parts.append("Clear")
            index += 1
            continue

        if upper == "CAVOK":
            visibility = "6+ sm"
            clouds = "No significant cloud below 5,000'"
            index += 1
            continue

        decoded_weather = _decode_weather_token(upper)
        if decoded_weather:
            other_parts.append(decoded_weather)
            index += 1
            continue

        if upper in {"RMK", "NXT", "FCST", "BY", "AMD", "COR"}:
            if upper == "RMK":
                remark_text = " ".join(tokens[index + 1 :]).strip("=").strip()
                if remark_text:
                    other_parts.append(f"Remark: {remark_text}")
                break
            index += 1
            continue

        if upper.endswith("="):
            stripped = upper.rstrip("=")
            if stripped:
                other_parts.append(stripped)
            index += 1
            continue

        if any(upper.startswith(prefix) for prefix in ("RMK", "WS", "PROB", "TEMPO", "BECMG", "FM")):
            index += 1
            continue

        if upper.isdigit() and len(upper) in {4, 6}:
            index += 1
            continue

        if re.match(r"^\d{4}/\d{4}$", upper):
            index += 1
            continue

        if upper not in {"CAVOK", "NSC", "NCD", "SKC", "CLR"}:
            other_parts.append(token)
        index += 1

    if cloud_parts:
        clouds = ", ".join(cloud_parts)

    ceiling_ft = min(ceiling_candidates_ft) if ceiling_candidates_ft else None

    return {
        "wind": wind,
        "visibility": visibility,
        "clouds": clouds,
        "other": ", ".join(dict.fromkeys(other_parts)) if other_parts else "None",
        "ws_hazard": ws_hazard,
        "ws_alert": ws_alert,
        "ceiling_ft": ceiling_ft,
    }


def build_taf_decoded_rows(taf_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    display_rows: list[dict[str, str]] = []

    for row in taf_rows:
        raw_taf = " ".join((row.get("text") or "").split())
        if not raw_taf:
            continue

        start_utc = parse_observed_utc(row.get("start"))
        end_utc = parse_observed_utc(row.get("end"))
        reference_utc = start_utc or datetime.now(tz=ZoneInfo("UTC"))

        header_match = re.match(
            r"^(TAF(?:\s+AMD|\s+COR)?\s+[A-Z]{4}\s+(\d{2})(\d{2})(\d{2})Z\s+(\d{2})(\d{2})/(\d{2})(\d{2}))\s*(.*)$",
            raw_taf,
            flags=re.IGNORECASE,
        )

        if not header_match:
            components = _extract_taf_components(raw_taf)
            category = _flight_category(components.get("ceiling_ft"), _parse_visibility_sm(components.get("visibility")))
            display_rows.append(
                {
                    "location": row.get("location", "N/A"),
                    "title": "FORECAST",
                    "title_zulu": "N/A",
                    "window_from": "N/A",
                    "window_until": "N/A",
                    "window_day": "",
                    "window_line": "",
                    "is_tempo": False,
                    "wind": components["wind"],
                    "visibility": components["visibility"],
                    "clouds": components["clouds"],
                    "other": components["other"],
                    "ws_hazard": components["ws_hazard"],
                    "ws_alert": components["ws_alert"],
                    "flight_category_label": category["label"],
                    "flight_category_color": category["color"],
                    "expires": "N/A",
                    "expires_zulu": "N/A",
                    "raw": raw_taf,
                }
            )
            continue

        _, issue_day, issue_hour, issue_min, valid_start_day, valid_start_hour, valid_end_day, valid_end_hour, remainder = (
            header_match.groups()
        )
        issue_utc = _resolve_day_hour_minute_utc(int(issue_day), int(issue_hour), int(issue_min), reference_utc)
        valid_start_utc = _resolve_day_hour_minute_utc(int(valid_start_day), int(valid_start_hour), 0, reference_utc)
        valid_end_utc = _resolve_day_hour_minute_utc(int(valid_end_day), int(valid_end_hour), 0, reference_utc)
        if valid_end_utc is not None and valid_start_utc is not None and valid_end_utc <= valid_start_utc:
            valid_end_utc = valid_end_utc + timedelta(days=1)

        if start_utc is None:
            start_utc = valid_start_utc or issue_utc
        if end_utc is None:
            end_utc = valid_end_utc

        marker_pattern = re.compile(
            r"\b(FM\d{6}|TEMPO\s+\d{4}/\d{4}|BECMG\s+\d{4}/\d{4}|PROB(?:30|40)(?:\s+TEMPO)?\s+\d{4}/\d{4})\b",
            flags=re.IGNORECASE,
        )
        matches = list(marker_pattern.finditer(remainder.strip()))

        base_text = remainder[: matches[0].start()].strip() if matches else remainder.strip()
        overlays: list[dict[str, Any]] = []
        fm_events: list[dict[str, Any]] = []

        for i, match in enumerate(matches):
            token = match.group(1).upper()
            next_pos = matches[i + 1].start() if i + 1 < len(matches) else len(remainder)
            segment_text = remainder[match.end() : next_pos].strip()

            if token.startswith("FM"):
                day = int(token[2:4])
                hour = int(token[4:6])
                minute = int(token[6:8])
                fm_time = _resolve_day_hour_minute_utc(day, hour, minute, reference_utc)
                fm_events.append({"time": fm_time, "text": segment_text, "raw": f"{token} {segment_text}".strip()})
                continue

            period_match = re.search(r"(\d{2})(\d{2})/(\d{2})(\d{2})", token)
            if not period_match:
                continue

            start_day = int(period_match.group(1))
            start_hour = int(period_match.group(2))
            end_day = int(period_match.group(3))
            end_hour = int(period_match.group(4))
            overlay_start = _resolve_day_hour_minute_utc(start_day, start_hour, 0, reference_utc)
            overlay_end = _resolve_day_hour_minute_utc(end_day, end_hour, 0, reference_utc)
            if overlay_start and overlay_end and overlay_end <= overlay_start:
                overlay_end = overlay_end + timedelta(days=1)

            probability_text = ""
            probability_match = re.match(r"PROB(30|40)", token)
            if probability_match:
                probability_text = f"Probability {probability_match.group(1)}%"

            overlays.append(
                {
                    "type": "TEMPO" if "TEMPO" in token else "BECMG" if token.startswith("BECMG") else "PROB",
                    "start": overlay_start,
                    "end": overlay_end,
                    "text": segment_text,
                    "probability_text": probability_text,
                    "raw": f"{token} {segment_text}".strip(),
                }
            )

        prevailing_segments: list[dict[str, Any]] = []
        current_text = base_text
        current_start = start_utc

        sorted_fm = sorted([event for event in fm_events if event["time"] is not None], key=lambda e: e["time"])
        for event in sorted_fm:
            if current_start is not None and event["time"] is not None and event["time"] > current_start:
                prevailing_segments.append(
                    {
                        "start": current_start,
                        "end": event["time"],
                        "text": current_text,
                        "raw": current_text,
                    }
                )
            current_text = event["text"]
            current_start = event["time"]

        if current_start is not None:
            prevailing_segments.append(
                {
                    "start": current_start,
                    "end": end_utc,
                    "text": current_text,
                    "raw": current_text,
                }
            )

        row_counter = 0

        for segment in prevailing_segments:
            seg_start_utc = segment["start"]
            seg_end_utc = segment["end"]
            if seg_start_utc is None or seg_end_utc is None or seg_end_utc <= seg_start_utc:
                continue

            seg_start_local = seg_start_utc.astimezone(LOCAL_TZ)
            seg_end_local = seg_end_utc.astimezone(LOCAL_TZ)
            window_day, window_line = _compact_window_lines(seg_start_local, seg_end_local)
            title_zulu = seg_start_utc.strftime("%Y-%m-%d %H:%MZ")
            expires_zulu = seg_end_utc.strftime("%Y-%m-%d %H:%MZ")

            now_utc = datetime.now(tz=ZoneInfo("UTC"))
            title_text = _format_friendly_local_time(seg_start_local).upper()
            if row_counter == 0 and seg_start_utc <= now_utc < seg_end_utc:
                title_text = f"{title_text} (CURRENT)"

            components = _extract_taf_components(segment["text"])
            category = _flight_category(components.get("ceiling_ft"), _parse_visibility_sm(components.get("visibility")))
            display_rows.append(
                {
                    "location": row.get("location", "N/A"),
                    "title": title_text,
                    "title_zulu": title_zulu,
                    "window_from": _format_friendly_local_time(seg_start_local),
                    "window_until": _format_friendly_local_time(seg_end_local),
                    "window_day": window_day,
                    "window_line": window_line,
                    "is_tempo": False,
                    "wind": components["wind"],
                    "visibility": components["visibility"],
                    "clouds": components["clouds"],
                    "other": components["other"],
                    "ws_hazard": components["ws_hazard"],
                    "ws_alert": components["ws_alert"],
                    "flight_category_label": category["label"],
                    "flight_category_color": category["color"],
                    "expires": _format_friendly_local_time(seg_end_local),
                    "expires_zulu": expires_zulu,
                    "raw": segment["raw"] or segment["text"],
                }
            )
            row_counter += 1

            for overlay in overlays:
                overlay_start = overlay["start"]
                overlay_end = overlay["end"]
                if overlay_start is None or overlay_end is None:
                    continue
                if overlay_start < seg_start_utc or overlay_end > seg_end_utc:
                    continue

                overlay_start_local = overlay_start.astimezone(LOCAL_TZ)
                overlay_end_local = overlay_end.astimezone(LOCAL_TZ)
                overlay_components = _extract_taf_components(overlay["text"], inherited=components)
                overlay_category = _flight_category(
                    overlay_components.get("ceiling_ft"),
                    _parse_visibility_sm(overlay_components.get("visibility")),
                )

                same_as_parent = overlay_start == seg_start_utc and overlay_end == seg_end_utc
                window_from = _format_friendly_local_time(overlay_start_local)
                window_until = _format_friendly_local_time(overlay_end_local)
                title_local = "TEMPORARY" if same_as_parent else _format_friendly_local_time(overlay_start_local).upper()

                display_rows.append(
                    {
                        "location": row.get("location", "N/A"),
                        "title": title_local,
                        "title_zulu": overlay_start.strftime("%Y-%m-%d %H:%MZ"),
                        "window_from": window_from,
                        "window_until": window_until,
                        "is_tempo": overlay["type"] == "TEMPO",
                        "omit_window_range": same_as_parent and overlay["type"] == "TEMPO",
                        "wind": overlay_components["wind"],
                        "visibility": overlay_components["visibility"],
                        "clouds": overlay_components["clouds"],
                        "other": overlay_components["other"],
                        "probability_text": str(overlay.get("probability_text") or ""),
                        "ws_hazard": overlay_components["ws_hazard"],
                        "ws_alert": overlay_components["ws_alert"],
                        "flight_category_label": overlay_category["label"],
                        "flight_category_color": overlay_category["color"],
                        "expires": window_until,
                        "expires_zulu": overlay_end.strftime("%Y-%m-%d %H:%MZ"),
                        "raw": overlay["raw"],
                    }
                )
                if overlay.get("probability_text"):
                    existing_other = str(display_rows[-1].get("other") or "").strip()
                    probability_text = str(overlay["probability_text"])
                    if existing_other and existing_other not in {"None", "N/A"}:
                        display_rows[-1]["other"] = f"{probability_text}; {existing_other}"
                    else:
                        display_rows[-1]["other"] = probability_text
                window_day, window_line = _compact_window_lines(overlay_start_local, overlay_end_local)
                display_rows[-1]["window_day"] = window_day
                display_rows[-1]["window_line"] = window_line

    return display_rows


def latest_taf_issue_time(taf_rows: list[dict[str, str]]) -> datetime | None:
    latest_issue: datetime | None = None

    for row in taf_rows:
        raw_taf = " ".join((row.get("text") or "").split())
        if not raw_taf:
            continue

        header_match = re.match(
            r"^TAF(?:\s+AMD|\s+COR)?\s+[A-Z]{4}\s+(\d{2})(\d{2})(\d{2})Z",
            raw_taf,
            flags=re.IGNORECASE,
        )
        if not header_match:
            continue

        issue_day, issue_hour, issue_min = header_match.groups()
        reference_utc = parse_observed_utc(row.get("start")) or datetime.now(tz=ZoneInfo("UTC"))
        issue_utc = _resolve_day_hour_minute_utc(int(issue_day), int(issue_hour), int(issue_min), reference_utc)
        if issue_utc is None:
            continue

        if latest_issue is None or issue_utc > latest_issue:
            latest_issue = issue_utc

    return latest_issue


def format_local_and_zulu(time_value: Any) -> tuple[str, str]:
    parsed = parse_observed_utc(time_value)
    if parsed is None:
        return "N/A", "N/A"
    local_value = parsed.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")
    zulu_value = parsed.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%MZ")
    return local_value, zulu_value


def normalize_altimeter_inhg(value: Any) -> float | None:
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric >= 100:
            return numeric * INHG_PER_HPA
        return numeric

    text = str(value).strip().upper()
    if not text:
        return None

    if text.startswith("A") and text[1:].isdigit() and len(text[1:]) == 4:
        return int(text[1:]) / 100.0

    if text.startswith("Q") and text[1:].isdigit() and len(text[1:]) == 4:
        return int(text[1:]) * INHG_PER_HPA

    try:
        numeric = float(text)
    except ValueError:
        return None

    if numeric >= 100:
        return numeric * INHG_PER_HPA
    return numeric


def decode_taf_text(text: str) -> str:
    tokens = text.split()
    decoded_parts: list[str] = []

    wind_pattern = re.compile(r"^(VRB|\d{3})(\d{2,3})(?:G(\d{2,3}))?KT$")
    cloud_pattern = re.compile(r"^(FEW|SCT|BKN|OVC)(\d{3})(CB|TCU)?$")

    cloud_map = {
        "FEW": "Few clouds",
        "SCT": "Scattered clouds",
        "BKN": "Broken clouds",
        "OVC": "Overcast",
    }

    weather_map = {
        "RA": "Rain",
        "SN": "Snow",
        "BR": "Mist",
        "FG": "Fog",
        "HZ": "Haze",
        "TS": "Thunderstorm",
        "DZ": "Drizzle",
        "SH": "Showers",
    }

    for token in tokens:
        upper = token.upper()

        wind_match = wind_pattern.match(upper)
        if wind_match:
            direction = wind_match.group(1)
            speed = int(wind_match.group(2))
            gust = wind_match.group(3)
            if direction == "VRB":
                wind_text = f"Variable wind at {speed} knots"
            else:
                wind_text = f"{int(direction)} degrees at {speed} knots"
            if gust is not None:
                wind_text += f", gusting {int(gust)} knots"
            decoded_parts.append(wind_text)
            continue

        if upper == "P6SM":
            decoded_parts.append("Visibility more than 6 statute miles")
            continue

        if upper.endswith("SM") and re.match(r"^\d+(?:/\d+)?SM$", upper):
            value = upper[:-2]
            decoded_parts.append(f"Visibility {value} statute miles")
            continue

        cloud_match = cloud_pattern.match(upper)
        if cloud_match:
            cover = cloud_match.group(1)
            altitude_hundreds = int(cloud_match.group(2))
            altitude_ft = altitude_hundreds * 100
            suffix = cloud_match.group(3)
            cloud_text = f"{cloud_map[cover]} at {altitude_ft} feet AGL"
            if suffix == "CB":
                cloud_text += " (cumulonimbus)"
            if suffix == "TCU":
                cloud_text += " (towering cumulus)"
            decoded_parts.append(cloud_text)
            continue

        if upper.startswith("A") and len(upper) == 5 and upper[1:].isdigit():
            alt_inhg = normalize_altimeter_inhg(upper)
            if alt_inhg is not None:
                decoded_parts.append(f"Altimeter {alt_inhg:.2f} inHg")
            continue

        if upper.startswith("Q") and len(upper) == 5 and upper[1:].isdigit():
            alt_inhg = normalize_altimeter_inhg(upper)
            if alt_inhg is not None:
                decoded_parts.append(f"Altimeter {alt_inhg:.2f} inHg")
            continue

        for wx_code, wx_text in weather_map.items():
            if wx_code in upper and len(upper) <= 6:
                decoded_parts.append(wx_text)
                break

    if not decoded_parts:
        return text
    return "; ".join(decoded_parts)


def find_taf_with_fallback(
    requested_airport: str,
    airport_index: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, str]], str, float | None, str | None, bool]:
    _load_taf_fallback_cache_if_needed()

    direct_rows = fetch_navcan_alpha_records(requested_airport, "taf", max_items=10)
    if direct_rows:
        return direct_rows, requested_airport, 0.0, None, False

    requested_coords = _station_coords(requested_airport, airport_index)
    if requested_coords is None:
        return [], requested_airport, None, None, False

    with _taf_fallback_cache_lock:
        cached_code = _taf_fallback_cache.get(requested_airport)

    if cached_code and cached_code != requested_airport:
        cached_rows = fetch_navcan_alpha_records(cached_code, "taf", max_items=10)
        if cached_rows:
            cached_coords = _station_coords(cached_code, airport_index)
            distance_km: float | None = (
                haversine_km(requested_coords[0], requested_coords[1], cached_coords[0], cached_coords[1])
                if cached_coords is not None
                else None
            )
            direction = None
            if cached_coords is not None:
                bearing = bearing_degrees(requested_coords[0], requested_coords[1], cached_coords[0], cached_coords[1])
                direction = cardinal_direction_from_bearing(bearing)
            return cached_rows, cached_code, distance_km, direction, True

    candidates: list[tuple[str, float]] = []
    for code in METAR_STATIONS:
        if code == requested_airport:
            continue
        coords = _station_coords(code, airport_index)
        if coords is None:
            continue
        distance_km = haversine_km(requested_coords[0], requested_coords[1], coords[0], coords[1])
        candidates.append((code, distance_km))

    for station_code, distance_km in sorted(candidates, key=lambda item: item[1]):
        rows = fetch_navcan_alpha_records(station_code, "taf", max_items=10)
        if not rows:
            continue

        with _taf_fallback_cache_lock:
            _taf_fallback_cache[requested_airport] = station_code
        _save_taf_fallback_cache()

        source_coords = _station_coords(station_code, airport_index)
        direction = None
        if source_coords is not None:
            bearing = bearing_degrees(requested_coords[0], requested_coords[1], source_coords[0], source_coords[1])
            direction = cardinal_direction_from_bearing(bearing)

        return rows, station_code, distance_km, direction, True

    return [], requested_airport, None, None, False


def split_notam_by_age(
    records: list[dict[str, str]],
    max_age_days: int = 60,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    cutoff = datetime.now(tz=ZoneInfo("UTC")) - timedelta(days=max_age_days)
    recent: list[dict[str, str]] = []
    older: list[dict[str, str]] = []

    for row in records:
        start_time = parse_observed_utc(row.get("start"))
        if start_time is not None and start_time < cutoff:
            older.append(row)
        else:
            recent.append(row)

    return recent, older


def _parse_heading(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        heading = int(round(float(stripped))) % 360
        return 360 if heading == 0 else heading
    except ValueError:
        return None


@lru_cache(maxsize=1)
def load_online_runways_index() -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    try:
        response = requests.get(RUNWAYS_URL, timeout=20)
        response.raise_for_status()
        csv_text = response.text
    except requests.RequestException:
        return index

    reader = csv.DictReader(io.StringIO(csv_text))

    for row in reader:
        airport_ident = (row.get("airport_ident") or "").strip().upper()
        if not airport_ident:
            continue

        closed_flag = (row.get("closed") or "0").strip().lower()
        if closed_flag in {"1", "true", "yes"}:
            continue

        ends = [
            ((row.get("le_ident") or "").strip(), _parse_heading(row.get("le_heading_degT"))),
            ((row.get("he_ident") or "").strip(), _parse_heading(row.get("he_heading_degT"))),
        ]

        for runway_name, heading in ends:
            if not runway_name or heading is None:
                continue
            index.setdefault(airport_ident, []).append({"name": runway_name, "heading": heading})

    for airport_ident, runways in index.items():
        deduped: dict[str, dict[str, Any]] = {}
        for runway in runways:
            deduped[runway["name"]] = runway
        index[airport_ident] = sorted(deduped.values(), key=lambda runway: runway["name"])

    return index


def get_runways_for_airport(airport_code: str) -> tuple[list[dict[str, Any]], str]:
    online_index = load_online_runways_index()
    online_runways = online_index.get(airport_code, [])
    if online_runways:
        return online_runways, "Online (OurAirports)"
    local_runways = AIRPORTS.get(airport_code, {}).get("runways", [])
    if local_runways:
        return local_runways, "Local fallback"
    return [], "None available"


@lru_cache(maxsize=1)
def load_online_frequencies_index() -> dict[str, list[dict[str, str]]]:
    index: dict[str, list[dict[str, str]]] = {}
    try:
        response = requests.get(FREQUENCIES_URL, timeout=20)
        response.raise_for_status()
        csv_text = response.text
    except requests.RequestException:
        return index

    reader = csv.DictReader(io.StringIO(csv_text))
    service_names = {
        "ATIS": "ATIS",
        "TWR": "Tower",
        "GND": "Ground",
    }

    for row in reader:
        airport_ident = (row.get("airport_ident") or "").strip().upper()
        if not airport_ident:
            continue

        frequency_raw = (row.get("frequency_mhz") or "").strip()
        if not frequency_raw:
            continue

        freq_type = (row.get("type") or "").strip().upper()
        description = (row.get("description") or "").strip()
        service = service_names.get(freq_type) or description or freq_type or "Unknown"

        try:
            frequency = f"{float(frequency_raw):.2f}"
        except ValueError:
            frequency = frequency_raw

        index.setdefault(airport_ident, []).append({"service": service, "frequency": frequency})

    for airport_ident, frequencies in index.items():
        deduped: dict[tuple[str, str], dict[str, str]] = {}
        for freq in frequencies:
            deduped[(freq["service"], freq["frequency"])] = freq
        sorted_freqs = sorted(deduped.values(), key=lambda entry: (entry["service"], entry["frequency"]))
        index[airport_ident] = sorted_freqs

    return index


def get_frequencies_for_airport(airport_code: str) -> tuple[list[dict[str, str]], str]:
    online_index = load_online_frequencies_index()
    online_frequencies = online_index.get(airport_code, [])
    if online_frequencies:
        return online_frequencies, "Online (OurAirports)"
    local_frequencies = AIRPORTS.get(airport_code, {}).get("frequencies", [])
    if local_frequencies:
        return local_frequencies, "Local fallback"
    return [], "None available"


def merge_airport_overrides(base_airports: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    airports = dict(base_airports)

    for airport_code, overrides in AIRPORTS.items():
        base = airports.get(
            airport_code,
            {
                "name": overrides.get("name", airport_code),
                "lat": overrides["lat"],
                "lon": overrides["lon"],
                "liveatc_url": None,
                "liveatc_available": False,
                "elevation_ft": None,
                "runways": [],
                "frequencies": [],
            },
        )
        airports[airport_code] = {
            **base,
            "name": overrides.get("name", base["name"]),
            "lat": overrides.get("lat", base["lat"]),
            "lon": overrides.get("lon", base["lon"]),
            "liveatc_url": overrides.get("liveatc_url", base.get("liveatc_url")),
            "liveatc_available": overrides.get("liveatc_available", base.get("liveatc_available", False)),
            "elevation_ft": overrides.get("elevation_ft", base.get("elevation_ft")),
            "runways": overrides.get("runways", base.get("runways", [])),
            "frequencies": overrides.get("frequencies", base.get("frequencies", [])),
        }

    return dict(sorted(airports.items(), key=lambda entry: entry[0]))


def fetch_canadian_airports_online() -> dict[str, dict[str, Any]]:
    airports: dict[str, dict[str, Any]] = {}

    try:
        response = requests.get(AIRPORTS_URL, timeout=25)
        response.raise_for_status()
        csv_text = response.text
    except requests.RequestException:
        csv_text = ""

    if csv_text:
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            airport_code = (row.get("ident") or "").strip().upper()
            if not airport_code:
                continue

            if (row.get("iso_country") or "").strip().upper() != "CA":
                continue

            try:
                lat = float((row.get("latitude_deg") or "").strip())
                lon = float((row.get("longitude_deg") or "").strip())
            except ValueError:
                continue

            elevation_raw = (row.get("elevation_ft") or "").strip()
            try:
                elevation_ft = int(round(float(elevation_raw))) if elevation_raw else None
            except ValueError:
                elevation_ft = None

            airports[airport_code] = {
                "name": (row.get("name") or airport_code).strip(),
                "lat": lat,
                "lon": lon,
                "liveatc_url": None,
                "liveatc_available": False,
                "elevation_ft": elevation_ft,
                "runways": [],
                "frequencies": [],
            }

    return merge_airport_overrides(airports)


def _fallback_airport_index() -> dict[str, dict[str, Any]]:
    return merge_airport_overrides({})


def _read_airport_cache_file() -> tuple[dict[str, dict[str, Any]] | None, float]:
    try:
        raw = AIRPORT_CACHE_FILE.read_text(encoding="utf-8")
        parsed = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return None, 0.0

    if not isinstance(parsed, dict):
        return None, 0.0

    data = parsed.get("data")
    timestamp = parsed.get("timestamp", 0)
    if not isinstance(data, dict):
        return None, 0.0

    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        ts = 0.0

    return merge_airport_overrides(data), ts


def _write_airport_cache_file(data: dict[str, dict[str, Any]], timestamp: float) -> None:
    try:
        payload = {"timestamp": timestamp, "data": data}
        AIRPORT_CACHE_FILE.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        return


def _refresh_airport_cache_worker() -> None:
    global _airport_cache_data, _airport_cache_timestamp, _airport_cache_refreshing

    try:
        refreshed = fetch_canadian_airports_online()
        if not refreshed:
            return

        now = time.time()
        with _airport_cache_lock:
            _airport_cache_data = refreshed
            _airport_cache_timestamp = now

        _write_airport_cache_file(refreshed, now)
    finally:
        with _airport_cache_lock:
            _airport_cache_refreshing = False


def _trigger_airport_cache_refresh() -> None:
    global _airport_cache_refreshing

    with _airport_cache_lock:
        if _airport_cache_refreshing:
            return
        _airport_cache_refreshing = True

    thread = threading.Thread(target=_refresh_airport_cache_worker, daemon=True)
    thread.start()


def get_cached_canadian_airports() -> dict[str, dict[str, Any]]:
    global _airport_cache_data, _airport_cache_timestamp

    now = time.time()
    should_refresh = False

    with _airport_cache_lock:
        if _airport_cache_data is None:
            file_data, file_timestamp = _read_airport_cache_file()
            if file_data:
                _airport_cache_data = file_data
                _airport_cache_timestamp = file_timestamp
            else:
                _airport_cache_data = _fallback_airport_index()
                _airport_cache_timestamp = 0.0

        cached = _airport_cache_data
        cache_age = now - _airport_cache_timestamp if _airport_cache_timestamp > 0 else float("inf")
        should_refresh = cache_age >= AIRPORT_CACHE_TTL_SECONDS

    if should_refresh:
        _trigger_airport_cache_refresh()

    return cached


def split_operational_frequencies(
    frequencies: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    operational_keywords = {
        "ATIS",
        "TOWER",
        "GROUND",
        "ATF",
        "MF",
        "UNICOM",
        "CTAF",
        "RDO",
        "RADIO",
        "FSS",
    }

    operational: list[dict[str, str]] = []
    others: list[dict[str, str]] = []

    for freq in frequencies:
        service_upper = freq["service"].upper()
        if any(keyword in service_upper for keyword in operational_keywords):
            operational.append(freq)
        else:
            others.append(freq)

    if not operational:
        return frequencies, []
    return operational, others


def find_metar_with_fallback(
    requested_airport: str,
    airport_index: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, str | None, float | None, dict[str, Any] | None]:
    primary = fetch_metar(requested_airport)
    local_latest = primary or _get_cached_metar_entry_any_age(requested_airport)
    if primary is not None and _is_metar_recent(primary, LOCAL_METAR_RECENT_MAX_AGE_HOURS):
        return primary, requested_airport, 0.0, local_latest

    requested = airport_index.get(requested_airport)
    if requested is None:
        return None, None, None, local_latest

    requested_lat = requested["lat"]
    requested_lon = requested["lon"]

    candidates: list[tuple[str, float]] = []
    for code, station in METAR_STATIONS.items():
        if code == requested_airport:
            continue
        distance_km = haversine_km(requested_lat, requested_lon, station["lat"], station["lon"])
        candidates.append((code, distance_km))

    for station_code, distance_km in sorted(candidates, key=lambda entry: entry[1]):
        cached = _get_cached_metar_entry(station_code)
        if cached is not None and _is_metar_recent(cached, FALLBACK_METAR_MAX_AGE_HOURS):
            return cached, station_code, distance_km, local_latest

    for station_code, distance_km in sorted(candidates, key=lambda entry: entry[1]):
        fallback = fetch_metar(station_code)
        if fallback is not None and _is_metar_recent(fallback, FALLBACK_METAR_MAX_AGE_HOURS):
            return fallback, station_code, distance_km, local_latest

    return None, None, None, local_latest


def parse_wind(metar: dict[str, Any]) -> Wind | None:
    speed = metar.get("wspd")
    if speed is None:
        return None

    raw_dir = metar.get("wdir")
    variable = False
    direction: int | None = None

    if isinstance(raw_dir, str):
        if raw_dir.upper() == "VRB":
            variable = True
        elif raw_dir.isdigit():
            direction = int(raw_dir)
    elif isinstance(raw_dir, (int, float)):
        direction = int(raw_dir)

    gust = metar.get("wgst")
    return Wind(
        direction=direction,
        speed_kt=int(speed),
        gust_kt=int(gust) if gust is not None else None,
        variable=variable,
    )


def _parse_fraction(text: str) -> float | None:
    value = text.strip()
    if not value:
        return None

    if "/" in value:
        left, right = value.split("/", 1)
        try:
            numerator = float(left)
            denominator = float(right)
        except ValueError:
            return None
        if denominator == 0:
            return None
        return numerator / denominator

    try:
        return float(value)
    except ValueError:
        return None


def _parse_visibility_sm(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().upper()
    if not text or text in {"N/A", "UNKNOWN"}:
        return None

    if text in {"P6SM", "6+ SM", "6+SM"}:
        return 6.0

    text = re.sub(r"\s+", " ", text)

    mixed_match = re.match(r"^(M?\d+)\s+(\d+/\d+)\s*SM$", text)
    if mixed_match:
        whole_part = mixed_match.group(1).lstrip("M")
        whole = _parse_fraction(whole_part)
        fraction = _parse_fraction(mixed_match.group(2))
        if whole is None or fraction is None:
            return None
        return whole + fraction

    fractional_match = re.match(r"^M?(\d+/\d+)\s*SM$", text)
    if fractional_match:
        return _parse_fraction(fractional_match.group(1))

    numeric_match = re.match(r"^M?(\d+(?:\.\d+)?)\s*SM$", text)
    if numeric_match:
        return _parse_fraction(numeric_match.group(1))

    return _parse_fraction(text)


def _flight_category(ceiling_ft: int | None, visibility_sm: float | None) -> dict[str, str]:
    if (ceiling_ft is not None and ceiling_ft < 500) or (visibility_sm is not None and visibility_sm < 1.0):
        return {
            "label": "LIFR",
            "color": "magenta",
            "concept": "Ceiling below 500 ft AGL and/or visibility less than 1 mile",
        }

    if (ceiling_ft is not None and 500 <= ceiling_ft < 1000) or (
        visibility_sm is not None and 1.0 <= visibility_sm <= 3.0
    ):
        return {
            "label": "IFR",
            "color": "red",
            "concept": "Ceiling 500 to below 1,000 ft AGL and/or visibility 1 to 3 miles",
        }

    if (ceiling_ft is not None and 1000 <= ceiling_ft <= 3000) or (
        visibility_sm is not None and 3.0 < visibility_sm <= 5.0
    ):
        return {
            "label": "MVFR",
            "color": "blue",
            "concept": "Ceiling 1,000 to 3,000 ft AGL and/or visibility over 3 to 5 miles",
        }

    if (ceiling_ft is not None and ceiling_ft > 3000) and (visibility_sm is not None and visibility_sm > 5.0):
        return {
            "label": "VFR",
            "color": "green",
            "concept": "Ceiling above 3,000 ft AGL and visibility greater than 5 miles",
        }

    return {
        "label": "N/A",
        "color": "",
        "concept": "Insufficient ceiling/visibility data for category",
    }


def decode_metar(metar: dict[str, Any]) -> dict[str, Any]:
    wind = parse_wind(metar)
    if wind is None:
        wind_text = "Wind unavailable"
    elif wind.variable:
        wind_text = f"Variable at {wind.speed_kt} kt"
    elif wind.direction is None:
        wind_text = f"{wind.speed_kt} kt"
    elif wind.gust_kt is not None:
        wind_text = f"{wind.direction:03d}° at {wind.speed_kt} kt, gust {wind.gust_kt} kt"
    else:
        wind_text = f"{wind.direction:03d}° at {wind.speed_kt} kt"

    vis_value = metar.get("visib")
    visibility_sm = _parse_visibility_sm(vis_value)
    visibility_text = f"{vis_value} SM" if vis_value not in (None, "") else "N/A"

    temp_value = metar.get("temp")
    temp_text = f"{temp_value}°C" if temp_value not in (None, "") else "N/A"

    dew_value = metar.get("dewp")
    dew_text = f"{dew_value}°C" if dew_value not in (None, "") else "N/A"

    altimeter_inhg = normalize_altimeter_inhg(metar.get("altim"))
    altim_text = f"{altimeter_inhg:.2f} inHg" if altimeter_inhg is not None else "N/A"
    raw_text = str(metar.get("rawOb") or metar.get("raw_text") or "").strip()
    ceiling_text, other_weather_text, ceiling_ft = _extract_metar_ceiling_and_other(raw_text)
    category = _flight_category(ceiling_ft, visibility_sm)

    return {
        "raw": raw_text or "N/A",
        "observed": metar.get("obsTime") or metar.get("observation_time") or "N/A",
        "wind": wind_text,
        "visibility": visibility_text,
        "temperature": temp_text,
        "dewpoint": dew_text,
        "altimeter": altim_text,
        "ceiling": ceiling_text,
        "other_weather": other_weather_text,
        "flight_category_label": category["label"],
        "flight_category_color": category["color"],
        "flight_category_concept": category["concept"],
    }


def _extract_metar_ceiling_and_other(raw_text: str) -> tuple[str, str, int | None]:
    upper = raw_text.upper()

    ceiling_heights_ft: list[int] = []
    for match in re.finditer(r"\b(?:BKN|OVC|VV)(\d{3})\b", upper):
        try:
            ceiling_heights_ft.append(int(match.group(1)) * 100)
        except ValueError:
            continue

    ceiling_ft: int | None = None
    if ceiling_heights_ft:
        ceiling_ft = min(ceiling_heights_ft)
        ceiling_text = f"{ceiling_ft} ft AGL"
    elif re.search(r"\b(?:SKC|CLR|NSC|NCD|CAVOK)\b", upper):
        ceiling_text = "No ceiling reported"
    else:
        ceiling_text = "N/A"

    weather_parts: list[str] = []
    for token in upper.split():
        decoded = _decode_weather_token(token.strip("= "))
        if decoded:
            weather_parts.append(decoded)

    if re.search(r"\bWS\s+ALL\s+RWY\b", upper):
        weather_parts.append("Windshear all runways")

    for match in re.finditer(r"\bWS\s+RWY(\d{2}[LRC]?)\b", upper):
        weather_parts.append(f"Windshear runway {match.group(1)}")

    for match in re.finditer(r"\bWS\d{3}/\d{3}\d{2,3}KT\b", upper):
        weather_parts.append(f"Low-level windshear {match.group(0)}")

    deduped_weather = list(dict.fromkeys(weather_parts))
    other_weather_text = ", ".join(deduped_weather) if deduped_weather else "None"

    return ceiling_text, other_weather_text, ceiling_ft


def format_observed_local(observed: Any) -> str:
    observed_utc = parse_observed_utc(observed)
    if observed_utc is None:
        return "N/A"
    return observed_utc.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")


def parse_observed_utc(observed: Any) -> datetime | None:
    if not observed or observed == "N/A":
        return None

    if isinstance(observed, (int, float)):
        try:
            epoch_value = float(observed)
            if epoch_value > 1e12:
                epoch_value /= 1000.0
            return datetime.fromtimestamp(epoch_value, tz=ZoneInfo("UTC"))
        except (ValueError, OSError, OverflowError):
            return None

    observed_str = str(observed)

    candidates = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in candidates:
        try:
            parsed = datetime.strptime(observed_str, fmt)
            return parsed.replace(tzinfo=ZoneInfo("UTC"))
        except ValueError:
            continue

    try:
        normalized = observed_str.replace("Z", "+00:00")
        parsed_iso = datetime.fromisoformat(normalized)
        if parsed_iso.tzinfo is None:
            parsed_iso = parsed_iso.replace(tzinfo=ZoneInfo("UTC"))
        return parsed_iso.astimezone(ZoneInfo("UTC"))
    except ValueError:
        return None


def metar_age(observed: Any) -> tuple[str, str]:
    observed_utc = parse_observed_utc(observed)
    if observed_utc is None:
        return "N/A", ""

    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    age_hours = max(0.0, (now_utc - observed_utc).total_seconds() / 3600.0)

    if age_hours < 1.0:
        age_minutes = max(1, int(round(age_hours * 60)))
        age_text = f"{age_minutes} min old"
    else:
        age_text = f"{age_hours:.1f} hr old"

    if age_hours < 3.0:
        return age_text, "green"
    if age_hours < 6.0:
        return age_text, "orange"
    return age_text, "red"


def calculate_density_altitude(elevation_ft: Any, temperature_c: Any, altimeter_inhg: Any) -> str:
    try:
        field_elevation_ft = float(elevation_ft)
        outside_air_temp_c = float(temperature_c)
    except (TypeError, ValueError):
        return "N/A"

    altimeter = normalize_altimeter_inhg(altimeter_inhg)
    if altimeter is None:
        return "N/A"

    pressure_altitude_ft = field_elevation_ft + (29.92 - altimeter) * 1000.0
    isa_temp_c = 15.0 - (1.98 * (field_elevation_ft / 1000.0))
    density_altitude_ft = pressure_altitude_ft + (120.0 * (outside_air_temp_c - isa_temp_c))
    return f"{int(round(density_altitude_ft))} ft"


def runway_components(runway_heading: int, wind: Wind) -> dict[str, Any]:
    if wind.direction is None:
        return {
            "angle": None,
            "headwind": None,
            "crosswind": None,
            "crosswind_side": "variable",
        }

    angle = normalize_angle_difference(float(wind.direction), float(runway_heading))
    theta = radians(angle)
    headwind = wind.speed_kt * cos(theta)
    crosswind = wind.speed_kt * sin(theta)
    crosswind_side = "right" if crosswind > 0 else "left"
    return {
        "angle": round(angle, 1),
        "headwind": round(headwind, 1),
        "crosswind": round(abs(crosswind), 1),
        "crosswind_side": crosswind_side,
    }


def choose_preferred(runway_rows: list[dict[str, Any]]) -> str | None:
    valid = [r for r in runway_rows if r["headwind"] is not None]
    if not valid:
        return None

    non_tailwind = [r for r in valid if r["headwind"] >= 0]
    if non_tailwind:
        chosen = min(non_tailwind, key=lambda r: (r["crosswind"], -r["headwind"]))
    else:
        chosen = max(valid, key=lambda r: (r["headwind"], -r["crosswind"]))
    return chosen["runway"]


def build_conditions_context(requested_airport: str | None = None) -> dict[str, Any]:
    airport_index = get_cached_canadian_airports()
    if not airport_index:
        airport_index = AIRPORTS

    selected_airport = (requested_airport or "CYAV").upper()
    if selected_airport not in airport_index:
        selected_airport = "CYAV" if "CYAV" in airport_index else next(iter(airport_index))

    selected_runways, runway_source = get_runways_for_airport(selected_airport)

    metar: dict[str, Any] | None = None
    decoded: dict[str, Any] | None = None
    runway_rows: list[dict[str, Any]] = []
    preferred: str | None = None
    error: str | None = None
    metar_source_code: str | None = None
    metar_distance_km: float | None = None
    metar_distance_nm: float | None = None
    metar_provider = "Unknown"
    local_recent_exists = False
    local_recent_label = ""
    local_recent_raw = ""
    local_recent_age_text = ""
    local_recent_age_color = ""
    frequency_source = "Local fallback"
    fallback_used = False
    taf_rows: list[dict[str, str]] = []
    notam_rows: list[dict[str, str]] = []
    sigmet_rows: list[dict[str, str]] = []
    taf_source_code = selected_airport
    taf_distance_km: float | None = None
    taf_distance_nm: float | None = None
    taf_direction: str | None = None
    taf_fallback_used = False
    taf_decoded_rows: list[dict[str, str]] = []
    taf_updated_local = "N/A"
    taf_updated_zulu = "N/A"
    taf_age_text = "N/A"
    taf_age_color = ""
    notam_recent_rows: list[dict[str, str]] = []
    notam_older_rows: list[dict[str, str]] = []

    try:
        metar, metar_source_code, metar_distance_km, local_latest = find_metar_with_fallback(selected_airport, airport_index)
        if metar is None:
            error = f"No recent METAR found for {selected_airport}."
        else:
            metar_provider = str(metar.get("_provider") or "AviationWeather")
            fallback_used = metar_source_code != selected_airport
            if local_latest is not None:
                observed_local, observed_zulu = format_local_and_zulu(
                    local_latest.get("obsTime") or local_latest.get("observation_time")
                )
                local_recent_exists = observed_local != "N/A"
                local_recent_label = observed_local
                if observed_zulu != "N/A":
                    local_recent_label = f"{observed_local} ({observed_zulu})"
                local_recent_raw = str(local_latest.get("rawOb") or local_latest.get("raw_text") or "").strip()
                local_recent_age_text, local_recent_age_color = metar_age(
                    local_latest.get("obsTime") or local_latest.get("observation_time")
                )
            decoded = decode_metar(metar)
            decoded["observed_local"] = format_observed_local(decoded["observed"])
            _, observed_zulu = format_local_and_zulu(decoded["observed"])
            decoded["observed_zulu"] = observed_zulu
            age_text, age_color = metar_age(decoded["observed"])
            decoded["metar_age"] = age_text
            decoded["metar_age_color"] = age_color
            decoded["density_altitude"] = calculate_density_altitude(
                airport_index[selected_airport].get("elevation_ft"),
                metar.get("temp"),
                metar.get("altim"),
            )
            wind = parse_wind(metar)
            for runway in selected_runways:
                if wind is not None:
                    components = runway_components(runway["heading"], wind)
                else:
                    components = {
                        "angle": None,
                        "headwind": None,
                        "crosswind": None,
                        "crosswind_side": "variable",
                    }
                runway_rows.append(
                    {
                        "runway": runway["name"],
                        "heading": runway["heading"],
                        **components,
                    }
                )
            if wind is not None:
                preferred = choose_preferred(runway_rows)
    except requests.RequestException as exc:
        error = f"Unable to fetch METAR data: {exc}"

    selected_frequencies, frequency_source = get_frequencies_for_airport(selected_airport)
    taf_rows, taf_source_code, taf_distance_km, taf_direction, taf_fallback_used = find_taf_with_fallback(
        selected_airport,
        airport_index,
    )
    taf_distance_nm = (taf_distance_km / 1.852) if taf_distance_km is not None else None
    taf_decoded_rows = build_taf_decoded_rows(taf_rows)
    taf_summary_text = "TAF"
    if selected_airport and taf_decoded_rows:
        first_row = taf_decoded_rows[0]
        first_window = " ".join(
            part for part in [str(first_row.get("window_day") or "").strip(), str(first_row.get("window_line") or "").strip()] if part
        ).strip()
        summary_parts = [
            first_window,
            str(first_row.get("flight_category_label") or "").strip(),
            str(first_row.get("wind") or "").strip(),
            str(first_row.get("visibility") or "").strip(),
        ]
        filtered_parts = [part for part in summary_parts if part and part not in {"N/A", "None"}]
        if filtered_parts:
            taf_summary_text = "TAF: " + " | ".join(filtered_parts)
    latest_taf_issue = latest_taf_issue_time(taf_rows)
    if latest_taf_issue is not None:
        taf_updated_local, taf_updated_zulu = format_local_and_zulu(latest_taf_issue)
        taf_age_text, taf_age_color = metar_age(latest_taf_issue)
    notam_rows = fetch_navcan_alpha_records(selected_airport, "notam", max_items=30)
    notam_recent_rows, notam_older_rows = split_notam_by_age(notam_rows, max_age_days=60)
    sigmet_rows = fetch_navcan_alpha_records(selected_airport, "sigmet", max_items=20)

    operational_frequencies, other_frequencies = split_operational_frequencies(selected_frequencies)
    selected_airport_data = {
        **airport_index[selected_airport],
        "frequencies": operational_frequencies,
    }

    airport_options = [
        {"code": code, "name": info["name"]}
        for code, info in airport_index.items()
    ]
    report_generated_local = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M %Z")

    return {
        "airports": airport_index,
        "airport_options": airport_options,
        "selected_airport": selected_airport,
        "selected_airport_data": selected_airport_data,
        "metar": metar,
        "decoded": decoded,
        "runway_rows": runway_rows,
        "preferred": preferred,
        "error": error,
        "metar_source_code": metar_source_code,
        "metar_provider": metar_provider,
        "local_recent_exists": local_recent_exists,
        "local_recent_label": local_recent_label,
        "local_recent_raw": local_recent_raw,
        "local_recent_age_text": local_recent_age_text,
        "local_recent_age_color": local_recent_age_color,
        "metar_distance_km": round(metar_distance_km, 1) if metar_distance_km is not None else None,
        "metar_distance_nm": round((metar_distance_km or 0.0) / 1.852, 1) if metar_distance_km is not None else None,
        "runway_source": runway_source,
        "frequency_source": frequency_source,
        "other_frequencies": other_frequencies,
        "taf_rows": taf_rows,
        "taf_decoded_rows": taf_decoded_rows,
        "taf_source_code": taf_source_code,
        "taf_distance_km": round(taf_distance_km, 1) if taf_distance_km is not None else None,
        "taf_distance_nm": round(taf_distance_nm, 1) if taf_distance_nm is not None else None,
        "taf_direction": taf_direction,
        "taf_fallback_used": taf_fallback_used,
        "taf_summary_text": taf_summary_text,
        "taf_updated_local": taf_updated_local,
        "taf_updated_zulu": taf_updated_zulu,
        "taf_age_text": taf_age_text,
        "taf_age_color": taf_age_color,
        "notam_rows": notam_recent_rows,
        "notam_older_rows": notam_older_rows,
        "sigmet_rows": sigmet_rows,
        "fallback_used": fallback_used,
        "report_generated_local": report_generated_local,
    }


def _prince_version() -> str | None:
    if shutil.which("prince") is None:
        return None
    try:
        result = subprocess.run(
            ["prince", "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    version_text = (result.stdout or result.stderr or "").strip()
    return version_text if version_text else "Prince"


def _query_bool(name: str, default: bool = True) -> bool:
    raw = request.args.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


@app.route("/prince-status")
def prince_status() -> Response:
    version = _prince_version()
    return jsonify(
        {
            "installed": version is not None,
            "version": version,
        }
    )


@app.route("/print-report")
def print_report() -> Response:
    version = _prince_version()
    if version is None:
        return jsonify({"error": "Prince is not installed on this server."}), 503

    selected_airport = request.args.get("airport", "CYAV")
    context = build_conditions_context(selected_airport)
    include_runway_freq = _query_bool("include_runway_freq", True)
    include_metar = _query_bool("include_metar", True)
    include_sigmet = _query_bool("include_sigmet", True)
    include_taf = _query_bool("include_taf", True)
    include_raw_taf = _query_bool("include_raw_taf", True)
    include_notam = _query_bool("include_notam", True)

    html = render_template(
        "print_report.html",
        **context,
        include_runway_freq=include_runway_freq,
        include_metar=include_metar,
        include_sigmet=include_sigmet,
        include_taf=include_taf,
        include_raw_taf=include_raw_taf,
        include_notam=include_notam,
    )

    try:
        with tempfile.TemporaryDirectory(prefix="flight-conditions-") as temp_dir:
            temp_path = Path(temp_dir)
            html_path = temp_path / "report.html"
            pdf_path = temp_path / "report.pdf"
            html_path.write_text(html, encoding="utf-8")

            result = subprocess.run(
                ["prince", str(html_path), "-o", str(pdf_path)],
                capture_output=True,
                text=True,
                check=False,
                timeout=45,
            )
            if result.returncode != 0 or not pdf_path.exists():
                error_text = (result.stderr or result.stdout or "Prince failed to render PDF.").strip()
                return jsonify({"error": error_text}), 500

            pdf_bytes = pdf_path.read_bytes()
    except (OSError, subprocess.SubprocessError) as exc:
        return jsonify({"error": f"Unable to generate PDF: {exc}"}), 500

    report_date = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    filename = f"{context['selected_airport']} - {report_date}.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename=\"{filename}\"",
            "Cache-Control": "no-store",
        },
    )


@app.route("/")
def index() -> str:
    context = build_conditions_context(request.args.get("airport", "CYAV"))
    return render_template("index.html", **context)


if __name__ == "__main__":
    app.run(debug=True)