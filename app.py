from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from math import asin, cos, radians, sin, sqrt
from typing import Any
from zoneinfo import ZoneInfo

import requests
from flask import Flask, render_template, request


app = Flask(__name__)

AIRPORTS: dict[str, dict[str, Any]] = {
    "CYAV": {
        "name": "St. Andrews Airport",
        "lat": 50.0564,
        "lon": -97.0325,
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
            {"service": "ATF", "frequency": "123.00"},
            {"service": "MF", "frequency": "122.70"},
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
RUNWAYS_URL = "https://ourairports.com/data/runways.csv"
LOCAL_TZ = ZoneInfo("America/Winnipeg")

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


def fetch_metar(airport: str) -> dict[str, Any] | None:
    params = {
        "ids": airport,
        "format": "json",
        "taf": "false",
        "hours": 3,
    }
    try:
        response = requests.get(API_URL, params=params, timeout=8)
        response.raise_for_status()
        if not response.text or not response.text.strip():
            return None
        data = response.json()
    except (requests.RequestException, ValueError):
        return None

    if not isinstance(data, list) or not data:
        return None
    if not isinstance(data[0], dict):
        return None
    return data[0]


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
    return AIRPORTS[airport_code]["runways"], "Local fallback"


def find_metar_with_fallback(requested_airport: str) -> tuple[dict[str, Any] | None, str | None, float | None]:
    primary = fetch_metar(requested_airport)
    if primary is not None:
        return primary, requested_airport, 0.0

    requested = AIRPORTS[requested_airport]
    requested_lat = requested["lat"]
    requested_lon = requested["lon"]

    candidates: list[tuple[str, float]] = []
    for code, station in METAR_STATIONS.items():
        if code == requested_airport:
            continue
        distance_km = haversine_km(requested_lat, requested_lon, station["lat"], station["lon"])
        candidates.append((code, distance_km))

    for station_code, distance_km in sorted(candidates, key=lambda entry: entry[1]):
        fallback = fetch_metar(station_code)
        if fallback is not None:
            return fallback, station_code, distance_km

    return None, None, None


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

    return {
        "raw": metar.get("rawOb") or metar.get("raw_text") or "N/A",
        "observed": metar.get("obsTime") or metar.get("observation_time") or "N/A",
        "wind": wind_text,
        "visibility": f"{metar.get('visib', 'N/A')} SM",
        "temperature": f"{metar.get('temp', 'N/A')}°C",
        "dewpoint": f"{metar.get('dewp', 'N/A')}°C",
        "altimeter": f"{metar.get('altim', 'N/A')} inHg",
    }


def format_observed_local(observed: Any) -> str:
    if not observed or observed == "N/A":
        return "N/A"

    if isinstance(observed, (int, float)):
        try:
            epoch_value = float(observed)
            if epoch_value > 1e12:
                epoch_value /= 1000.0
            local_time = datetime.fromtimestamp(epoch_value, tz=ZoneInfo("UTC")).astimezone(LOCAL_TZ)
            return local_time.strftime("%Y-%m-%d %H:%M %Z")
        except (ValueError, OSError, OverflowError):
            return "N/A"

    observed_str = str(observed)

    candidates = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in candidates:
        try:
            parsed = datetime.strptime(observed_str, fmt)
            utc_time = parsed.replace(tzinfo=ZoneInfo("UTC"))
            local_time = utc_time.astimezone(LOCAL_TZ)
            return local_time.strftime("%Y-%m-%d %H:%M %Z")
        except ValueError:
            continue

    try:
        normalized = observed_str.replace("Z", "+00:00")
        parsed_iso = datetime.fromisoformat(normalized)
        if parsed_iso.tzinfo is None:
            parsed_iso = parsed_iso.replace(tzinfo=ZoneInfo("UTC"))
        local_time = parsed_iso.astimezone(LOCAL_TZ)
        return local_time.strftime("%Y-%m-%d %H:%M %Z")
    except ValueError:
        return "N/A"


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


@app.route("/")
def index() -> str:
    selected_airport = request.args.get("airport", "CYAV")
    if selected_airport not in AIRPORTS:
        selected_airport = "CYAV"

    metar: dict[str, Any] | None = None
    decoded: dict[str, Any] | None = None
    runway_rows: list[dict[str, Any]] = []
    preferred: str | None = None
    error: str | None = None
    metar_source_code: str | None = None
    metar_distance_km: float | None = None
    metar_distance_nm: float | None = None
    runway_source = "Local fallback"
    fallback_used = False

    try:
        metar, metar_source_code, metar_distance_km = find_metar_with_fallback(selected_airport)
        if metar is None:
            error = f"No recent METAR found for {selected_airport}."
        else:
            fallback_used = metar_source_code != selected_airport
            decoded = decode_metar(metar)
            decoded["observed_local"] = format_observed_local(decoded["observed"])
            wind = parse_wind(metar)
            selected_runways, runway_source = get_runways_for_airport(selected_airport)
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

    return render_template(
        "index.html",
        airports=AIRPORTS,
        selected_airport=selected_airport,
        selected_airport_data=AIRPORTS[selected_airport],
        metar=metar,
        decoded=decoded,
        runway_rows=runway_rows,
        preferred=preferred,
        error=error,
        metar_source_code=metar_source_code,
        metar_distance_km=round(metar_distance_km, 1) if metar_distance_km is not None else None,
        metar_distance_nm=round((metar_distance_km or 0.0) / 1.852, 1) if metar_distance_km is not None else None,
        runway_source=runway_source,
        fallback_used=fallback_used,
    )


if __name__ == "__main__":
    app.run(debug=True)