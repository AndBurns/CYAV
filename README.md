# CYAV METAR Flask App

Simple Flask app to:
- Select an airport from a searchable dropdown (auto-refresh on selection)
- Show recent airport selections (last 5) at the top of the dropdown
- Populate airport choices dynamically from all Canadian airports
- Fetch and display decoded METAR details
- Show METAR age with color status (green <3h, orange 3-6h, red >6h)
- Show density altitude when temperature, altimeter, and airport elevation are available
- Show collapsible NAV CANADA TAF / NOTAM / SIGMET sections
- Show all available runways for that airport
- Calculate headwind/tailwind and crosswind for each runway
- Highlight the computed best runway and allow client-side runway filtering/isolation
- Fall back to the nearest station with an available METAR when the selected airport has none,
  and show the distance to that station
- Provide a top-right hamburger menu with Print and About actions
- Generate a Prince XML PDF report from a print-options modal (with per-section include toggles)
- Persist optional preferences (recent airports and print options) via privacy-controlled localStorage
- Show Privacy & Cookies controls with accept/reject and per-item retention toggles

The page uses a responsive layout:
- Left side: runway wind components
- Right side: decoded METAR in a two-column table + airport frequencies

Airport selector behavior:
- No load button required; the page refreshes automatically when selection changes
- Search is built into the airport dropdown (Select2) and focuses automatically when opened
- Recent selections and print options are stored in browser localStorage only when optional storage is enabled

## Run locally

1. Create/activate a virtual environment (optional but recommended)
2. Install dependencies:

	```bash
	pip install -r requirements.txt
	```

3. Start the app:

	```bash
	flask --app app run --debug
	```

4. Open:

	`http://127.0.0.1:5000`

## Notes

- METAR data sources: AviationWeather API (`aviationweather.gov`) with NAV CANADA Weather Recall (`plan.navcanada.ca`) fallback for station LWIS/METAR coverage
- Runway data source: OurAirports (`runways.csv`) with automatic local fallback from `app.py` if online data is unavailable
- Frequency data source: OurAirports (`airport-frequencies.csv`) with automatic local fallback from `app.py` if online data is unavailable
- PDF generation: Prince XML CLI (`prince`) must be installed on the server; app checks availability before opening print flow
- This app does not set first-party tracking cookies; it uses localStorage for consent state and optional preference retention

## Deploy on Apache at /FlightInfo

This project is now set up to run behind Apache/mod_wsgi at:

- `https://a-burns.com/FlightInfo`

### 1) Install Apache + mod_wsgi + venv dependencies

Example (Debian/Ubuntu):

```bash
sudo apt update
sudo apt install -y apache2 libapache2-mod-wsgi-py3 python3-venv
```

### 2) Create a virtual environment and install app requirements

```bash
cd /var/www/a-burns.com/public_html/FlightInfo
python3 -m venv /var/www/a-burns.com/venv
/var/www/a-burns.com/venv/bin/pip install --upgrade pip
/var/www/a-burns.com/venv/bin/pip install -r requirements.txt
```

### 3) Enable the Apache config snippet

Use the provided file:

- `apache/flightinfo.conf`

Important:

- Update `python-home` in that file if your venv path differs.
- It expects your app path to be `/var/www/a-burns.com/public_html/FlightInfo` (your symlink target/path).

Then include that snippet in your active vhost for `a-burns.com`, or copy its directives into the vhost.

### 4) Reload Apache

```bash
sudo apachectl configtest
sudo systemctl reload apache2
```

### 5) Verify

Open:

- `https://a-burns.com/FlightInfo`

If needed, check logs:

```bash
sudo tail -f /var/log/apache2/error.log
```
