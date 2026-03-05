# CYAV METAR Flask App

Simple Flask app to:
- Select an airport from a searchable dropdown (auto-refresh on selection)
- Show recent airport selections (last 5) at the top of the dropdown
- Populate airport choices dynamically from all Canadian airports
- Fetch and display decoded METAR details
- Show METAR age with color status (green <3h, orange 3-6h, red >6h)
- Show density altitude when temperature, altimeter, and airport elevation are available
- Show collapsible NAV CANADA TAF / NOTAM / SIGMET sections
- Show available runways for that airport
- Calculate headwind/tailwind and crosswind for each runway
- Highlight the preferred runway (non-negative headwind + smallest crosswind)
- Fall back to the nearest station with an available METAR when the selected airport has none,
  and show the distance to that station

The page uses a responsive layout:
- Left side: runway wind components
- Right side: decoded METAR in a two-column table + airport frequencies

Airport selector behavior:
- No load button required; the page refreshes automatically when selection changes
- Search is built into the airport dropdown (Select2) and focuses automatically when opened
- Recent selections are stored in browser localStorage (client-side)

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
