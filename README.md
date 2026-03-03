# CYAV METAR Flask App

Simple Flask app to:
- Select an airport from a dropdown (`CYAV`, `CYWG`, `CYQK`, `CYGM`)
- Fetch and display decoded METAR details
- Show available runways for that airport
- Calculate headwind/tailwind and crosswind for each runway
- Highlight the preferred runway (non-negative headwind + smallest crosswind)
- Fall back to the nearest station with an available METAR when the selected airport has none,
  and show the distance to that station

The page uses a responsive layout:
- Left side: runway wind components
- Right side: decoded METAR in a two-column table + airport frequencies

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

- METAR data source: AviationWeather API (`aviationweather.gov`)
- Runway data source: OurAirports (`runways.csv`) with automatic local fallback from `app.py` if online data is unavailable
