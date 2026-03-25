# Weather Data Pipeline CLI Tool

## Overview
Python-based command-line application that fetches real-time weather data from a public REST API and processes it into structured, human-readable output. The tool demonstrates API integration, data pipeline processing, and clean terminal-based visualization of weather insights.

---

## Features
- Real-time weather data retrieval via REST API  
- CLI argument parsing (`--city`, `--lat`, `--lon`, `--days`, etc.)  
- JSON parsing and transformation into structured datasets  
- Data pipeline for processing, summarizing, and categorizing weather trends  
- Error handling, retry logic, and input validation  
- Export options (JSON, CSV) for downstream use  
- Clean, formatted terminal output with summaries and visual indicators
- End-to-end data pipeline: API ingestion → transformation → summarization → formatted output

---

## Tech Stack
- Python  
- REST APIs  
- Requests library  
- CLI (argparse)  

---

## System Design

1. Input handling via CLI arguments (city or coordinates)  
2. Geocoding to retrieve latitude/longitude  
3. API request to fetch weather data  
4. Data processing pipeline:
   - JSON parsing  
   - Data cleaning and structuring  
   - Feature extraction (temperature, precipitation, etc.)  
5. Aggregation and summarization of forecast data  
6. Output formatting for terminal display

---

## Project Structure

```
weather-tool/
│── weather_tool_v2.py
│── README.md
│── requirements.txt
```

---

## How to Run

```bash
# Clone the repository
git clone https://github.com/evelynxlam/weather-tool.git

# Navigate into the project directory
cd weather-tool

# Run the script
python weather_tool_v2.py --city "Denver"
```

---

## Example Usage

```bash
python weather_tool_v2.py --city "Denver"
```

This example demonstrates the CLI tool fetching real-time weather data, processing it through a data pipeline, and displaying structured output including current conditions, a 7-day forecast, and aggregated summaries.

---

## Example Output

```
🔍 Geocoding 'Denver' …
🌐 Fetching 7-day forecast …
⚙️  Running data pipeline …

──────────────────────────────────────────────────────────────
📍 Denver, Colorado, United States
Lat 39.7392  Lon -104.9847
Pipeline latency: 5098 ms
──────────────────────────────────────────────────────────────

Current Conditions
Temperature : 18.2 °C
Wind        : 8.6 km/h @ 255.0°
Time of day : Night 🌙

Daily Forecast
Date         Conditions               High    Low    Rain     Wind  Category        Bar
──────────── ────────────────────── ────── ────── ─────── ────────  ──────────────  ──────────
2026-03-24   Overcast                28.1°   9.5°    0.0mm    21.1kph  Warm            ████████░░
2026-03-25   Overcast                30.9°  12.3°    0.0mm    12.5kph  Warm            ████████░░
2026-03-26   Light drizzle           28.7°  12.9°    0.6mm    29.4kph  Warm            ████████░░
2026-03-27   Light drizzle           13.3°   6.9°    0.1mm    18.8kph  Cool            █████░░░░░
2026-03-28   Light drizzle           25.9°   8.4°    0.2mm    30.1kph  Warm            ███████░░░
2026-03-29   Overcast                24.5°  14.5°    0.0mm    23.6kph  Mild            ███████░░░
2026-03-30   Overcast                23.5°  15.1°    0.0mm    20.9kph  Mild            ███████░░░

──────────────────────────────────────────────────────────────
Period Summary (7 days)
Avg high      : 24.99 °C  (peak 30.9 °C)
Avg low       : 11.37 °C  (min 6.9 °C)
Total rain    : 0.9 mm
Avg wind      : 22.34 kph
Dominant feel : Warm
Data complete : 100.0%
──────────────────────────────────────────────────────────────
```

---

## Features Demonstrated
- API integration and external data handling  
- CLI tool design and argument parsing  
- Data pipeline construction and transformation logic  
- Structured output formatting for usability  
- Debugging and iteration of real-world data workflows  

---

## What I Learned
- How to work with REST APIs and handle real-time data  
- Parsing and transforming JSON into usable data structures  
- Designing modular, maintainable Python code  
- Debugging and improving performance using AI-assisted development tools  

---

## Notes
AI tool (Claude) was used to assist with debugging, iteration, and performance improvements during development.
