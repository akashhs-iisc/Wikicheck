#!/usr/bin/env python3
"""
Idea 3: Global Geospatial Edit Pulse (Live World Map)
Data Engineering Techniques:
- Spatial stream enrichment:
  1. Geocoding anonymous editor IPs -> (Country, City, Lat, Lon)
  2. Entity coordinate resolution via Wikipedia Page Summary -> Article (Lat, Lon)
- Spatial grouping and country-level edit frequency aggregation
- Preparing GeoJSON/Point payloads for map visualization (Leaflet / Streamlit / Kepler.gl)
"""

import requests
import json
import time
import ipaddress
from collections import Counter

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"
HEADERS = {"User-Agent": "IISc-DataEngineering-Project/1.0"}

DURATION = 45
OUTPUT_FILE = "geopulse_events.json"

# In-memory caches to avoid duplicate network lookups
ip_geo_cache = {}
article_coords_cache = {}

country_counter = Counter()
geo_points = []


def is_ip(user_str: str) -> bool:
    try:
        ipaddress.ip_address(user_str)
        return True
    except ValueError:
        return False


def lookup_ip_geolocation(ip: str) -> dict:
    """Resolve IP to geographic coordinates using IP-API."""
    if ip in ip_geo_cache:
        return ip_geo_cache[ip]
    try:
        r = requests.get(f"http://ip-api.com/json/{ip}", timeout=2)
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == "success":
                geo = {
                    "country": data.get("country"),
                    "city": data.get("city"),
                    "region": data.get("regionName"),
                    "lat": data.get("lat"),
                    "lon": data.get("lon"),
                    "isp": data.get("isp"),
                }
                ip_geo_cache[ip] = geo
                return geo
    except Exception:
        pass
    ip_geo_cache[ip] = None
    return None


def lookup_article_coordinates(title: str) -> dict:
    """Check if the article represents a place with physical coordinates."""
    if title in article_coords_cache:
        return article_coords_cache[title]
    try:
        url = f"{SUMMARY_API}{requests.utils.quote(title)}"
        r = requests.get(url, headers=HEADERS, timeout=2)
        if r.status_code == 200:
            data = r.json()
            coords = data.get("coordinates")
            if coords:
                res = {
                    "lat": coords.get("lat"),
                    "lon": coords.get("lon"),
                    "description": data.get("description", "Location on Wikipedia")
                }
                article_coords_cache[title] = res
                return res
    except Exception:
        pass
    article_coords_cache[title] = None
    return None


print("=" * 65)
print(" IDEA 3: GLOBAL GEOSPATIAL EDIT PULSE")
print(f" Target: en.wikipedia.org | Duration: {DURATION}s")
print(" Press Ctrl+C anytime to stop early.")
print("=" * 65 + "\n")

start_time = time.time()
total_checked = 0

try:
    with requests.get(STREAM_URL, headers=HEADERS, stream=True) as resp:
        resp.raise_for_status()
        print("Connected! Listening for spatial events...\n")

        for line in resp.iter_lines():
            now = time.time()
            if now - start_time >= DURATION:
                break

            if not line.startswith(b"data:"):
                continue

            try:
                event = json.loads(line[5:].strip())
            except json.JSONDecodeError:
                continue

            if event.get("server_name") != "en.wikipedia.org":
                continue

            total_checked += 1
            user = event.get("user", "")
            title = event.get("title", "")

            found_geo = False

            # 1. Check if editor is an anonymous IP
            if is_ip(user):
                geo = lookup_ip_geolocation(user)
                if geo:
                    country_counter[geo["country"]] += 1
                    point = {
                        "type": "editor_location",
                        "lat": geo["lat"],
                        "lon": geo["lon"],
                        "label": f"Editor in {geo['city']}, {geo['country']}",
                        "article": title,
                        "ip": user
                    }
                    geo_points.append(point)
                    print(f"🌍 [EDITOR GEO] IP {user} edited '{title}' from {geo['city']}, {geo['country']} ({geo['lat']}, {geo['lon']})")
                    found_geo = True

            # 2. Check if the article itself has physical coordinates (places/landmarks)
            if not found_geo and event.get("namespace") == 0:
                coords = lookup_article_coordinates(title)
                if coords:
                    point = {
                        "type": "article_subject_location",
                        "lat": coords["lat"],
                        "lon": coords["lon"],
                        "label": f"Article Subject: {title}",
                        "description": coords.get("description"),
                        "article": title,
                        "editor": user
                    }
                    geo_points.append(point)
                    print(f"📍 [SUBJECT GEO] Article '{title}' located at ({coords['lat']}, {coords['lon']}) | {coords.get('description')}")

except KeyboardInterrupt:
    print("\nStopped by user.")

# Save points to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(geo_points, f, indent=2)

elapsed = time.time() - start_time
print("\n" + "=" * 65)
print(f"Completed in {elapsed:.1f}s")
print(f"Total enwiki events examined      : {total_checked}")
print(f"Geolocated points captured        : {len(geo_points)}")
if country_counter:
    print("\nTop Editor Countries Identified:")
    for country, count in country_counter.most_common(5):
        print(f"  • {country:<25}: {count} edits")
print(f"\nMap points exported to            : {OUTPUT_FILE}")
print("=" * 65)
