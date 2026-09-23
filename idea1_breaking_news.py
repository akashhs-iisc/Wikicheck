#!/usr/bin/env python3
"""
Idea 1: Real-time Breaking News & Edit Spike Radar
Data Engineering Techniques:
- Streaming window aggregation (Sliding window of edit frequency per article)
- Out-of-band REST enrichment (Page Summary API for articles exceeding threshold)
- Anomaly / velocity detection for breaking events
"""

import requests
import json
import time
from collections import defaultdict, deque

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/"
HEADERS = {"User-Agent": "IISc-DataEngineering-Project/1.0"}

DURATION = 45          # Run for 45s test
WINDOW_SECONDS = 30    # Rolling window size
ALERT_THRESHOLD = 2    # Min edits within window to trigger enrichment in test
OUTPUT_FILE = "breaking_news_alerts.json"

# State: article_title -> deque of edit timestamps
edit_history = defaultdict(deque)
alerts_triggered = []
enriched_articles_cache = {}

print("=" * 65)
print(" IDEA 1: BREAKING NEWS & EDIT SPIKE RADAR")
print(f" Target: en.wikipedia.org | Window: {WINDOW_SECONDS}s | Threshold: >={ALERT_THRESHOLD} edits")
print(" Press Ctrl+C anytime to stop early.")
print("=" * 65 + "\n")

start_time = time.time()
total_events = 0


def fetch_article_summary(title: str) -> dict:
    """Fetch rich metadata from Wikipedia Page Summary REST API."""
    if title in enriched_articles_cache:
        return enriched_articles_cache[title]

    try:
        url = f"{SUMMARY_API}{requests.utils.quote(title)}"
        resp = requests.get(url, headers=HEADERS, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            summary = {
                "title": data.get("title"),
                "description": data.get("description", "No description"),
                "extract": data.get("extract", "")[:180] + "..." if data.get("extract") else "",
                "thumbnail": data.get("thumbnail", {}).get("source", "No image"),
                "wikibase_item": data.get("wikibase_item", "N/A"),
            }
            enriched_articles_cache[title] = summary
            return summary
    except Exception as e:
        pass

    fallback = {"title": title, "description": "N/A", "extract": "N/A", "thumbnail": "N/A"}
    enriched_articles_cache[title] = fallback
    return fallback


try:
    with requests.get(STREAM_URL, headers=HEADERS, stream=True) as resp:
        resp.raise_for_status()
        print("Connected to stream. Monitoring edit velocity...\n")

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

            # Only track main article edits (namespace 0)
            if event.get("namespace") != 0:
                continue

            total_events += 1
            title = event.get("title", "")
            user = event.get("user", "")

            # Maintain sliding window for this title
            timestamps = edit_history[title]
            timestamps.append(now)

            # Purge timestamps older than WINDOW_SECONDS
            while timestamps and now - timestamps[0] > WINDOW_SECONDS:
                timestamps.popleft()

            velocity = len(timestamps)

            # Check if threshold crossed
            if velocity >= ALERT_THRESHOLD:
                # Enrich with Wikipedia Page Summary API
                summary = fetch_article_summary(title)

                alert_record = {
                    "timestamp": now,
                    "title": title,
                    "recent_edits_in_window": velocity,
                    "window_seconds": WINDOW_SECONDS,
                    "latest_editor": user,
                    "summary": summary
                }
                alerts_triggered.append(alert_record)

                print(f"[SPIKE ALERT] '{title}' -> {velocity} edits in last {WINDOW_SECONDS}s!")
                print(f"  Description: {summary.get('description')}")
                print(f"  Extract:     {summary.get('extract')}")
                print(f"  Wikidata:    {summary.get('wikibase_item')}\n")

                # Clear window to avoid duplicate spamming
                timestamps.clear()

except KeyboardInterrupt:
    print("\nStopped by user.")

# Save alerts to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(alerts_triggered, f, indent=2)

elapsed = time.time() - start_time
print("=" * 65)
print(f"Completed in {elapsed:.1f}s")
print(f"Total enwiki main articles checked : {total_events}")
print(f"Spike alerts triggered             : {len(alerts_triggered)}")
print(f"Enriched alerts logged to          : {OUTPUT_FILE}")
print("=" * 65)
