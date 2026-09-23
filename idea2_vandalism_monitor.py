#!/usr/bin/env python3
"""
Idea 2: Real-Time Vandalism & Revert Risk Monitor
Data Engineering Techniques:
- Direct ingestion of Wikimedia's real-time ML inference stream
- Classification & scoring of streaming edits
- Real-time alerting for anomalies (high revert-risk edits)
- Editor reputation correlation (new user vs veteran vs bot)
"""

import requests
import json
import time

STREAM_URL = "https://stream.wikimedia.org/v2/stream/mediawiki.page_revert_risk_prediction_change.v1"
HEADERS = {"User-Agent": "IISc-DataEngineering-Project/1.0"}

DURATION = 45  # Run for 45s test
TARGET_WIKI = "enwiki"
OUTPUT_FILE = "vandalism_risk_events.json"

print("=" * 65)
print(" IDEA 2: REAL-TIME VANDALISM & REVERT RISK MONITOR")
print(f" Target Stream: {STREAM_URL.split('/')[-1]}")
print(f" Filter: {TARGET_WIKI} (English Wikipedia)")
print(" Press Ctrl+C anytime to stop early.")
print("=" * 65 + "\n")

start_time = time.time()
total_scored = 0
high_risk_count = 0
medium_risk_count = 0
low_risk_count = 0
flagged_events = []

try:
    with requests.get(STREAM_URL, headers=HEADERS, stream=True) as resp:
        resp.raise_for_status()
        print("Connected to ML Revert Risk stream. Analyzing incoming edits...\n")

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

            # Filter for English Wikipedia
            if event.get("wiki_id") != TARGET_WIKI:
                continue

            total_scored += 1

            page = event.get("page", {})
            title = page.get("page_title", "Unknown")
            revision = event.get("revision", {})
            editor = revision.get("editor", {})
            user_text = editor.get("user_text", "Unknown")
            is_bot = editor.get("is_bot", False)
            user_edits = editor.get("edit_count", 0)

            # Get predicted revert risk (probability edit is malicious / will be reverted)
            prediction = event.get("predicted_classification", {})
            probs = prediction.get("probabilities", {})
            revert_risk = probs.get("true", 0.0)

            # Risk levels
            if revert_risk >= 0.50:
                risk_level = "HIGH RISK"
                high_risk_count += 1
                flag_icon = "🚨"
            elif revert_risk >= 0.20:
                risk_level = "MEDIUM RISK"
                medium_risk_count += 1
                flag_icon = "⚠️"
            else:
                risk_level = "LOW RISK"
                low_risk_count += 1
                flag_icon = "✅"

            record = {
                "timestamp": event.get("dt", now),
                "title": title,
                "user": user_text,
                "is_bot": is_bot,
                "user_total_edits": user_edits,
                "revert_risk": round(revert_risk, 4),
                "risk_level": risk_level,
                "comment": revision.get("comment", "")
            }

            # Print immediately if high or medium risk
            if revert_risk >= 0.20:
                print(f"{flag_icon} [{risk_level}] Risk: {revert_risk:.2%} | Article: '{title}'")
                print(f"   Editor: {user_text} (Total edits: {user_edits:,} | Bot: {is_bot})")
                print(f"   Comment: {revision.get('comment', 'None')}\n")
                flagged_events.append(record)
            elif total_scored % 10 == 0:
                print(f"   [Scored {total_scored} edits] Latest: '{title}' -> Risk: {revert_risk:.2%}")

except KeyboardInterrupt:
    print("\nStopped by user.")

# Save flagged events to JSON
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(flagged_events, f, indent=2)

elapsed = time.time() - start_time
print("=" * 65)
print(f"Completed in {elapsed:.1f}s")
print(f"Total enwiki edits evaluated by ML : {total_scored}")
print(f"  - Low Risk (<20%)                 : {low_risk_count}")
print(f"  - Medium Risk (20%-50%)           : {medium_risk_count}")
print(f"  - High Risk / Potential Vandal (>50%): {high_risk_count}")
print(f"Flagged events logged to            : {OUTPUT_FILE}")
print("=" * 65)
