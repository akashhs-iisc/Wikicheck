#!/usr/bin/env python3
"""
Idea 4: Knowledge Growth & Churn Analytics
Data Engineering Techniques:
- Multi-dimensional stream rollups (bytes delta, bot vs human, namespace distributions)
- Net knowledge creation calculation (bytes added vs deleted)
- High-frequency rolling statistics and dashboard rendering
- Metrics export for time-series / analytical data stores (ClickHouse, Prometheus)
"""

import requests
import json
import time
from collections import Counter

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {"User-Agent": "IISc-DataEngineering-Project/1.0"}

DURATION = 45
TARGET_SERVER = "en.wikipedia.org"
OUTPUT_FILE = "churn_summary.json"

# Namespace map
NAMESPACES = {
    0: "Main Article",
    1: "Talk",
    2: "User",
    3: "User Talk",
    4: "Wikipedia Project",
    5: "Project Talk",
    6: "File",
    10: "Template",
    14: "Category",
}

print("=" * 65)
print(" IDEA 4: KNOWLEDGE GROWTH & CHURN ANALYTICS")
print(f" Target: {TARGET_SERVER} | Duration: {DURATION}s")
print(" Press Ctrl+C anytime to stop early.")
print("=" * 65 + "\n")

start_time = time.time()
last_print_time = start_time

total_edits = 0
bytes_added = 0
bytes_deleted = 0

human_count = 0
bot_count = 0
human_bytes_delta = 0
bot_bytes_delta = 0

minor_edits = 0
article_counter = Counter()
namespace_counter = Counter()
user_counter = Counter()

try:
    with requests.get(STREAM_URL, headers=HEADERS, stream=True) as resp:
        resp.raise_for_status()
        print("Connected to stream. Computing rolling churn metrics...\n")

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

            if event.get("server_name") != TARGET_SERVER:
                continue

            total_edits += 1

            title = event.get("title", "Unknown")
            user = event.get("user", "Unknown")
            is_bot = event.get("bot", False)
            is_minor = event.get("minor", False)
            ns = event.get("namespace", 0)

            # Track distributions
            article_counter[title] += 1
            namespace_name = NAMESPACES.get(ns, f"Other (NS {ns})")
            namespace_counter[namespace_name] += 1
            user_counter[user] += 1
            if is_minor:
                minor_edits += 1

            # Calculate content churn (bytes added / removed)
            length = event.get("length", {})
            old_len = length.get("old", 0)
            new_len = length.get("new", 0)

            if old_len is not None and new_len is not None:
                delta = new_len - old_len
                if delta > 0:
                    bytes_added += delta
                else:
                    bytes_deleted += abs(delta)

                if is_bot:
                    bot_count += 1
                    bot_bytes_delta += delta
                else:
                    human_count += 1
                    human_bytes_delta += delta
            else:
                if is_bot:
                    bot_count += 1
                else:
                    human_count += 1

            # Live status update every 5 seconds
            if now - last_print_time >= 5:
                elapsed = now - start_time
                net_kb = (bytes_added - bytes_deleted) / 1024
                rate = total_edits / elapsed if elapsed > 0 else 0
                print(
                    f"[{elapsed:4.1f}s] Edits: {total_edits:4d} | "
                    f"Net Growth: {net_kb:+6.1f} KB | "
                    f"Humans: {human_count:3d} ({human_count / max(total_edits, 1) * 100:.0f}%) | "
                    f"Bots: {bot_count:3d} | "
                    f"Throughput: {rate:.1f} edits/s",
                    flush=True
                )
                last_print_time = now

except KeyboardInterrupt:
    print("\nStopped by user.")

elapsed = time.time() - start_time
net_bytes = bytes_added - bytes_deleted

summary_metrics = {
    "duration_seconds": round(elapsed, 1),
    "total_edits": total_edits,
    "edits_per_second": round(total_edits / elapsed, 2) if elapsed > 0 else 0,
    "churn": {
        "bytes_added": bytes_added,
        "bytes_deleted": bytes_deleted,
        "net_bytes": net_bytes,
        "net_kb": round(net_bytes / 1024, 2),
        "gross_churn_kb": round((bytes_added + bytes_deleted) / 1024, 2)
    },
    "actors": {
        "human_edits": human_count,
        "bot_edits": bot_count,
        "bot_ratio_pct": round(bot_count / max(total_edits, 1) * 100, 1),
        "human_net_bytes": human_bytes_delta,
        "bot_net_bytes": bot_bytes_delta
    },
    "top_edited_articles": dict(article_counter.most_common(5)),
    "namespace_breakdown": dict(namespace_counter.most_common(5)),
    "top_contributors": dict(user_counter.most_common(5))
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(summary_metrics, f, indent=2)

print("\n" + "=" * 65)
print(" FINAL KNOWLEDGE CHURN DASHBOARD")
print("=" * 65)
print(f" Duration            : {elapsed:.1f} seconds")
print(f" Total Edits         : {total_edits:,} ({total_edits / max(elapsed, 0.001):.2f} edits/sec)")
print(f" Net Content Growth  : {net_bytes:+d} bytes ({net_bytes / 1024:+.2f} KB)")
print(f"   ├─ Bytes Added    : +{bytes_added:,} bytes")
print(f"   └─ Bytes Deleted  : -{bytes_deleted:,} bytes")
print(f" Actor Split         : Humans {human_count} ({human_count / max(total_edits, 1) * 100:.1f}%) | Bots {bot_count} ({bot_count / max(total_edits, 1) * 100:.1f}%)")
print(f" Minor Edit Ratio    : {minor_edits / max(total_edits, 1) * 100:.1f}% ({minor_edits} edits)")

print("\n Top 3 Most Active Articles:")
for title, cnt in article_counter.most_common(3):
    print(f"   • {title[:40]:<40} : {cnt} edits")

print("\n Top Namespaces:")
for ns, cnt in namespace_counter.most_common(3):
    print(f"   • {ns:<25} : {cnt} edits")

print(f"\n Detailed metrics written to: {OUTPUT_FILE}")
print("=" * 65)
