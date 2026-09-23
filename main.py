import requests
import json
import time
import ipaddress
from collections import deque

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
DURATION = 60  # 1 minute
TARGET_SERVER = "en.wikipedia.org"
OUTPUT_FILE = "example.json"

headers = {
    "User-Agent": "IISc-DataEngineering-Project/1.0"
}


def is_ip_address(user_str: str) -> bool:
    try:
        ipaddress.ip_address(user_str)
        return True
    except ValueError:
        return False


event_count = 0
byte_count = 0
bot_count = 0
human_count = 0
ip_count = 0
registered_count = 0

# Rolling window for the last 10 seconds of events
recent_events_10s = deque()

start = time.time()
last_progress_time = start

print(f"Connecting to Wikimedia stream ({URL})...")
print(f"Filtering for: {TARGET_SERVER} (English Wikipedia only)")
print(f"Streaming for {DURATION}s (Press Ctrl+C anytime to stop early and see summary)...\n")

try:
    with requests.get(URL, headers=headers, stream=True) as response:
        response.raise_for_status()
        print("Connected! Receiving events...")

        for line in response.iter_lines():
            now = time.time()
            if now - start >= DURATION:
                break

            # SSE event payloads start with "data:"
            if line.startswith(b"data:"):
                payload = line[5:].strip()

                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue

                # 1. Restrict strictly to English Wikipedia
                if data.get("server_name") != TARGET_SERVER:
                    continue

                event_count += 1
                byte_count += len(payload)

                # 2. Bot vs Human tracking
                is_bot = data.get("bot", False)
                if is_bot:
                    bot_count += 1
                else:
                    human_count += 1

                # 3. Location / User identification (IP vs registered account)
                user = data.get("user", "")
                if is_ip_address(user):
                    ip_count += 1
                else:
                    registered_count += 1

                # 4. Maintain rolling 10-second buffer
                recent_events_10s.append((now, data))
                while recent_events_10s and now - recent_events_10s[0][0] > 10:
                    recent_events_10s.popleft()

            # Print live progress every 5 seconds
            if now - last_progress_time >= 5:
                elapsed_so_far = now - start
                rate = event_count / elapsed_so_far if elapsed_so_far > 0 else 0
                mb = byte_count / (1024 ** 2)
                print(
                    f"[{elapsed_so_far:4.1f}s / {DURATION}s] "
                    f"enwiki Events: {event_count:,} | "
                    f"Humans: {human_count} | "
                    f"Bots: {bot_count} | "
                    f"Rate: {rate:.1f} events/s",
                    flush=True
                )
                last_progress_time = now

except KeyboardInterrupt:
    print("\nStopped early by user. Generating summary for captured data...")

elapsed = time.time() - start
if elapsed <= 0:
    elapsed = 0.001

events_per_second = event_count / elapsed

# Extrapolate to 24 hours
events_per_day = events_per_second * 86400
bytes_per_day = (byte_count / elapsed) * 86400
gb_per_day = bytes_per_day / (1024 ** 3)

# Save events from the last 10 seconds into example.json
final_time = time.time()
while recent_events_10s and final_time - recent_events_10s[0][0] > 10:
    recent_events_10s.popleft()

saved_events = [event for _, event in recent_events_10s]
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(saved_events, f, indent=2)

print("\n" + "=" * 45)
print(f"Target Project       : {TARGET_SERVER}")
print(f"Measurement duration : {elapsed:.1f} seconds")
print(f"Total Events         : {event_count:,}")
print(f"  - Human edits      : {human_count:,} ({human_count / max(event_count, 1) * 100:.1f}%)")
print(f"  - Bot edits        : {bot_count:,} ({bot_count / max(event_count, 1) * 100:.1f}%)")
print(f"  - Anonymous (IPs)  : {ip_count:,} (location resolvable via GeoIP)")
print(f"  - Registered Users : {registered_count:,} (location private)")
print(f"Data received        : {byte_count / 1024**2:.2f} MB")
print(f"Events/sec           : {events_per_second:.2f}")
print(f"Estimated events/day : {events_per_day:,.0f}")
print(f"Estimated GB/day     : {gb_per_day:.2f} GB")
print(f"Logged to file       : {OUTPUT_FILE} ({len(saved_events)} events from last 10s)")
print("=" * 45)