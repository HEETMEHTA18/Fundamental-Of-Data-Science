"""Generate synthetic traffic & area-monitor datasets (1M+ rows each) for Apache Spark demos.

Run:
    python data_generation.py

Outputs:
    data/traffic_data.csv   (1,500,000 rows)
    data/area_monitor.csv   (1,000,000 rows)
"""

import numpy as np
import pandas as pd

rng = np.random.default_rng(42)

TRAFFIC_ROWS = 1_500_000
MONITOR_ROWS = 1_000_000
N_AREAS = 100
CHUNK = 100_000

AREAS = np.arange(1, N_AREAS + 1)
CITIES = np.array(["Mumbai", "Delhi", "Bengaluru", "Pune", "Ahmedabad"], dtype=object)
AREA_CITY = CITIES[np.arange(N_AREAS) % len(CITIES)]
VEHICLE_TYPES = np.array(["car", "bus", "truck", "bike", "auto"], dtype=object)
WEATHER = np.array(["clear", "rain", "fog", "cloudy"], dtype=object)
CONGESTION = np.array(["low", "medium", "high"], dtype=object)
SENSOR_TYPES = np.array(["speed", "volume", "incident", "weather"], dtype=object)
STATUS = np.array(["active", "active", "active", "faulty"], dtype=object)
ROAD_IDS = np.array([f"R{i:03d}" for i in range(1, 51)], dtype=object)


def make_traffic_chunk(start: int, size: int) -> pd.DataFrame:
    area_ids = rng.integers(1, N_AREAS + 1, size=size)
    base = np.datetime64("2024-01-01T00:00:00")
    ts = base + rng.integers(0, 90 * 24 * 3600, size=size).astype("timedelta64[s]")
    speeds = np.clip(rng.normal(55, 22, size=size), 5, 130).round(1)
    volumes = rng.integers(0, 500, size=size)
    congestion = np.where(volumes < 150, "low", np.where(volumes < 350, "medium", "high"))
    return pd.DataFrame(
        {
            "event_id": np.arange(start, start + size),
            "area_id": area_ids,
            "road_id": ROAD_IDS[rng.integers(0, len(ROAD_IDS), size=size)],
            "timestamp": ts,
            "date": ts.astype("datetime64[D]"),
            "hour": (ts.astype("datetime64[h]") - ts.astype("datetime64[D]")).astype("timedelta64[h]").astype(int),
            "vehicle_type": rng.choice(VEHICLE_TYPES, size=size),
            "speed_kmh": speeds,
            "traffic_volume": volumes,
            "weather_condition": rng.choice(WEATHER, size=size),
            "congestion_level": congestion,
        }
    )


def make_monitor_chunk(start: int, size: int) -> pd.DataFrame:
    area_ids = rng.integers(1, N_AREAS + 1, size=size)
    base = np.datetime64("2024-01-01T00:00:00")
    ts = base + rng.integers(0, 90 * 24 * 3600, size=size).astype("timedelta64[s]")
    sensor_type = rng.choice(SENSOR_TYPES, size=size)
    reading = np.where(
        sensor_type == "speed", np.clip(rng.normal(50, 20, size=size), 5, 130),
        np.where(sensor_type == "volume", rng.integers(0, 600, size=size),
        np.where(sensor_type == "incident", rng.integers(0, 15, size=size),
                 rng.integers(0, 4, size=size))),
    ).round(1)
    return pd.DataFrame(
        {
            "monitor_id": np.arange(start, start + size),
            "area_id": area_ids,
            "city": AREA_CITY[area_ids - 1],
            "sensor_type": sensor_type,
            "reading_value": reading,
            "reading_timestamp": ts,
            "status": rng.choice(STATUS, size=size),
        }
    )


def write_chunked(df_maker, path: str, total: int):
    first = True
    for s in range(0, total, CHUNK):
        size = min(CHUNK, total - s)
        chunk = df_maker(s, size)
        chunk.to_csv(path, mode="w" if first else "a", header=first, index=False)
        first = False
        print(f"  wrote {s + size:,} / {total:,} rows -> {path}")
    print(f"Done: {path}")


if __name__ == "__main__":
    print("Generating traffic_data.csv (1.5M rows)...")
    write_chunked(make_traffic_chunk, "data/traffic_data.csv", TRAFFIC_ROWS)
    print("Generating area_monitor.csv (1M rows)...")
    write_chunked(make_monitor_chunk, "data/area_monitor.csv", MONITOR_ROWS)
