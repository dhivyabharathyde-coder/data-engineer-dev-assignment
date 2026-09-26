import requests
import json
from pathlib import Path
from datetime import datetime, timezone

api_url = 'https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/UniversityChapters_Public/FeatureServer/0'

query_url = f"{api_url}/query"

params = {
    "where": "State IN ('CA','OR','WA')",
    "outFields": "*",
    "returnGeometry": "true",
    "f": "json"
}

response = requests.get(query_url, params=params)

print(response.status_code)

data = response.json()

bronze_dir = Path("bronze")
bronze_dir.mkdir(exist_ok=True)

bronze_file = bronze_dir / "university_chapters_raw.json"

bronze_data = {
    "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
    "source": "ArcGIS University Chapters API",
    "data": data
}

with open(bronze_file, "w", encoding="utf-8") as f:
    json.dump(bronze_data, f, indent=2)

print(f"Records received: {len(data.get('features', []))}")
print(f"Bronze file created: {bronze_file}")