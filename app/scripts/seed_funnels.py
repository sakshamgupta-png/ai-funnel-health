from __future__ import annotations

import json
from pathlib import Path

from pymongo import MongoClient

from app.configs.settings import get_settings


def main() -> None:
    settings = get_settings()

    # seed_funnels.py is inside: app/scripts/
    # so parents[1] = app/
    app_dir = Path(__file__).resolve().parents[1]
    funnels_dir = app_dir / "funnels"

    print(f"Looking for funnel.json files inside: {funnels_dir}")

    funnel_files = list(funnels_dir.glob("*/funnel.json"))
    if not funnel_files:
        print("No funnel.json files found")
        return

    client = MongoClient(settings.mongodb_uri)
    db = client[settings.mongodb_db_name]
    coll = db["funnels"]

    for path in funnel_files:
        with path.open("r", encoding="utf-8") as f:
            doc = json.load(f)

        funnel_id = doc["funnel_id"]
        coll.update_one(
            {"funnel_id": funnel_id},
            {"$set": doc},
            upsert=True,
        )
        print(f"Upserted funnel: {funnel_id} from {path}")

    print("\nCurrent funnels in Mongo:")
    for item in coll.find({}, {"_id": 0, "funnel_id": 1, "name": 1, "enabled": 1}):
        print(item)


if __name__ == "__main__":
    main()