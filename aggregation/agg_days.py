from pymongo import MongoClient
from datetime import datetime
import hashlib


pipeline = [
    {"$match": {"created": {"$ne": None}}},
    {"$lookup": {
        "from": "d_produkt",
        "localField": "quantityId",
        "foreignField": "product_quantityID",
        "as": "product"}
    },
    {"$match": {"product.0": {"$exists": True}}},
    {"$facet": {
        "id": [
            {"$group": {
                "_id": {"id": {"$arrayElemAt": ['$product.id', 0]},
                        "year": {"$year": "$created"},
                        "month": {"$month": "$created"},
                        "day": {"$dayOfMonth": "$created"}
                        },
                "count": {"$sum": 1},
                "target": {"$sum": "$Absatzmenge"}}
            }
        ],
        "cat1": [
            {"$group": {
                "_id": {"id": {"$arrayElemAt": ['$product.01 | Produktebene', 0]},
                        "year": {"$year": "$created"},
                        "month": {"$month": "$created"},
                        "day": {"$dayOfMonth": "$created"}
                        },
                "count": {"$sum": 1},
                "target": {"$sum": "$Absatzmenge"}}
            }

        ],
        "cat2": [
            {"$group": {
                "_id": {"id": {"$arrayElemAt": ['$product.02 | Produktkategorie', 0]},
                        "year": {"$year": "$created"},
                        "month": {"$month": "$created"},
                        "day": {"$dayOfMonth": "$created"}
                        },
                "count": {"$sum": 1},
                "target": {"$sum": "$Absatzmenge"}}
            }
        ],
        "cat3": [
            {"$group": {
                "_id": {"id": {"$arrayElemAt": ['$product.03 | Produktgruppe', 0]},
                        "year": {"$year": "$created"},
                        "month": {"$month": "$created"},
                        "day": {"$dayOfMonth": "$created"}
                        },
                "count": {"$sum": 1},
                "target": {"$sum": "$Absatzmenge"}}
            }
        ]
    }},
    {"$project": {
        "total": {"$setUnion": ["$id", "$cat1", "$cat2", "$cat3"]},
    }},
    {"$unwind": "$total"},
    {"$replaceRoot": {"newRoot": "$total"}}
]

client = MongoClient("localhost", 27017)
db = client.dwh
col = db.agg_d

col.create_index("id", unique=False)

# with open('agg_m.json') as f:
#     std = json.load(f)
#
# assert std is not None

for doc in db.f_sales.aggregate(pipeline, allowDiskUse=True):
    doc["id"] = hashlib.md5(str(doc["_id"]["id"]).encode()).hexdigest()
    doc["id_desc"] = doc["_id"]["id"]
    doc["year"] = doc["_id"]["year"]
    doc["month"] = doc["_id"]["month"]
    doc["day"] = doc["_id"]["day"]
    doc["date"] = datetime(year=doc["year"], month=doc["month"], day=doc["day"])
    del doc["_id"]
    # doc["target_std"] = (doc["target"] - std["mean"]) / std["std"]
    col.insert(doc)
