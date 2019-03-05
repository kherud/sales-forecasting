from pymongo import MongoClient
from datetime import datetime
import hashlib
import json

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
                        },
                "count": {"$sum": 1},
                "target": {"$sum": "$Absatzmenge"}}
            }
        ]
    }},
    {"$project": {
        "total": {"$setUnion": ["$id", "$cat1", "$cat2", "$cat3"]},
        "std": {"$stdDevSamp": "$id.target"},
        "mean": {"$avg": "$id.target"},
    }},
    {"$unwind": "$total"},
]

client = MongoClient("localhost", 27017)
db = client.dwh
col = db.agg_m

col.create_index("id", unique=False)

std, mean = 0, 0
for doc in db.f_sales.aggregate(pipeline, allowDiskUse=True):
    try:
        std, mean = doc["std"], doc["mean"]
        doc["id"] = hashlib.md5(str(doc["total"]["_id"]["id"]).encode()).hexdigest()
        doc["id_desc"] = doc["total"]["_id"]["id"]
        doc["year"] = doc["total"]["_id"]["year"]
        doc["month"] = doc["total"]["_id"]["month"]
        doc["date"] = datetime(year=doc["year"], month=doc["month"], day=1)
        doc["target_std"] = (doc["total"]["target"] - mean) / std
        del doc["total"]
        del doc["std"]
        del doc["mean"]
        col.insert(doc)
    except TypeError:
        print(doc)

with open('agg_m.json', 'w') as outfile:
    json.dump({"std": std, "mean": mean}, outfile)
