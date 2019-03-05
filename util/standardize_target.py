from pymongo import MongoClient
import json

pipeline = [
    # {"$match": {"target": {"$lte": 250000}}},
    {"$group": {
        "_id": None,
        "std": {"$stdDevPop": "$y"},
        "mean": {"$avg": "$y"},
    }}
]

if __name__ == "__main__":
    cl = MongoClient("localhost", 27017)
    db = cl.dwh

    std = db.train_lim.aggregate(pipeline).next()

    print(std)

    for doc in db.train_lim.find():
        doc["y_std"] = (doc["y"] - std["mean"]) / std["std"]
        db.train_lim.update_one({'_id': doc["_id"]}, {"$set": doc}, upsert=False)

    for doc in db.test_lim.find():
        doc["y_std"] = (doc["y"] - std["mean"]) / std["std"]
        db.test_lim.update_one({'_id': doc["_id"]}, {"$set": doc}, upsert=False)

    with open('nn-std.json', 'w') as outfile:
        json.dump({"std": std["std"], "mean": std["mean"]}, outfile)
