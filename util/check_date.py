from pymongo import MongoClient

client = MongoClient("localhost", 27017)
db = client.dwh2
col = db.f_sales

pipeline = [
    # {"$project": {"fieldType": {"$type": "$created"}, "date": "$created"}},
    {"$match": {"created": {"$ne": None}}},
    {"$project": {
        "year": {"$year": "$created"},
        "month": {"$month": "$created"}
    }},
    {"$group": {
        "_id": None,
        "distinctDate": {"$addToSet": {"year": "$year", "month": "$month"}}
    }}
]

for x in col.aggregate(pipeline):
    print(x)
