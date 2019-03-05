pipeline = [
    {"$facet": {
        "h": [
            {"$match": {"$and": [{"target": {"$lte": 75000}}, {"target": {"$gte": 6000}}]}},
            {"$sample": {"size": 9000}}
        ],
        "l": [
            {"$match": {"$and": [{"target": {"$lte": 6000}}]}},
            {"$sample": {"size": 1000}}
        ]
    }},
    {"$project": {
        "total": {"$setUnion": ["$h", "$l"]},
    }},
    {"$unwind": "$total"},
    {"$replaceRoot": {"newRoot": "$total"}},
    {"$out": "agg_d_lim"}
]
