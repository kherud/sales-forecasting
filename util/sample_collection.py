from pymongo import MongoClient


def sample_collection(col_name):
    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db[col_name]

    pipeline = [
        {"$sample": {"size": col.count()}},
        {"$out": col_name}
    ]
    col.aggregate(pipeline)


if __name__ == "__main__":
    sample_collection("test")
