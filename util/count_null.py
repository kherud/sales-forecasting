from tqdm import tqdm
from pymongo import MongoClient
from dateutil.relativedelta import relativedelta

"""
{0: 1565373, 1: 156582, 2: 107224, 3: 91474, 4: 84159, 5: 81974, 6: 79872, 7: 80064, 8: 82053, 9: 84229, 10: 87865, 11: 95181, 12: 102484}
"""


client = MongoClient("localhost", 27017)
db = client.dwh
total = db.agg_d.count()

n = {k: 0 for k in range(13)}
print(total)
with tqdm(total=total) as pbar:
    for doc in db.agg_d.find():
        hist = db.histories.find_one({"id": doc["id"]})

        assert hist is not None

        nl = 0

        for i in range(12):
            date = doc["date"] - relativedelta(months=i + 1)
            month = "{:02d}-{:02d}".format(date.year, date.month)

            if month not in hist:
                nl += 1

        pbar.update(1)
        n[nl] += 1

print(n)
