from pymongo import MongoClient
import matplotlib.pyplot as plt


if __name__ == "__main__":
    cl = MongoClient("localhost", 27017)
    db = cl.dwh
    col = db.train

    x = [doc["y"] for doc in col.find()]

    plt.hist(x, bins=100)
    plt.show()
