from pymongo import MongoClient
import numpy as np
import _pickle
import keras
import threading


class DataGenerator(keras.utils.Sequence):
    def __init__(self, mode="train", batch_size=32):
        self.connection = MongoClient("localhost", 27017)
        if mode == "train":
            self.col = self.connection.dwh.train_lim
        else:
            self.col = self.connection.dwh.test_lim
        self.size = self.col.count() // batch_size
        self.batch_size = batch_size
        self.nb_classes = 1

    def __len__(self):
        return self.size

    def __getitem__(self, index):
        start_index = index * self.batch_size
        cursor = self.col.find()[start_index:start_index + self.batch_size]
        return self.__data_generation(cursor)

    def __del__(self):
        self.connection.close()

    def on_epoch_end(self):
        'Updates indexes after each epoch'
        # self.indexes = np.arange(len(self.list_IDs))
        # if self.shuffle == True:
        #    np.random.shuffle(self.indexes)

    def __data_generation(self, cursor):
        x1 = np.empty((self.batch_size, 12, 1))
        x2 = np.empty((self.batch_size, 7))
        y = np.empty((self.batch_size, self.nb_classes))

        for i, doc in enumerate(cursor):
            x = _pickle.loads(doc["x"])
            x1[i, ] = np.reshape(x[:12], (-1, 1))
            x2[i, ] = x[12:]
            y[i, ] = doc["y_std"]

        return [x1, x2], y


if __name__ == "__main__":
    generator = DataGenerator(batch_size=1)
    print(len(generator))
    # for x in generator:
    #    print(x)
