import keras.backend as K
import tensorflow as tf

from keras import Sequential, Input, Model
from keras.callbacks import ModelCheckpoint, EarlyStopping, TensorBoard
from keras.layers import Dense, Dropout, Embedding, LSTM, Bidirectional, Concatenate

from data_generator import DataGenerator
from datetime import datetime

from pymongo import MongoClient


def smooth_l1(y_true, y_pred, huber_delta=0.5):
    x = K.abs(y_true - y_pred)
    x = tf.where(x < huber_delta, 0.5 * x ** 2, huber_delta * (x - 0.5 * huber_delta))
    return K.sum(x)


def huber_loss(y_true, y_pred):
    return tf.losses.huber_loss(y_true, y_pred)


def get_model(dropout=0.5):
    seq_input = Input((12, 1,))
    vector_input = Input((7,))

    lstm = Bidirectional(LSTM(64, return_sequences=True))(seq_input)
    lstm2 = Bidirectional(LSTM(32))(lstm)

    dense = Dense(64, activation="relu")(vector_input)

    concat = Concatenate()([lstm2, dense])

    dense2 = Dense(128, activation="relu")(concat)
    dropout1 = Dropout(dropout)(dense2)
    dense3 = Dense(64, activation="relu")(dropout1)
    dropout2 = Dropout(dropout)(dense3)
    dense4 = Dense(32, activation="relu")(dropout2)
    dropout3 = Dropout(dropout)(dense4)

    output = Dense(1, activation="linear")(dropout3)

    return Model(inputs=[seq_input, vector_input], outputs=output)


if __name__ == "__main__":

    model = get_model()
    batch_size=32

    # loss="logcosh"
    model.compile(loss=huber_loss, optimizer='adam', metrics=['mse', 'mae'])

    train_generator = DataGenerator(batch_size=batch_size)
    test_generator = DataGenerator(batch_size=batch_size, mode="test")

    filepath = "models_debug/weights-improvement-{epoch:02d}.hdf5"
    checkpoint = ModelCheckpoint(filepath, verbose=1, save_best_only=True)
    early_stopper = EarlyStopping(monitor="val_mean_absolute_error", patience=3)
    tensorboard = TensorBoard(log_dir='./graph', histogram_freq=0, write_graph=True, write_images=True)
    # tensorboard.set_model(model2)
    
    # model2.load_weights("models/weights-improvement-28-26781.hdf5")

    history = model.fit_generator(generator=train_generator,
                                  validation_data=test_generator,
                                  steps_per_epoch=len(train_generator),
                                  validation_steps=len(test_generator),
                                  workers=10,
                                  max_queue_size=10,
                                  epochs=100000,
                                  callbacks=[checkpoint, early_stopper, tensorboard])

    model.save_weights('dwh_nn.hdf5')

    document = {"model2": {}, "date": datetime.now()}
    for layer in model.layers:
        document["model2"][layer.name] = layer.rate if "dropout" in layer.name else layer.output_shape[1]

    client = MongoClient("localhost", 27017)
    col = client.chess.models
    document["history"] = history.history
    col.insert_one(document)

