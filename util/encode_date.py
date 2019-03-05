import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# https://ianlondon.github.io/blog/encoding-cyclical-features-24hour-time/

x = ["Jan.", "Feb.", "März", "April", "Mai", "Juni", "Juli", "Aug.", "Sep.", "Okt.", "Nov.", "Dez."]
y = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

plt.scatter(x, y)
plt.show()

df = pd.read_csv("data/D_Datum.csv", sep=";")

# df["sin_week_day"] = np.sin(2 * np.pi * df["Tag in der Woche (#)"] / df["Tag in der Woche (#)"].max())
df["sin_month"] = np.sin(2 * np.pi * df["Monat (#)"] / df["Monat (#)"].max())
df["cos_month"] = np.cos(2 * np.pi * df["Monat (#)"] / df["Monat (#)"].max())


# fig = plt.figure(figsize=(100, 2))
df.sin_month.plot()

plt.show()

df.sin_month.plot()
df.cos_month.plot()
plt.show()

df.plot.scatter('sin_month', 'cos_month').set_aspect('equal')
plt.show()

df["sin_week_day"] = np.sin(2 * np.pi * df["Tag in der Woche (#)"] / df["Tag in der Woche (#)"].max())
df["cos_week_day"] = np.cos(2 * np.pi * df["Tag in der Woche (#)"] / df["Tag in der Woche (#)"].max())

df.plot.scatter('sin_week_day', 'cos_week_day').set_aspect('equal')
plt.show()

x.append("Jan. ")
y = np.sin(2 * np.pi * np.arange(1, 14, 1) / 12)
plt.scatter(x, y)
y = np.cos(2 * np.pi * np.arange(1, 14, 1) / 12)
plt.scatter(x, y)
plt.show()
