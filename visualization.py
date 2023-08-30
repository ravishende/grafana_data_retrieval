import pandas as pd
import seaborn as sns
import time
import matplotlib.pyplot as plt
from datetime import datetime as dt

df = pd.read_csv("cpu_usage.csv")

tick_spacing = 1

time_0 = df["Time"][0]
df["Time"] = df.apply(lambda row: row["Time"]-time_0,axis=1)
my_datetime = dt.fromtimestamp(time_0)
print(my_datetime)
s = my_datetime.strftime("%H:%M")

ax = sns.lineplot(data=df, x="Time", y="Rate of Transmitted Packets",hue="Pod")
# ax.set(xlim=0)
# labels = ["13:44", "2023-08-30 13:50:42.138000128", "2023-08-30 13:55:42.138000128"]
# ax.set(xticklabels=labels)
plt.title("Rate of Transmitted Packets")
plt.xlabel("Seconds Since "+s)
plt.xticks(rotation = 25)
plt.show()
