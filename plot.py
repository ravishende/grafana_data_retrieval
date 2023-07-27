import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime


# any python lists or numpy arrays will do, make sure they have same element count though
x_data = [datetime(month=2, year=2023, day=4),datetime(month=2, year=2023, day=23), datetime(month=2, year=2023, day=26)]
y_data = [1,3,4]

plt.title("CPU Usage vs Time")
plt.xlabel("Time")
plt.ylabel("CPU Usage")
plt.plot(x_data,y_data)
plt.scatter(x_data,y_data,s=30,alpha=1)
plt.show()

