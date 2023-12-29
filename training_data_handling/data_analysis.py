import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# csv files
runs_csv = "csv_files/summed.csv"
zero_runs_csv = "csv_files/zeros.csv"
non_zero_runs_csv = "csv_files/summed_success.csv"

# get csv files as dataframes
runs = pd.read_csv(runs_csv, index_col=0)
zero_runs = pd.read_csv(zero_runs_csv, index_col=0)
non_zero_runs = pd.read_csv(non_zero_runs_csv, index_col=0)


# Plotting the distribution of runtimes for zero and non-zero runs
plt.figure(figsize=(12, 6))
sns.histplot(zero_runs['runtime'], color="red", label='Zero Runs', kde=True)
sns.histplot(non_zero_runs['runtime'], color="blue", label='Non-Zero Runs', kde=True)
plt.title('Distribution of Runtimes for Zero vs Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('Frequency')
plt.legend()
plt.show()

# Plotting CPU total vs Runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='runtime', y='cpu_total', data=non_zero_runs)
plt.title('CPU Total vs Runtime for Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('CPU Total')
plt.show()

# Plotting Memory total vs Runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='runtime', y='mem_total', data=non_zero_runs)
plt.title('Memory Total vs Runtime for Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('Memory Total')
plt.show()




# Replotting the distribution of runtimes for zero and non-zero runs
plt.figure(figsize=(12, 6))
sns.histplot(zero_runs['runtime'], color="red", label='Zero Runs', kde=True)
sns.histplot(non_zero_runs['runtime'], color="blue", label='Non-Zero Runs', kde=True)
plt.title('Distribution of Runtimes for Zero vs Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('Frequency')
plt.legend()
plt.show()

# Replotting CPU total vs Runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='runtime', y='cpu_total', data=non_zero_runs)
plt.title('CPU Total vs Runtime for Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('CPU Total')
plt.show()

# Replotting Memory total vs Runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='runtime', y='mem_total', data=non_zero_runs)
plt.title('Memory Total vs Runtime for Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('Memory Total')
plt.show()
