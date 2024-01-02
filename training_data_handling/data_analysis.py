import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# csv files
runs_csv = "csv_files/summed.csv"
zero_runs_csv = "csv_files/zeros.csv"
non_zero_runs_csv = "csv_files/summed.csv"

# get csv files as dataframes
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


# Calculating ratios for CPU usage and duration for non-zero runs
non_zero_runs['cpu_t1_ratio'] = non_zero_runs['cpu_t1'] / non_zero_runs['duration_t1']
non_zero_runs['cpu_total_ratio'] = non_zero_runs['cpu_total'] / non_zero_runs['runtime']

# Removing any infinite or NaN values for accurate plotting
non_zero_runs_filtered = non_zero_runs.replace([np.inf, -np.inf], np.nan).dropna(subset=['cpu_t1_ratio', 'cpu_total_ratio'])

# Plotting cpu_t1/duration_t1 vs cpu_total/runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='cpu_t1_ratio', y='cpu_total_ratio', data=non_zero_runs_filtered)
plt.title('CPU Usage Ratio (cpu_t1/duration_t1) vs Total CPU Usage Ratio (cpu_total/runtime)')
plt.xlabel('CPU Usage Ratio (cpu_t1/duration_t1)')
plt.ylabel('Total CPU Usage Ratio (cpu_total/runtime)')
plt.show()
