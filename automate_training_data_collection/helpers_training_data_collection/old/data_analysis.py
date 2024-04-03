import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import sys
import os
# get set up to be able to import helper files from parent directory (grafana_data_retrieval)
sys.path.append("../../grafana_data_retrieval")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
grandparent = os.path.dirname(parent)
sys.path.append(grandparent)
from helpers.time_functions import datetime_ify

# csv files
runs_csv = "csv_files/summed.csv"
zero_runs_csv = "csv_files/zeros.csv"
non_zero_runs_csv = "csv_files/non_zeros.csv"


# get csv files as dataframes
total_runs = pd.read_csv(runs_csv, index_col=0)
zero_runs = pd.read_csv(zero_runs_csv, index_col=0)
non_zero_runs = pd.read_csv(non_zero_runs_csv, index_col=0)


# ============================
#     plot distributions
# ============================

# Plotting the distribution of runtimes for zero and non-zero runs
plt.figure(figsize=(12, 6))
sns.histplot(zero_runs['runtime'], color="red", label='Zero Runs', kde=True)
sns.histplot(non_zero_runs['runtime'], color="blue", label='Non-Zero Runs', kde=True)
plt.title('Distribution of Runtimes for Zero vs Non-Zero Runs')
plt.xlabel('Runtime (seconds)')
plt.ylabel('Frequency')
plt.legend()
plt.show()

# Plotting the distribution of indices for non_zero runs
zero_mask = (total_runs['cpu_total'] == 0) | (total_runs['mem_total'] == 0)
zero_runs_indices = total_runs[zero_mask].index
non_zero_runs_indices = total_runs[~zero_mask].index
plt.figure(figsize=(12, 6))
sns.histplot(zero_runs_indices, color="red", label='Zero Runs')
sns.histplot(non_zero_runs_indices, color="blue", label='Non-Zero Runs')
plt.title('Distribution of Indices for Zero vs Non-Zero Runs')
plt.xlabel('Index')
plt.ylabel('Frequency')
plt.legend()
plt.show()


# get start times as seconds since epoch (01/01/1970)
zero_runs['start'] = zero_runs['start'].apply(datetime_ify)
zero_runs['start_seconds'] = zero_runs['start'].apply(lambda time: time.timestamp())
non_zero_runs['start'] = non_zero_runs['start'].apply(datetime_ify)
non_zero_runs['start_seconds'] = non_zero_runs['start'].apply(lambda time: time.timestamp())
# Plotting the distribution of start times for non_zero runs
plt.figure(figsize=(12, 6))
sns.histplot(zero_runs['start'], color="red", label='Zero Runs')
sns.histplot(non_zero_runs['start'], color="blue", label='Non-Zero Runs')
plt.title('Distribution of Start times for Zero vs Non-Zero Runs')
plt.xlabel('Start Time')
plt.ylabel('Frequency')
plt.legend()
plt.show()

# ===============================
#     plot totals vs runtime
# ===============================

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


# ======================
#     plot ratios
# ======================

# Calculating ratios for CPU and Memory usage vs duration for non-zero runs
non_zero_runs['cpu_t1_ratio'] = non_zero_runs['cpu_t1'] / non_zero_runs['duration_t1']
non_zero_runs['cpu_t2_ratio'] = non_zero_runs['cpu_t2'] / non_zero_runs['duration_t2']
non_zero_runs['cpu_total_ratio'] = non_zero_runs['cpu_total'] / non_zero_runs['runtime']
non_zero_runs['mem_t1_ratio'] = non_zero_runs['mem_t1'] / non_zero_runs['duration_t1']
non_zero_runs['mem_t2_ratio'] = non_zero_runs['mem_t2'] / non_zero_runs['duration_t2']
non_zero_runs['mem_total_ratio'] = non_zero_runs['mem_total'] / non_zero_runs['runtime']

# Removing any infinite or NaN values for accurate plotting
non_zero_runs_filtered = non_zero_runs.replace([np.inf, -np.inf], np.nan).dropna(subset=['cpu_t1_ratio', 'cpu_total_ratio'])


# ===============
#   ratios t1
# ===============

# Plotting cpu_t1/duration_t1 vs cpu_total/runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.regplot(x='cpu_t1_ratio', y='cpu_total_ratio', data=non_zero_runs_filtered)
plt.title('CPU Usage Ratio (cpu_t1/duration_t1) vs Total CPU Usage Ratio (cpu_total/runtime)')
plt.xlabel('CPU Usage Ratio (cpu_t1/duration_t1)')
plt.ylabel('Total CPU Usage Ratio (cpu_total/runtime)')
plt.show()


# Plotting mem_t1/duration_t1 vs cpu_total/runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='mem_t1_ratio', y='mem_total_ratio', data=non_zero_runs_filtered)
plt.title('Memory Usage Ratio (mem_t1/duration_t1) vs Total Memory Usage Ratio (mem_total/runtime)')
plt.xlabel('Memory Usage Ratio (mem_t1/duration_t1)')
plt.ylabel('Total Memory Usage Ratio (mem_total/runtime)')
plt.show()


# ===============
#   ratios t2
# ===============

# Plotting cpu_t2/duration_t2 vs cpu_total/runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.regplot(x='cpu_t2_ratio', y='cpu_total_ratio', data=non_zero_runs_filtered)
plt.title('CPU Usage Ratio (cpu_t2/duration_t2) vs Total CPU Usage Ratio (cpu_total/runtime)')
plt.xlabel('CPU Usage Ratio (cpu_t2/duration_t2)')
plt.ylabel('Total CPU Usage Ratio (cpu_total/runtime)')
plt.show()

# Plotting mem_t2/duration_t2 vs cpu_total/runtime for non-zero runs
plt.figure(figsize=(12, 6))
sns.scatterplot(x='mem_t2_ratio', y='mem_total_ratio', data=non_zero_runs_filtered)
plt.title('Memory Usage Ratio (mem_t2/duration_t2) vs Total Memory Usage Ratio (mem_total/runtime)')
plt.xlabel('Memory Usage Ratio (mem_t2/duration_t2)')
plt.ylabel('Total Memory Usage Ratio (mem_total/runtime)')
plt.show()
