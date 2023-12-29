import pandas as pd
import shutil

# settings
read_file = "csv_files/summed.csv"
write_file = "csv_files/zeros.csv"

# display settings
pd.set_option("display.max_columns", None)
terminal_width = shutil.get_terminal_size().columns
pd.set_option('display.width', terminal_width)

summed = pd.read_csv(read_file, index_col=0)

zeros = summed[(summed['cpu_total'] == 0) | (summed['mem_total'] == 0)]
zeros = zeros.reset_index(drop=True)

nontrivial_zeros = zeros[zeros['runtime'] > 45]
nontrivial_zeros = nontrivial_zeros.reset_index(drop=True)

lowest_runtime = nontrivial_zeros['runtime'].min()
highest_runtime = nontrivial_zeros['runtime'].max()

print(zeros, "\n"*10)
print(nontrivial_zeros, "\n\nLowest runtime:", lowest_runtime, "seconds\nHighest runtime:", round(highest_runtime/3600, 1), "hours", "\n"*5)

zeros.to_csv(write_file)