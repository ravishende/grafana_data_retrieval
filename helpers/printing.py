from termcolor import colored

# add enough padding to a string to center it within its border
def pad_str(string, border_len):
    padding_len = (border_len - len(string)) // 2
    padded_str = " "*padding_len + string
    return padded_str

# Printing Functions
def print_heading(heading):
    border_len = 100
    padded_heading = pad_str(heading, border_len)
    print("\n\n\n\n" + "*"*border_len)
    print(colored(padded_heading, "magenta"))
    print("*" * border_len)


def print_title(title):
    border_len = 75
    padded_title = pad_str(title, border_len)
    print("\n\n" + "-" * border_len)
    print(colored(padded_title, "green"))
    print("-" * border_len, "\n")


def print_sub_title(sub_title):
    border_len = 30
    paded_subtitle = pad_str(sub_title, border_len)
    print("\n\n" + '=' * border_len)
    print(colored(paded_subtitle, "blue"))
    print('=' * border_len)


# for a given dictionary in the form {titles:dataframes}
# print the title and dataframe of each item in the dict
def print_dataframe_dict(dictionary):
    for title, dataframe in dictionary.items():
        print_title(title)
        # if the dataframe is empty, print "No Data"
        if dataframe is None or len(dataframe.index) == 0:
            print(colored("No Data", "red"))
        else:
            print(dataframe)
        print("\n\n")