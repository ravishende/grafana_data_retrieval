from termcolor import colored

# Printing Functions
def print_heading(heading):
    indented_heading = " "*15 + heading + ":"
    print("\n\n\n\n" + "*"*100)
    print(colored(indented_heading, "magenta"))
    print("*" * 100)


def print_title(title):
    print("\n\n" + "-"*100, "\n")
    print("            ", colored(title, "green"))
    print("-" * 100, "\n")


def print_sub_title(sub_title):
    print("\n\n" + '='*30)
    print(colored(sub_title, "blue"))
    print('='*30)


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