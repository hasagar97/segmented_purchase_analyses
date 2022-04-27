import pandas as pd
import numpy as np
import dask.dataframe as dd  # Dask Multiprocessing
from itertools import combinations
import math

"""# Human Readable String generation module"""


def map_category(key_value):
    """
    returns human readable segments string given segement_name in specific format
    example: "Users who are from australia and are VIP"
    """
    pair = key_value.split(":")
    c = ""
    if(pair[0] == "country"):
        c = "are from "+pair[1]
    else:
        if(pair[1] == "False"):
            c = "are not VIP"
        else:
            c = "are VIP"
    return c

def get_stats_string(amount,percentage):
    """
    returns human readable stats String given current amount and percentage
    example: "$2805633 (Down 212.58%  from yesterday)"
    """
    string = ': $%d' % amount
    if(percentage >= 0):
        string = string + " (Up %.2f%%  from yesterday)"%percentage
    else:
        string = string + " (Down %.2f%%  from yesterday)"%abs(percentage)
    return string

def humanReadableString(segment_name,current_amount,percentage_change):
    """
    returns Human readble string using:
    segment_name: segment's name in given format "country:australia,is_vip:True"
    Current_amount: Today's amount
    percentage_change: float number which represents change from yesterday
    Example output: "Users who are from australia and are VIP: $2805633 (Down 212.58%  from yesterday)"
    """
    categories = segment_name.split(",")
    final_string = "Users who "
    categories = [map_category(c) for c in categories]

    final_string = final_string + " and ".join(categories) + get_stats_string(current_amount,percentage_change)
    return final_string

# print(humanReadableString("country:australia,is_vip:True",2.805633e+06,0))