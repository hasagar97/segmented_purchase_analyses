import pandas as pd
import numpy as np
import dask.dataframe as dd  # Dask Multiprocessing
from itertools import combinations
import math

"""# Reader and Filter Module"""

def getDf(filename):
    """
    function to return data frame from csv,
    can be modified to read from mysql
    """
    df = pd.read_csv(filename)
    return df


def user_type(yesterday, today):
    """
    function to find out the types of users
    """
    if (yesterday == 0 and today == 0):
        return "non_paying"
    if (yesterday == 0 and today != 0):
        return "abstained_yesterday"
    if (yesterday != 0 and today == 0):
        return "abstained_today"
    if (yesterday != 0 and today != 0):
        return "regular_user"


def amount_change(yesterday, today):
    return today - yesterday

