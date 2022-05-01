import pandas as pd
import numpy as np
import dask.dataframe as dd  # Dask Multiprocessing
from itertools import combinations
import math

from modules.readingModule import *
from modules.statsGenerationModule import *
from modules.humanReadableModule import *

"""# Driver Module"""


def analyse_and_print_observations(df, metric, SEGMENT_CATEGORIES, NUMERATOR_COLUMN, DENOMINATOR_COLUMN,
                                   NUMERATOR_COLUMN_PERCENTAGE, FILTER_THRESHOLD):
    """
    Driver function to trigger analysis on given metric and print results
    Arguments:
    metric: "total" or "average"
    SEGMENT_CATEGORIES: "column header on which segments to be generated"\
    NUMERATOR_COLUMN: "numerator column header which has today's purchase"
    DENOMINATOR_COLUMN: "denominator column which holds value for last day"
    FILTER_THRESHOLD: "threshold percentage for which we show observations"
    """

    # Getting Analysis on segment level and then on overall level
    print("analysing for %s metric" % metric)
    metric_analysis_df = get_analysis(df, metric, SEGMENT_CATEGORIES, NUMERATOR_COLUMN_PERCENTAGE, NUMERATOR_COLUMN,
                                      DENOMINATOR_COLUMN)
    overall_absolute_value, overall_absolute_change, overall_change_percentage = get_overallStats(df, metric,
                                                                                                  NUMERATOR_COLUMN,
                                                                                                  DENOMINATOR_COLUMN)

    ## Creatig comparision columns for filtering
    metric_analysis_df["value_change_wrt_overall"] = ((metric_analysis_df[NUMERATOR_COLUMN] - metric_analysis_df[
        DENOMINATOR_COLUMN]) * 100) / overall_absolute_change
    metric_analysis_df["value_change_wrt_overall"] = metric_analysis_df["value_change_wrt_overall"].abs()
    metric_analysis_df["percentage_change_wrt_overall"] = (metric_analysis_df[
                                                               NUMERATOR_COLUMN_PERCENTAGE] - overall_change_percentage)
    metric_analysis_df["percentage_change_wrt_overall"] = metric_analysis_df["percentage_change_wrt_overall"].abs()

    # filtering based on FILTER_THRESHOLD
    metric_analysis_df = metric_analysis_df[(metric_analysis_df['value_change_wrt_overall'] > FILTER_THRESHOLD) & (
            metric_analysis_df['percentage_change_wrt_overall'] > FILTER_THRESHOLD)]

    # getting results in Human readable string and printing them
    final_df = metric_analysis_df
    final_df['human_readable_observation'] = final_df.apply(
        lambda x: humanReadableString(x['segment_name'], x[NUMERATOR_COLUMN], x[NUMERATOR_COLUMN_PERCENTAGE]), axis=1)

    if (overall_change_percentage >= 0):
        print(metric.upper() + " purchases %d (Up %.2f%% from yesterday)" % (
            overall_absolute_value, overall_change_percentage))
    else:
        print(metric.upper() + " purchases %d (Down %.2f%% from yesterday)" % (
            overall_absolute_value, overall_change_percentage))

    results = final_df['human_readable_observation'].tolist()

    for result in results:
        print(result)
    return final_df


if __name__ == '__main__':

    # Location of csv file
    FILEPATH = "comparative.ai_take_home_test.csv"

    # Configuration of parameters
    FILTER_THRESHOLD = 2
    SEGMENT_CATEGORIES = ["country", "is_vip"]
    IGNORE_USER_TYPES = ["non_paying", "abstained_yesterday", "abstained_today"]

    # Detailed Configuration of parameters
    use_parallel_processing = True
    num_partitions = 12
    NUMERATOR_COLUMN = 'purchased_amount_today'
    DENOMINATOR_COLUMN = 'purchased_amount_yesterday'
    NUMERATOR_COLUMN_PERCENTAGE = 'amount_change_percentage'

    df = getDf(FILEPATH)
    print("Read CSV")
    # Creation of user_type column to represent the type of user in terms of:
    # ["regular_customer","non_paying","abstained_yesterday","abstained_today"]
    if use_parallel_processing == True:
        partitionedDf = dd.from_pandas(df, npartitions=num_partitions)
        df['user_type'] = partitionedDf.map_partitions(
            lambda df: df.apply(lambda x: user_type(x[DENOMINATOR_COLUMN], x[NUMERATOR_COLUMN]),
                                axis=1)).compute(scheduler='processes')
    else:
        df['user_type'] = df.apply(lambda x: user_type(x[DENOMINATOR_COLUMN], x[NUMERATOR_COLUMN]),
                                   axis=1)

    results_df = analyse_and_print_observations(df, "total", SEGMENT_CATEGORIES, NUMERATOR_COLUMN, DENOMINATOR_COLUMN,
                                                NUMERATOR_COLUMN_PERCENTAGE, FILTER_THRESHOLD)
    print("\n***************************************\n")
    results_df = analyse_and_print_observations(df, "average", SEGMENT_CATEGORIES, NUMERATOR_COLUMN, DENOMINATOR_COLUMN,
                                                NUMERATOR_COLUMN_PERCENTAGE, FILTER_THRESHOLD)
