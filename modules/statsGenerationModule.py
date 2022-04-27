import pandas as pd
import numpy as np
import dask.dataframe as dd  # Dask Multiprocessing
from itertools import combinations
import math

"""# Stats Generation and Analysis Module

"""


def segment_name(row, columns):
    values = [column + ":" + str(row[column]) for column in columns]
    return ','.join(values)


def getCombinations(columns):
    n = len(columns)
    res = [list(com) for sub in range(n) for com in combinations(columns, sub + 1)]
    return res


def getChangePercentage(today, yesterday):
    if (today == yesterday):  # handles if both values are 0
        return 0
    elif (yesterday == 0):
        return 100
    elif (math.isnan(today)):
        return -100
    elif (math.isnan(yesterday)):
        return 100
    else:
        return (today - yesterday) * 100 / yesterday


def get_analysis(df, metric, segment_columns, metric_column_change_percentage, numerator_column, denominator_column):
    """
    Gets Segments on segment_columns and calculates stats(value and percentage)
    on the given metric and returns an standard dataframe

    Arguments:
    df: input data frame
    metric: "total"/"average"
    segment_columns: "column header on which segments to be generated"
    numerator_column: numerator column which has today's value
    denominator_column: denominator column which has today's value
    """

    # get all posible combinations of given segment categories
    all_segments = getCombinations(segment_columns)

    combined_df = pd.DataFrame()
    for segment in all_segments:
        current_segment = df[df[numerator_column] > 0]
        old_segment = df[df[denominator_column] > 0]

        #  get metric for current and old column
        if (metric == "total"):
            current_segment = current_segment.groupby(segment)[numerator_column].sum().reset_index()
            old_segment = old_segment.groupby(segment)[denominator_column].sum().reset_index()
        elif (metric == "average"):
            current_segment = current_segment.groupby(segment)[numerator_column].mean().reset_index()
            old_segment = old_segment.groupby(segment)[denominator_column].mean().reset_index()

        # joining on segment
        segment_df = pd.merge(current_segment, old_segment, how="outer", on=segment)

        # get percentage
        segment_df[metric_column_change_percentage] = segment_df.apply(
            lambda x: getChangePercentage(x[numerator_column], x[denominator_column]), axis=1)
        segment_df["segment_name"] = segment_df.apply(lambda x: segment_name(x, segment), axis=1)

        # After outer join if NaN value is present for today it means it's 100% drop
        # NaN value for yesterday means it is 100 increase
        segment_df[numerator_column] = segment_df[numerator_column].replace(np.nan, -100)
        segment_df[denominator_column] = segment_df[denominator_column].replace(np.nan, 100)
        # selecting columns to maintain standard format
        segment_df = segment_df[['segment_name', numerator_column, metric_column_change_percentage]]
        combined_df = pd.concat([combined_df, segment_df])

    # sorting by segment_name to group segments together
    combined_df = combined_df.sort_values(by=['segment_name']).reset_index(drop=True)
    return combined_df


def get_overallStats(df, metric, numerator_column, denominator_column):
    """
    Returns overall absolute change, overall absolute change percentage
    and overall_purchase today for the given metric without segmenting
    """
    if (metric == "total"):
        overall_purchase_yesterday = df[denominator_column].sum()
        overall_purchase_today = df[numerator_column].sum()
    elif (metric == "average"):
        overall_purchase_yesterday = df[denominator_column].replace(0, np.nan).mean(skipna=True)
        overall_purchase_today = df[numerator_column].replace(0, np.nan).mean(skipna=True)

    if (math.isnan(overall_purchase_today)):
        overall_purchase_today = 0
    if (math.isnan(overall_purchase_yesterday)):
        overall_purchase_yesterday = 0

    overall_absolute_change = overall_purchase_today - overall_purchase_yesterday
    overall_change_percentage = getChangePercentage(overall_purchase_today, overall_purchase_yesterday)
    return overall_purchase_today, overall_absolute_change, overall_change_percentage