import pandas as pd
import numpy as np
import dask.dataframe as dd  # Dask Multiprocessing
from itertools import combinations

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

"""# Stats Generation and Analysis Module

"""

def segment_name(row, columns):
    values = [column + ":" + str(row[column]) for column in columns]
    return ','.join(values)


def getCombinations(columns):
    n = len(columns)
    res = [list(com) for sub in range(n) for com in combinations(columns, sub + 1)]
    return res


def get_analysis(df, metric, segment_columns, maintain_columns, numerator_column, denominator_column):
    """
    Gets Segments on segment_columns and calculates stats(value and percentage) 
    on the given metric and returns an standard dataframe
    
    Arguments:
    df: input data frame
    metric: "total"/"average"
    segment_columns: "column header on which segments to be generated"
    maintain_column: list of columns to be maintained after group by
    numerator_column: numerator column which has today's value
    denominator_column: denominator column which has today's value
    """

    # get all posible combinations of given segment categories
    all_segments = getCombinations(segment_columns)
    metric_column_denominator = denominator_column
    metric_column_change_percentage = numerator_column + "_percentage"

    combined_df = pd.DataFrame()
    for segment in all_segments:
        segment_df = df
        #  get metric 
        if(metric == "total"):
            segment_df = segment_df.groupby(segment)[
                [numerator_column, metric_column_denominator] + maintain_columns].sum().reset_index()
        elif(metric == "average"):
            segment_df = segment_df.groupby(segment)[
                [numerator_column, metric_column_denominator] + maintain_columns].mean().reset_index()

        # get percentage
        segment_df[metric_column_change_percentage] = segment_df.apply(
            lambda x: x[numerator_column] * 100 / x[metric_column_denominator], axis=1)
        segment_df["segment_name"] = segment_df.apply(lambda x: segment_name(x, segment), axis=1)

        # selecting columns to maintain standard format
        segment_df = segment_df[
            ['segment_name', numerator_column, metric_column_change_percentage] + maintain_columns]

        combined_df = pd.concat([combined_df,segment_df])

    # sorting by segment_name to group segments together
    combined_df = combined_df.sort_values(by=['segment_name']).reset_index(drop=True)
    return combined_df




def get_overallStats(df, current_column,numerator_column, denominator_column,metric):
    """
    Returns overall absolute change, overall absolute change percentage
    and overall_purchase today for the given metric without segmenting
    """
    if(metric == "total"):
        overall_absolute_change = df[numerator_column].sum()
        overall_purchase_yesterday = df[denominator_column].sum()
        overall_purchase_today = df[current_column].sum()
        overall_change_percentage = df[numerator_column].sum() / overall_purchase_yesterday * 100
    elif(metric == "average"):
        overall_absolute_change = df[numerator_column].mean()
        overall_purchase_yesterday = df[denominator_column].mean()
        overall_change_percentage = df[numerator_column].mean() / overall_purchase_yesterday * 100
        overall_purchase_today = df[current_column].mean()
    return overall_purchase_today,overall_absolute_change, overall_change_percentage

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

# print(humanReadableString("country:australia,is_vip:True",2.805633e+06,-212.579900))

"""# Driver Module"""

def analyse_and_print_observations(df,metric,SEGMENT_CATEGORIES,CURRENT_COLUMN,NUMERATOR_COLUMN,DENOMINATOR_COLUMN,NUMERATOR_COLUMN_PERCENTAGE,FILTER_THRESHOLD):
    """
    Driver function to trigger analysis on given metric and print results
    Arguments:
    metric: "total" or "average"
    SEGMENT_CATEGORIES: "column header on which segments to be generated"
    CURRENT_COLUMN: "represents column header for absolute value to be printed"
    NUMERATOR_COLUMN: "numerator column header which has delta change"
    DENOMINATOR_COLUMN: "denominator column which holds value for last day"
    FILTER_THRESHOLD: "threshold percentage for which we show observations"
    """
    
    # Getting Analysis on segment level and then on overall level
    print("analysing for %s metric" % metric)
    metric_analysis_df = get_analysis(df, metric, SEGMENT_CATEGORIES, [CURRENT_COLUMN], NUMERATOR_COLUMN,
                                      DENOMINATOR_COLUMN)
    overall_absolute_value, overall_absolute_change, overall_change_percentage = get_overallStats(df, CURRENT_COLUMN,NUMERATOR_COLUMN, DENOMINATOR_COLUMN,
                                                                          metric)

    ## Creatig comparision columns for filtering 
    metric_analysis_df["value_change_wrt_overall"] = (metric_analysis_df[
                                                          NUMERATOR_COLUMN] * 100) / overall_absolute_change
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
        lambda x: humanReadableString(x['segment_name'], x[CURRENT_COLUMN], x[NUMERATOR_COLUMN_PERCENTAGE]), axis=1)

    
    if (overall_change_percentage >= 0):
        print(metric.upper()+" purchases %d (Up %.2f%% from yesterday)" % (
        overall_absolute_value, overall_change_percentage))
    else:
        print(metric.upper()+" purchases %d (Down %.2f%% from yesterday)" % (
        overall_absolute_value, overall_change_percentage))
    results = final_df['human_readable_observation'].tolist()

    for result in results:
        print(result)

if __name__ == '__main__':

    # Location of csv file
    FILEPATH = "comparative.ai_take_home_test.csv"

    # Configuration of parameters
    FILTER_THRESHOLD = 2
    SEGMENT_CATEGORIES = ["country", "is_vip"]
    IGNORE_USER_TYPES = ["non_paying","abstained_yesterday","abstained_today"]

    # Detailed Configuration of parameters
    use_parallel_processing = True
    num_partitions = 12
    CURRENT_COLUMN = 'purchased_amount_today'
    NUMERATOR_COLUMN = 'amount_change'
    DENOMINATOR_COLUMN = 'purchased_amount_yesterday'
    NUMERATOR_COLUMN_PERCENTAGE = 'amount_change_percentage'
    

    df = getDf(FILEPATH)
    print("Read CSV")
    # Creation of user_type column to represent the type of user in terms of:
    # ["regular_customer","non_paying","abstained_yesterday","abstained_today"]
    if use_parallel_processing == True:
        partitionedDf = dd.from_pandas(df, npartitions=num_partitions)
        df['user_type'] = partitionedDf.map_partitions(
            lambda df: df.apply(lambda x: user_type(x[DENOMINATOR_COLUMN], x[CURRENT_COLUMN]),
                                axis=1)).compute(scheduler='processes')
    else:
        df['user_type'] = df.apply(lambda x: user_type(x[DENOMINATOR_COLUMN], x[CURRENT_COLUMN]),
                                   axis=1)

    # excluding users in IGNORE_USER_TYPES

    print("Ignoring Users: "+str(IGNORE_USER_TYPES))
    df = df[~df["user_type"].isin(IGNORE_USER_TYPES)]


    # Creating a change column which would be used throughout the code 
    # for calculating absolute change, absolute change percentage and other metrics
    if use_parallel_processing:
        partitionedDf = dd.from_pandas(df, npartitions=num_partitions)
        df['amount_change'] = partitionedDf.map_partitions(
            lambda df: df.apply(lambda x: amount_change(x[DENOMINATOR_COLUMN], x[CURRENT_COLUMN]),
                                axis=1)).compute(scheduler='processes')
    else:
        df['amount_change'] = df.apply(
            lambda x: amount_change(x[DENOMINATOR_COLUMN], x[CURRENT_COLUMN]), axis=1)

    analyse_and_print_observations(df,"total",SEGMENT_CATEGORIES,CURRENT_COLUMN,NUMERATOR_COLUMN,DENOMINATOR_COLUMN,NUMERATOR_COLUMN_PERCENTAGE,FILTER_THRESHOLD)
    print("\n***************************************\n")
    analyse_and_print_observations(df,"average",SEGMENT_CATEGORIES,CURRENT_COLUMN,NUMERATOR_COLUMN,DENOMINATOR_COLUMN,NUMERATOR_COLUMN_PERCENTAGE,FILTER_THRESHOLD)
