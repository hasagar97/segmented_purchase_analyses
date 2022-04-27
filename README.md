# Readme




## how to use

1. Unzip and Install requirements
```
dask==2022.4.1
numpy==1.22.3
pandas==1.4.2
notebook==6.4.2
```


2. [option 1] Start Jupyter Notebook in the same folder

```bash
pip install -r requirements.txt
jupyter notebook
```
3. [option 2] run python file
```bash
python analyse_purchases.py
```

### Quick-Start

- populate `FILEPATH` with the relative or absolute location of csv and run the Cells

### Configuration of parameters

    FILTER_THRESHOLD: Populate this variable to filter relavant observations, defaulted to 2
    SEGMENT_CATEGORIES: Populate this list with the column headers of the csv file on which you want to create segments(refer to functionalities in next section for more details)
    




## Functionalities and Design Choices
- segement columns: Populate SEGMENT_CATEGORIES with the column names on which you want the segments to be created.
  - for example if SEGMENT_CATEGORIES = ["country"], then you will see observations like 
    - users from canada
    - users from korea
  - for example if  SEGMENT_CATEGORIES = ["country"], then you would see observation like 
    - users who are from canada
    - users who are from canada and are VIP
    - users who are from canada and are not VIP
- Segments are grouped, they will be ordered by the sequence of SEGMENT_CATEGORIES
  - for example if it is set to  ["country", "is_vip"] then it will be first grouped by country and then VIP status
    - For example all segments belonging to CANADA will occur together
      - users who are from canada
      - users who are from canada and are VIP
      - users who are from canada and are not VIP
<!-- - Ignoring Users: a function called user_type add a column called `user_type` which categorizes the users into 4 categories: ["regular_customer","non_paying","abstained_yesterday","abstained_today"]. A variable IGNORE_USER_TYPES is populated and all user types in this list would be excluded from the analysis. -->
- `getDf()` Have created this function so that if in future if we need to read from some other source such as mysql, we can just edit this function
- `metric` have tried to maintain this variable whenever possible so that in future in we have a new metric we can easily add more conditions

## Assumptions
- percentage calculation logic: The percentage is being calculated by comparing to yesterday
$$
 PercentChange = \frac{PurchaseToday - PurchaseYesterday}{PurchaseYesterday}\times 100
  $$
- if the amount for a segment was 0 yesterday and non zero today then that is considered as 100% increase
- if the amount for a segement was non zero yesterday and 0 today then that is considered as 100% drop
- Ignore users on each day who did not purchase any amount. An user can be included in yesterday's analysis and not be included in today's analysis
- User_ids are not repeated: if repeated we can first groupBy-Sum on user_id,country and is_vip and then start the whole process



## Future steps

- Reusability: add functions so that if business logic changes, the amount of changes we need to do is minimal
- Parallelization: Very similar to apache spark we can use dask in an asynchronus manner, so that it computes only when results are needed. The current parallelization is close to none