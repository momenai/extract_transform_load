# Online Retailer: Extracting, Transforming, and
The code is the implementation of extract, transform, and load (ETL) process to prepare sales data from an online storefront for later analysis and modeling.

## Extract
Data are stored in sql databese as tables. In this phase, I have connected to db and get all tables and save tables as pandas dataframes 

## Transform
Aggregate the online_retail_history and stock_description datasets. In this phase, I have written a query to left join both tables so that the stock descriptions are in the same table as
every other column from online_retail_history. Then,  I identify and fix corrupt or unusable data by writing code to get the count of all
distinct values in the Description field and eliminate  descriptions records if it is just question mark (?). After that, I remove duplicates values and Correct date formats to be datetime fromat. 

## Load
I have loaded the dataset into a pickle file, by writing code to save the cleaned DataFrame into a pickle file and save it in the same directory. 
