import Utils

import pandas as pd
import glob
import json
import logging


def read_json_folder(json_folder: str):
    """
    Reads Json file from given path and converts into one single pandas dataframe
    :param json_folder: path of folder consisting json files that need to be process and
                        converted in to dataframe
    :return: single dataframe with all json data
    """
    logging.info("Reading JSON files from {}".format(json_folder))
    jsonFiles = glob.glob("{}*.json".format(json_folder))
    transactional_df = []
    try:
        for tf in jsonFiles:
            with open(tf, 'r') as f:
                data = json.loads(f.read())
                transactional_df.append(
                    pd.json_normalize(data, record_path=['attributes'], meta=['ORDERNUMBER', 'PRODUCTCODE']))
        return pd.concat(transactional_df)
    except ValueError as v:
        logging.error("No json files were found on given location {}".format(json_folder))
        raise ValueError


def to_date(date_column, f):
    """
    Convert pandas column (string) into date type

    :param date_column: Column that need to be converted
    :param f: format of date
    :return: column with date data type
    """
    logging.info("Converting data type of date")
    try:
        return pd.to_datetime(date_column, format=f)
    except TypeError as t:
        logging.error("Date format of json data doesnt match with {}".format(f))
        raise TypeError


def split_date(df_column):
    """

    Split date column into year,month and day

    :param df_column: column of pandas dataframe have date as dataype
    :return: Year,Month and Day for date
    """
    logging.info("Splitting date into Year,Month and day")
    return (df_column.apply(
        lambda x: x.year), df_column.apply(lambda x: x.month), df_column.apply(lambda x: x.day))


def status_stats(df, status):
    """


    Print statistics of the data for given status and also segmented by year

    :param df: dataframe which consist of transactional data
    :param status: For which stats need to generated
    """
    try:
        df_c = df.query('STATUS=="{}"'.format(status))
        print("Total Number of {} orders : {}".format(status, df_c['ORDERNUMBER'].count()))
        print("Year wise {} orders :".format(status))
        print(df_c.groupby(['YEAR'])['ORDERNUMBER'].count().reset_index(name="TOTAL").to_string())
        print()
    except KeyError as k:
        raise k


def get_unique_products_per_category(df):
    """

    Gives count of unique products based on product category/ product line

    :param df: dataframe which consist of transactional data
    :return: count of unique products based on product category/ product line
    """
    try:
        return df.groupby(['PRODUCTLINE'])['PRODUCTCODE'].count().reset_index(name="TOTAL").to_string()
    except KeyError as k:
        raise k


def get_sales_trend(df, product_line, status):
    """
    Generate sale trend across entire data for given product and status

    :param df: dataframe which consist of transactional data
    :param product_line: Product category for which stats need to be generated
    :param status: Status for which stats need to be generated
    :return: dataframe stats segmented by year,month and status
    """
    try:
        logging.info("Generating sales trend for {} having status {}".format(product_line, status))
        df_g = df.query('PRODUCTLINE=="{}" and STATUS=="{}"'.format(product_line, status))
        return df_g.groupby(['YEAR', 'MONTH', 'STATUS'])['ORDERNUMBER'].count()
    except KeyError as k:
        raise k


def calculate_discounts(df):
    """
    Calculates volume based discount based on given values

    :param df: dataframe which consist of transactional data
    :return: Created new column named as Discount with discount rate
    """
    try:
        logging.info("Calculating volume based discount")
        mask = df['PRODUCTLINE'].isin(Utils.discounting_scheme)

        df['DISCOUNT'] = df[mask].apply(lambda row: row['MSRP'] * get_discounts(row['QUANTITYORDERED']), axis=1)
        df['DISCOUNT'] = df['DISCOUNT'].fillna(0)

        return df
    except KeyError as k:
        raise k


def get_discounts(quantity):
    """

    % of discount need to applied as per quantity condition given in excel sheet

    :param quantity: Total No of product items for which order is generated
    :return: % of discount need to applied based on quantiy orderd
    """
    for i, j in Utils.discount_excel.items():
        values = i.split("-")
        try:
            upper_bound = int(values[1])
            lower_bound = int(values[0])
            if lower_bound <= quantity < upper_bound:
                return (j)

        except:
            return (j)


def run_transformations(transaction_location: str):
    """
    Generate Stats & also runs transformation of data in parquet format
    :param transaction_location:
    :return: Prints all the required status
    """
    try:
        transactionDF = read_json_folder(transaction_location)

        logging.info("Json Data converted to pandas Dataframe successfully")
        transactionDF.reset_index(drop=True, inplace=True)

        transactionDF['ORDERDATE'] = to_date(transactionDF['ORDERDATE'], f="%m/%d/%Y %H:%M")

        logging.info("Data type for date converted successfully")

        transactionDF['YEAR'], transactionDF['MONTH'], transactionDF['DAY'] = split_date(transactionDF['ORDERDATE'])

        # Storing data in Parquet format
        transactionDF.to_parquet(path='transactional_parquet/{}'.format(Utils.timeLocal.strftime("%Y%m%d_%H%M%S/")),
                                 partition_cols=['YEAR', 'MONTH', 'DAY'], compression='gzip')

        logging.info("Data saved in Parquet format successfully")

        # Getting count of particular status in total and year wise
        for i in Utils.status_stats:
            logging.info("Generating stats for {} status".format(i))
            status_stats(transactionDF, i)
            logging.info("Stats for {} status completed".format(i))

        # Getting count of unique products per product line
        logging.info("Generating Count of Unique product per product line")
        print("Count of Unique product per product line")
        print(get_unique_products_per_category(transactionDF))

        # Getting sales trend of particular product line with status (year and month wise)
        for product_line, status in Utils.product_line_sales_trend.items():
            print(("Sales trend for {} having status {}".format(product_line, status)))
            print(get_sales_trend(transactionDF, product_line, status))
            logging.info("Sales trend completed for {} having status {}".format(product_line, status))

        # Calculating discount
        print("Calculated volume-based discount")
        print(calculate_discounts(transactionDF))
        logging.info("Completed")

    except KeyError as k:
        logging.error("Column doesnt exist in dataframe : {}".format(str(k)))

    except Exception as e:
        logging.error("Something went wrong {}".format(str(e)))


if __name__ == "__main__":
    run_transformations(Utils.transactional_data)

