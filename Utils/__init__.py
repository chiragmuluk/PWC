import logging
from datetime import datetime
from logging import config
import os
import time
import pandas as pd
import pytz
import yaml
import glob


def read_Config_File(yaml_config_path) -> object:
    """
        To read yaml file and convert it into Json format
        :param yaml_config_path: path where config file is placed
        :return: yaml file in json format
    """
    with open(yaml_config_path) as fh:
        yaml_data = yaml.load(fh, Loader=yaml.FullLoader)
    return yaml_data


def load_discounts(excel_path):
    xls = pd.ExcelFile(excel_path)

    workbook = pd.read_excel(xls, 'Sheet')
    return dict(workbook.values)


try:
    timeUTC = datetime.now()

    timezoneLocal = pytz.timezone('Europe/London')
    utc = pytz.utc
    timeLocal = utc.localize(timeUTC).astimezone(timezoneLocal)

    yaml_data = read_Config_File('Utils/config.yaml')

    logs_folder = yaml_data['logs_folder']

    # location where logs are saved
    logger_file_path = logs_folder + "file.log"

    # Number of historical logs file saved
    restore_logs = yaml_data['restorelogfile']

    # old logs files greater than restore_logs variable are deleted
    if os.path.exists(logger_file_path):

        log_file_created_date = logs_folder + (
            time.strftime("%d%m%Y%H%M%S", time.localtime(os.path.getmtime(logger_file_path)))) + ".log"
        os.rename(logger_file_path, log_file_created_date)
        old_logs = (sorted(glob.glob(logs_folder + "*.log"), key=os.path.getmtime)[:-restore_logs])
        for log in old_logs:
            os.remove(log)

    # logger is set from log config file
    config.fileConfig(yaml_data['loggerconfig'])

    # loading discount excel file
    discount_excel = load_discounts(yaml_data['discount_excel'])
    status_stats=yaml_data['status_stats']
    transactional_data=yaml_data['transactional_folder']
    discounting_scheme=yaml_data['discounting_scheme']
    product_line_sales_trend=yaml_data['product_line_sales_trend']

except Exception as e:
    logging.error("Something went wrong {}".format(str(e)))
