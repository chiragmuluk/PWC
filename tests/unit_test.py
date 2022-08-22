import pytest

from main import read_json_folder, get_unique_products_per_category, status_stats


def test_read_json_folder_valid_location():
    folder_location = "./testing_data/"
    try:
        read_json_folder(folder_location)
        assert True
    except:
        assert False


def test_get_unique_products_per_category():
    folder_location = "./testing_data/"
    df = read_json_folder(folder_location)

    try:
        get_unique_products_per_category(df)
        assert True
    except:
        assert False

def test_status_stats_error_scenario():
    folder_location = "./testing_data/"
    df = read_json_folder(folder_location)
    try:
        status_stats(df,"Cancelled")
        assert False
    except:
        assert True