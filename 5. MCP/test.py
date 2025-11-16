import csv_handler
from csv_handler import CSVHandler
import data_quality
from data_quality import DataQualityChecker

# testing the CSV Handler class

file = CSVHandler('test.csv')
df = file.read_csv()
print(df.head())
print(df.info())

file_modified_time = file.get_last_modified_time()
print('The file modified time is: ', file_modified_time)

file_stats = file.get_file_stats()
print('The file stats are: ', file_stats)

# testing the DataQualityChecker class

threshold_settings = {'max_nulls_percentage':0.02, "max_dupes_percentage": 0.05}
dq_checker = DataQualityChecker(df, threshold_settings)
print('null check results are: ', dq_checker.check_nulls())
print('duplicates are: ', dq_checker.check_duplicates())