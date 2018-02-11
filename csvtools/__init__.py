# coding: utf-8

"""CSVtools - A set of utilitarian classes for csv files manipulation.

Classes defined here:
    * CsvMerger           : merge some csv files into a single one.
    * CsvSorter           : sort a csv file along one of its columns.
    * LargeCsvReader      : read a csv file by chunks.
    * LargeCsvTransformer : apply transformations to csv data by chunks.
    * DictCsvWriter       : store dynamically dict records to csv.
    * DataframeCsvWriter  : store dynamically pandas.DataFrame records to csv.
"""

from ._csv_merger import CsvMerger
from ._csv_reader import LargeCsvReader
from ._csv_sorter import CsvSorter
from ._csv_transformer import LargeCsvTransformer
from ._csv_writers import DictCsvWriter, DataframeCsvWriter


__version__ = '0.1'
