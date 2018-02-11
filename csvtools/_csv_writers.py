# coding: utf-8

"""Classes to handle record-type specific dynamic csv storage."""

from functools import reduce

import pandas as pd
from yaptools import check_type_validity

from csvtools._csv_writer import (
    AbstractCsvWriter, CSV_WRITER_DOCSTRING, CSV_WRITER_EXAMPLE
)


class DictCsvWriter(AbstractCsvWriter):
    """Class to handle dynamic csv storage of dict records.
    {0}

    Usage:

    # Writer instanciation.
    >>> writer = DictCsvWriter('file.csv', buffer_size=100, sep=',')
    {1}
    """
    __doc__ = __doc__.format(CSV_WRITER_DOCSTRING, CSV_WRITER_EXAMPLE)

    def _reset_buffer(self):
        """Reset the buffer to its empty state."""
        self.buffer = []

    def _add_to_buffer(self, record):
        """Bufferize a given dict."""
        if not isinstance(record, dict):
            self.log(
                'Rejected a record: invalid type %s.' % type(record),
                level='error'
            )
            return None
        self.buffer.append(record)

    def _get_buffer_columns(self):
        """Return a list of unique column names appearing in the buffer."""
        return reduce(
            lambda x, y: list(set(x + y)),
            map(lambda dict_x: list(dict_x.keys()), self.buffer)
        )

    def _to_csv(self, first_time):
        """Write buffered elements to csv."""
        def clean_row(string, replace):
            """Clean a record row."""
            string = string.replace(self.sep, replace.get(self.sep, '§'))
            return string.replace('\n', '')
        replace = {';': '.,', '§': ';'}
        rows = (
            self.sep.join(
                clean_row(str(row.get(column, '')), replace)
                for column in self.header
            )
            for row in self.buffer
        )
        with open(self.path, 'a', encoding='utf-8') as csv_file:
            if first_time:
                csv_file.write(self.sep.join(self.header) + '\n')
            for row in rows:
                csv_file.write(row + '\n')


class DataframeCsvWriter(AbstractCsvWriter):
    """Class to handle dynamic csv storage of pandas.DataFrame records.
    {0}

    Usage:

    # Writer instanciation.
    >>> writer = DataframeCsvWriter('file.csv', buffer_size=100, sep=',')
    {1}
    """
    __doc__ = __doc__.format(CSV_WRITER_DOCSTRING, CSV_WRITER_EXAMPLE)

    def __init__(
            self, path, buffer_size, sep=';', logger=None, write_index=False
        ):
        """Set up the handler's initial state.

        path        : path to the destination csv file, which may pre-exist
        buffer_size : maximum number of rows to keep in memory before writing
                      them to the csv file (positive integer)
        sep         : values separator of the csv file (str, default ';')
        logger      : optional Logger object to use instead of the default
                      one (which logs everything to the console)
        write_index : whether to write down the records' index as first column
                      (bool, default False) ; note that index will be written
                      if using a pre-existing file whose first column is not
                      named
        """
        check_type_validity(write_index, bool, 'write_index')
        self._write_index = write_index
        super().__init__(path, buffer_size, sep, logger)

    def _get_current_csv_header(self):
        """Read the csv file's initial header, if any."""
        header = super()._get_current_csv_header()
        if header and header[0] == '':
            self.write_index = True
            del header[0]
        return header

    def _reset_buffer(self):
        """Reset the buffer to its empty state."""
        self.buffer = pd.DataFrame()

    def _add_to_buffer(self, record):
        """Bufferize a given pandas.DataFrame."""
        if isinstance(record, pd.Series):
            record = pd.DataFrame(record)
        elif not isinstance(record, pd.DataFrame):
            self.log(
                'Rejected a record: invalid type %s.' % type(record),
                level='error'
            )
            return None
        self.buffer = pd.concat([self.buffer, record])

    def _get_buffer_columns(self):
        """Return a list of unique column names appearing in the buffer."""
        return list(self.buffer.columns)

    def _to_csv(self, first_time):
        """Write buffered elements to csv."""
        if self.buffer.columns.tolist() != self.header:
            for column in self.header:
                if column not in self.buffer.columns:
                    self.buffer[column] = None
            self.buffer = self.buffer[self.header]
        self.buffer.to_csv(
            self.path, sep=self.sep, mode='a', index=self._write_index,
            header=self.header if first_time else None, encoding='utf-8'
        )

    def _update_csv_header(self):
        """Update the csv file's header."""
        if self._write_index:
            self.header = [''] + self.header
        super()._update_csv_header()
