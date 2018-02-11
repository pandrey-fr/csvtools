# coding: utf-8

"""Class to read a large csv file by chunks."""

import os

import pandas as pd
from yaptools import check_type_validity, lazyproperty
from yaptools.logger import LoggedObject, loggedmethod


class LargeCsvReader(LoggedObject):
    """Class to read a large csv file by chunks.

    This class basically wraps 'pandas.read_csv', adding some reading
    options and allowing to read a single file multiple times without
    specifying all parameters again. It also improves columns naming
    when skipping rows, provides with the total file's size, etc.
    """

    def __init__(self, filepath, chunksize=10000, logger=None, **kwargs):
        """Initialize the csv reader.

        filepath  : path to a csv file
        chunksize : number of rows to fetch at once (int, default 10000)
        logger    : optional Logger object to use instead of the default
                    one (which logs everything to the console)

        Additionally, any valid keyword arguments for csv reading using
        the 'pandas.read_csv' function may be passed.
        """
        # Check provided path validity.
        check_type_validity(filepath, str, 'filepath')
        if not os.path.exists(filepath):
            raise FileNotFoundError('Cannot find "%s".' % filepath)
        is_csv = (
            os.path.isfile(filepath)
            and filepath.endswith(os.path.extsep + 'csv')
        )
        if not is_csv:
            raise ValueError('The provided path does not lead to a csv file.')
        self.filepath = os.path.abspath(filepath)
        # Check other arguments' validity.
        check_type_validity(chunksize, int, 'chunksize')
        if chunksize <= 0:
            raise ValueError('Negative chunksize value.')
        self.chunksize = chunksize
        if 'filepath_or_buffer' in kwargs.keys():
            raise KeyError("Forbidden keyword argument: 'filepath_or_buffer'.")
        self.kwargs = kwargs
        self.kwargs['encoding'] = self.kwargs.get('encoding', 'utf-8')
        super().__init__(logger)

    def __len__(self):
        """Return the csv file's number of rows."""
        return self._len

    @lazyproperty
    def _len(self):
        """Return the csv file's number of rows."""
        with open(self.filepath, encoding=self.kwargs['encoding']) as csv_file:
            nrows = sum(1 for _ in csv_file)
        return nrows - (self.kwargs.get('header', '') is not None)

    @lazyproperty
    def columns(self):
        """Return the csv file's columns."""
        if 'names' in self.kwargs.keys():
            return self.kwargs['names']
        kwargs = self.kwargs.copy()
        kwargs.pop('index_col', None)
        kwargs.pop('usecols', None)
        if kwargs.get('header', 0) is None:
            kwargs['nrows'] = 1
            return [None] * pd.read_csv(self.filepath, **kwargs).shape[1]
        kwargs['nrows'] = 0
        return pd.read_csv(self.filepath, **kwargs).columns.tolist()

    def shape(self):
        """Return the csv file's shape (rows * columns)."""
        return (len(self), len(self.columns))

    @loggedmethod
    def read(
            self, usecols=None, skipchunks=None, skiprows=None, nrows=None,
            at_once=False, as_series=False, chunksize=None
        ):
        """Yield the csv file's contents by chunks.

        usecols    : optional list of columns to read
                     (this overrides any preset `usecols` argument)
        skipchunks : optional number of chunks to skip
        skiprows   : optional number of rows to skip (overriden by skipchunks)
        nrows      : optional total number of rows to read
        at_once    : whether to read all selected rows instead of
                     yielding them by chunks (bool, default False)
        as_series  : whether to return data as pandas.Series instead
                     of pandas.DataFrame (bool, default False)
        chunksize  : optional chunksize argument overriding the object's
                     chunksize attribute as well as the `at_once` argument

        Note: argument `as_series` may only be True when returning a single
              column of data.
        """
        # Arguments serve modularity, hence pylint: disable=too-many-arguments
        # Check arguments validity.
        check_type_validity(skipchunks, (int, type(None)), 'skipchunks')
        check_type_validity(skiprows, (int, type(None)), 'skiprows')
        check_type_validity(nrows, (int, type(None)), 'nrows')
        check_type_validity(at_once, bool, 'at_once')
        check_type_validity(chunksize, (int, type(None)), 'chunksize')
        # Handle arguments.
        kwargs = self.kwargs.copy()
        if chunksize is not None:
            kwargs['chunksize'] = chunksize
            at_once = False
        elif not at_once:
            kwargs['chunksize'] = self.chunksize
        if usecols is not None:
            self.__manage_usecols(kwargs, usecols)
        self.__manage_index_col(kwargs)
        self.__check_usecols_validity(kwargs)
        self.__manage_rows_skipping(kwargs, skipchunks, skiprows, nrows)
        if as_series and not self.__check_column_unicity(kwargs):
            raise RuntimeError(
                'Attempt at coercing the return of a pandas.Series '
                + 'while loading multiple columns.'
            )
        # Read file and return it using proper parameters and format.
        data = pd.read_csv(self.filepath, **kwargs)
        if not as_series:
            return data
        if at_once:
            return data.iloc[:, 0]
        return map(lambda x: x.iloc[:, 0], data)

    @staticmethod
    def __manage_usecols(kwargs, usecols):
        """Handle `usecols` argument at LargeCorpus.read()."""
        if isinstance(usecols, str):
            usecols = [usecols]
        elif not isinstance(usecols, list):
            raise TypeError(
                "Expected 'usecols' to be of type str, list"
                + " or NoneType, not %s." % type(usecols).__name__
            )
        kwargs['usecols'] = usecols

    def __manage_index_col(self, kwargs):
        """Ensure the index column is properly read at LargeCorpus.read()."""
        index_col = kwargs.get('index_col', None)
        if index_col is not None and 'usecols' in kwargs.keys():
            column = (
                index_col if isinstance(index_col, str)
                else self.columns[index_col]
            )
            if column not in kwargs['usecols']:
                kwargs['usecols'].append(column)
            kwargs['index_col'] = column

    def __check_usecols_validity(self, kwargs):
        """Ensure specified columns to load at LargeCorpus.read() exist."""
        bad_columns = [
            column for column in kwargs.get('usecols', [])
            if column not in self.columns
        ]
        if bad_columns:
            raise KeyError(
                "Cannot match the following column%s: '%s'."
                % ('s' * (len(bad_columns) > 1), "', '".join(bad_columns))
            )

    def __manage_rows_skipping(self, kwargs, skipchunks, skiprows, nrows):
        """Handle rows selection arguments at LargeCorpus.read()."""
        if skipchunks is not None:
            skiprows = skipchunks * kwargs.get('chunksize', self.chunksize)
        if skiprows is not None:
            kwargs['skiprows'] = skiprows
            if self.columns is not None:
                col = self.columns[0]
                kwargs['skiprows'] += len(col) if isinstance(col, tuple) else 1
                kwargs['header'] = None
                kwargs['names'] = self.columns
        if nrows is not None:
            kwargs['nrows'] = nrows

    def __check_column_unicity(self, kwargs):
        """Check if data loaded at LargeCorpus.read() has a single column."""
        n_columns = (
            len(kwargs.get('usecols', self.columns))
            - int(kwargs.get('index_col') is not None)
        )
        return n_columns == 1
