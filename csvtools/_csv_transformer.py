# coding: utf-8

"""Generic class to manipulate a large csv file."""

import os
import gc
import time
import pickle
import functools
import multiprocessing

import pandas as pd
from yaptools import check_type_validity, pool_transform, _wrap_apply
from yaptools.logger import loggedmethod

from csvtools import LargeCsvReader, CsvMerger, CsvSorter


class LargeCsvTransformer(LargeCsvReader):
    """Class to manipulate and transform data read from a large csv file.

    This class provides with user-friendly and scalable methods to transform
    csv data or conduct comuputations on it without overloading the available
    memory resources, and optionally making use of multiple processor cores.


    * The 'map' and 'apply' methods:

    The key methods defined here are 'map' and 'apply', which share a common
    signature. They conduct a computation by chunks and either yield results
    or store them in a new csv file.

    To choose which one to use, one may use this (rough) equivalence:
        'function(pandas.Dataframe)'    : 'LargeCsvTransformer.map(function)'
        'pandas.Series.apply(function)' : 'LargeCsvTransformer.apply(function)'

    If the function transforms data into a pandas.Series or DataFrame, the
    'output_file' parameter allows for storing to disk and aggregating the
    resulting chunks, so as not to overload memory.

    It the 'output_file' is left to None, both functions return a generator
    yielding chunk-wise results, whose aggregation is left to the user.


    * The 'read' method:

    The 'read' method is inherited from csvtools.LargeCsvReader, wraps
    'pandas.read_csv' and provides with parameters to easily read the
    dataset (or specific parts of it) by chunks.


    * Other methods defined here:
        * 'sort'          : sort the csv file along a given column
        * 'sort_randomly' : sort the csv file in a random order
        * 'value_counts'  : count unique values' occurences in a given column
    """

    @loggedmethod
    def map(
            self, function, read_kwargs=None, pool_size=1, output_file=None,
            **kwargs
        ):
        """Yield the results of passing the corpus' chunks to a given function.

        function    : function taking a pd.Series or pd.DataFrame as input
        read_kwargs : optional dictionary specifying valid reading parameters
                      (see `help(LargeCsvReader.read)` for details)
        pool_size   : number of workers to divide work between (int, default 1)
        output_file : optional path to a csv file where to write results
                      instead of yielding them (str, default None)

        Any valid keyword argument of the mapped function may also be passed.

        Note: writing results to a csv file is only possible if the mapped
              function returns a pandas.Series or pandas.DataFrame object.
        """
        if output_file is None:
            return self._transform_yield(
                function, read_kwargs, pool_size, apply_func=False,
                aggregate=None, **kwargs
            )
        return self._transform_store(
            function, output_file, read_kwargs, pool_size,
            apply_func=False, **kwargs
        )

    @loggedmethod
    def apply(
            self, function, read_kwargs=None, pool_size=1, output_file=None,
            **kwargs
        ):
        """Yield the results of applying a function to the corpus' chunks.

        function    : function taking a pd.Series or pd.DataFrame as input
        read_kwargs : optional dictionary specifying valid reading parameters
                      (see `help(LargeCsvReader.read)` for details)
        pool_size   : number of workers to divide work between (int, default 1)
        output_file : optional path to a csv file where to write results
                      instead of yielding them (str, default None)

        Any valid keyword argument of the applied function may also be passed.

        Note: writing results to a csv file is only possible if the mapped
              function returns a pandas.Series or pandas.DataFrame object.
        """
        if output_file is None:
            return self._transform_yield(
                function, read_kwargs, pool_size, apply_func=True,
                aggregate=pd.concat, **kwargs
            )
        return self._transform_store(
            function, output_file, read_kwargs, pool_size,
            apply_func=True, **kwargs
        )

    def _transform_yield(
            self, function, read_kwargs=None, pool_size=1, apply_func=False,
            aggregate=None, **kwargs
        ):
        """Produce a transformation of the data by chunk and yield it.

        function    : function that either takes a pd.Series or pd.DataFrame
                      as input, or is to be applied to one such object
        read_kwargs : dictionary specifying valid reading parameters
                      for the 'LargeCsvReader.read' method (default None)
        pool_size   : number of workers to divide work between
                      (positive int, default 1)
        apply_func  : whether to call `data.apply(function)` instead
                      of `function(data)` (bool, default False)
        aggregate   : optional results aggregation function
                      (only valid when pool_size > 1)

        Any valid keyword argument of `function` may be also be passed.
        """
        check_type_validity(read_kwargs, (dict, type(None)), 'read_kwargs')
        if read_kwargs is None:
            read_kwargs = {}
        for chunk in self.read(**read_kwargs):
            transformed = pool_transform(
                chunk, function, pool_size, apply_func, aggregate, **kwargs
            )
            if aggregate is None and pool_size > 1:
                for result in transformed:
                    yield result
            else:
                yield transformed
            gc.collect()

    @staticmethod
    def _check_output_file_validity(output_file):
        """Check a file path's validity and build any necessary folder."""
        check_type_validity(output_file, str, 'output_file')
        if not output_file.endswith(os.path.extsep + 'csv'):
            raise ValueError('Incorrect file extension (expected csv).')
        path = os.path.normpath(output_file)
        dirname = os.path.dirname(path)
        if not (dirname == '' or os.path.isdir(dirname)):
            os.makedirs(os.path.dirname(path))

    def _transform_store(
            self, function, output_file, read_kwargs=None, pool_size=1,
            apply_func=False, **kwargs
        ):
        """Produce and store to a csv file a transformation of the data.

        function    : function that either takes a pd.Series or pd.DataFrame
                      as input, or is to be applied to one such object
        output_file : path to the csv file where to write results
        read_kwargs : dictionary specifying valid reading parameters
                      for the 'LargeCsvReader.read' method (default None)
        pool_size   : number of workers to divide work between (positive int)
        apply_func  : whether to call `data.apply(function)` instead
                      of `function(data)` (bool, default False)

        Any valid keyword argument of the `function` may be also be passed.
        """
        # Check arguments validity and build necessary folders if any.
        self._check_output_file_validity(output_file)
        check_type_validity(read_kwargs, (dict, type(None)), 'read_kwargs')
        if read_kwargs is None:
            read_kwargs = {}
        check_type_validity(pool_size, int, 'pool_size')
        if pool_size <= 0:
            raise ValueError('Invalid pool size value: negative integer.')
        check_type_validity(apply_func, bool, 'apply_func')
        # Set up the function to use, chunks' size and csv storage variables.
        function = (
            functools.partial(_wrap_apply, func=function, **kwargs)
            if apply_func else functools.partial(function, **kwargs)
        )
        chunksize = read_kwargs.get('chunksize', self.chunksize)
        read_kwargs['chunksize'] = chunksize
        temp_name = ('_part{0}' + os.path.extsep).join(
            output_file.rsplit(os.path.extsep, 1)
        )
        # Conduct actual transformation and storage of data chunks.
        temp_files = self.__transform_to_temporary_files(
            function, read_kwargs, pool_size, temp_name
        )
        self.__merge_temporary_files(temp_files, output_file)

    def __transform_to_temporary_files(
            self, function, read_kwargs, pool_size, temp_name
        ):
        """Multiprocess the transformation and storage to temp files of data.

        This method is to be called from the '_transform_store' one, which
        sets up its arguments in accordance with API-level inputs.
        """
        temp_files = []
        with multiprocessing.Pool(pool_size) as pool:
            for i, chunk in enumerate(self.read(**read_kwargs)):
                callback = functools.partial(
                    self.__write_to_csv, write_index=True,
                    output_file=temp_name.format(i)
                )
                pool.apply_async(
                    function, (chunk,), callback=callback,
                    error_callback=self.log_exception
                )
                temp_files.append(temp_name.format(i))
                # Wait until a pool worker is available.
                # No other way to do so, thus pylint: disable=protected-access
                while len(pool._cache) >= pool_size:
                    time.sleep(1)
            pool.close()
            pool.join()
        return temp_files

    def __write_to_csv(self, chunk, output_file, write_index=True):
        """Write a given data chunk to csv. Pickle it on type invalidity.

        output_file : path to a csv file where to write the data chunk
        write_index : whether to write the index (bool, default True)
        """
        if isinstance(chunk, pd.Series):
            chunk = pd.DataFrame(chunk)
        if isinstance(chunk, pd.DataFrame):
            chunk.to_csv(
                output_file, mode='w', encoding='utf-8',
                header=True, index=write_index, sep=';'
            )
            self.log(
                "Successfully wrote '%s' (%s rows)."
                % (output_file, len(chunk)), 'info'
            )
            return None
        # Verbosely pickle the record in case of type invalidity.
        with open(output_file[:-3] + 'pickle', 'wb') as dump:
            try:
                pickle.dump(chunk, dump)
            except pickle.PicklingError as exception:
                pickling_msg = 'Attempt to pickle it failed: %s.' % (
                    'PicklingError: ' + ';'.join(map(str, exception.args))
                )
            else:
                pickling_msg = 'Suscessfully pickled it.'
        self.log(
            "Invalid record type: '%s'. %s"
            % (type(chunk).__name__, pickling_msg), level='error'
        )

    def __merge_temporary_files(self, temp_files, output_file):
        """Merge a list of temporary csv files if none of them are missing.

        temp_files  : list of paths to the temporary csv files
        output_file : path to the csv file to merge files into which
        """
        missing = [
            path for path in temp_files if not os.path.isfile(path)
        ]
        if missing:
            self.log(
                'Missing temporary files: %s. Merging aborted.' % missing,
                level='info'
            )
            return None
        self.log('Starting to merge temporary file...', 'info')
        csv_merger = CsvMerger(os.path.dirname(output_file))
        csv_merger.stage(temp_files)
        csv_merger.merge_staged_files(
            os.path.basename(output_file), sort_files=True
        )
        self.log(
            'Successfully merged temporary files into %s.' % output_file,
            level='info'
        )

    def value_counts(self, column, normalize=False, pool_size=1):
        """Return unique values' occurences counts for a given variable.

        This wraps 'pandas.Series.value_counts' to be computed by chunk.

        column    : name of the column whose values to count (str)
        normalize : whether to return relative frequencies instead
                    of absolute counts (bool, default False)
        pool_size : number of workers to divide work between (positive int)
        """
        check_type_validity(column, str, 'column')
        read_kwargs = {'usecols': [column], 'as_series': True}
        counts = pd.Series(name=column)
        for count in self.map(pd.Series.value_counts, read_kwargs, pool_size):
            counts = counts.add(count, fill_value=0, )
        return counts / len(self) if normalize else counts.apply(int)

    @loggedmethod
    def sort(
            self, output_file, sorting_column, assign_as_data=True
        ):
        """Create a copy of the csv file, sorted along one of its columns.

        output_file    : path to the sorted csv file (str)
        sorting_column : column name (or cardinal) along which to sort
                         the dataset ; if None, the file is sorted randomly
        assign_as_data : whether to replace this corpus' data file
                         with the sorted one (bool, default True)
        """
        msg = 'randomly' if sorting_column is None else 'along column %s' % (
            sorting_column if isinstance(sorting_column, str)
            else self.columns[sorting_column]
        )
        self.log('Starting to sort the csv file %s.' % msg, 'info')
        sorter = CsvSorter(self.chunksize, min(100, self.chunksize // 100))
        sorter.sort_file(
            self.filepath, output_file, sorting_axis=None,
            has_header=(self.kwargs.get('header', 0) is not None),
            sep=self.kwargs.get('sep', ','),
            encoding=self.kwargs.get('encoding', 'utf-8')
        )
        self.log(
            'Successfully created a copy of the csv file sorted %s.' % msg,
            level='info'
        )
        if assign_as_data:
            self.filepath = os.path.abspath(output_file)
            self.log('Assigned sorted data as the current dataset.', 'info')

    def sort_randomly(self, output_file, assign_as_data=True):
        """Create a randomly sorted copy of the csv file.

        output_file    : path to the sorted csv file (str)
        assign_as_data : whether to replace this corpus' data file
                         with the sorted one (bool, default True)
        """
        self.sort(output_file, None, assign_as_data)
