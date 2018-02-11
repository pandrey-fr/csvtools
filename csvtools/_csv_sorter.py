# coding: utf-8

"""Class to sort a csv file."""

import os
import heapq
import tempfile
import shutil

import numpy as np
from yaptools import _alphanum_key, check_type_validity


# No need for more methods, hence pylint: disable=too-few-public-methods
class CsvSorter:
    """Class to sort a csv file along one of its columns or randomly.

    The memory cost of the sorting operation is kept low by using the 'heapq'
    standard library, which provides with a binary heap-based sorting system.
    More precisely, the sorting procedure is the following:
      1. The initial file is read by chunks, each of which is (locally)
         sorted and stored to a temporary file. Here, sorting is based
         on the 'sorted' built-in function, and its cost relies on the
         chunks' size.
      2. The former temporary files are read from by the 'heapq.merge'
         function, which creates a generator yielding sorted results.
         The binary heap sorting algorithm's efficiency (O(log n)) is
         doubled with the memory-costless nature of using a generator
         instead of sorting everything in memory at once.
      3. If there are too many "initial" temporary files to read all
         of them at once, the former step is conducted iteratively on
         sets of temporary files, until all rows have been sorted into
         a single file.

    The implementation was inspired by a blog post by Guido von Rossum,
    creator of Python and Benevolent Dictator For Life, published here:
    {url}

    Usage:

    # Instanciation.
    >>> sorter = CsvSorter(chunksize=5000, max_open=200)

    # Sort a file along one of its columns.
    >>> sorter.sort_file(
    ...         'file_a.csv', 'sorted_file.csv', sorting_axis='some_column',
    ...     )

    # Sort a file randomly (here, the file has a non-default separator).
    >>> sorter.sort_file(
    ...     'file_b.csv', 'randomly_sorted.csv', sorting_axis=None, sep=';'
    ... )
    """
    __doc__ = __doc__.format(url=(
        'https://neopythonic.blogspot.fr/2008/10/'
        + 'sorting-million-32-bit-integers-in-2mb.html'
    ))

    def __init__(self, chunksize=2000, max_open=200):
        """Initialize the object.

        chunksize : number of lines per temporary file - thus also the
                    maximum number of lines sorted through loading in
                    memory (int, default 2000)
        max_open  : maximum number of files that can be opened (read from)
                    at the same time (int, default 200)
        """
        check_type_validity(chunksize, int, 'chunksize')
        check_type_validity(max_open, int, 'max_open')
        self.tempdir = tempfile.mkdtemp()
        shutil.rmtree(self.tempdir)
        self.chunksize = chunksize
        self.max_open = max_open

    def sort_file(
            self, input_file, output_file, sorting_axis,
            has_header=True, sep=',', encoding='utf-8'
        ):
        """Sort a given csv file along one of its columns or randomly.

        input_file   : path to the file which needs sorting
        output_file  : path where to write the sorted file
        sorting_axis : column along which to sort the file ; either a
                       column name (str), cardinal (int) or None, implying
                       a random sorting
        has_header   : whether the file has a header row
                       (bool, default True)
        sep          : value separator of the file (str, default ',')
        encoding     : encoding of the file (str, default 'utf-8')

        Note: If `sorting_axis` is neither None (random) or the first column,
              the value separator must *not* appear inside value fields (i.e.
              within strings).
        """
        # Arguments serve modularity, hence pylint: disable=too-many-arguments
        # Check arguments validity.
        check_type_validity(input_file, str, 'input_file')
        input_file = os.path.abspath(input_file)
        if not os.path.isfile(input_file):
            raise FileNotFoundError("File '%s' cannot be found." % input_file)
        check_type_validity(output_file, str, 'output_file')
        output_file = os.path.abspath(output_file)
        if not os.path.isdir(os.path.dirname(output_file)):
            os.makedirs(os.path.dirname(output_file))
        check_type_validity(
            sorting_axis, (str, int, type(None)), 'sorting_axis'
        )
        check_type_validity(has_header, bool, 'has_header')
        if isinstance(sorting_axis, str) and not has_header:
            raise ValueError(
                "Cannot infer sorting column's position without a header."
            )
        # Conduct sorting.
        header, sorting_key = self._initial_step(
            input_file, has_header, sorting_axis, sep, encoding
        )
        final_step = self._merging_steps(sorting_key)
        self._cleanup_step(
            output_file, final_step, header, (sorting_axis is None), sep
        )

    def _initial_step(self, filepath, has_header, sorting_axis, sep, encoding):
        """Cut the initial file into sorted temporary files.

        Return the file's header, if any, and a row-sorting key function.
        """
        # Compute the number of initial temporary files to create.
        with open(filepath, encoding='utf-8') as initial_file:
            n_rows = sum(1 for _ in initial_file) - int(has_header)
        n_tempfiles = (
            n_rows // self.chunksize + int(n_rows % self.chunksize > 0)
        )
        # Create the initial temporary files, made of sorted data chunks.
        os.makedirs(self.tempdir)
        os.mkdir(os.path.join(self.tempdir, '0'))
        with open(filepath, encoding=encoding) as initial_file:
            # Read the file header if any and establish sorting function.
            header = next(initial_file) if has_header else None
            if sorting_axis is None:
                # False positive on numpy C binding, pylint: disable=no-member
                index = map(str, np.random.permutation(n_rows))
                initial_file = map(sep.join, zip(index, initial_file))
                sorting_key = _alphanum_key
            else:
                if isinstance(sorting_axis, str):
                    sorting_axis = header.split(sep).index(sorting_axis)
                def sorting_key(value):
                    """Contextual rows sorting key function."""
                    return _alphanum_key(value.split(sep)[sorting_axis])
            # Write the n-1 first temporary files.
            for i in range(n_tempfiles - 1):
                rows = (next(initial_file) for _ in range(self.chunksize))
                sorted_rows = sorted(rows, key=sorting_key)
                self._write_tempfile(sorted_rows, step=0, cardinal=i)
            # Write the final temporary file.
            self._write_tempfile(
                sorted(list(initial_file), key=sorting_key),
                step=0, cardinal=(n_tempfiles - 1)
            )
        print('Done creating initial temporary files.')
        return header, sorting_key

    def _merging_steps(self, sorting_key):
        """Iteratively merge temporary files into bigger (sorted) ones.

        Return the cardinal of the final step reached.
        """
        step_n = 0
        while True:
            # List current temporary files. If the file is unique, break.
            temp_files = os.listdir(os.path.join(self.tempdir, str(step_n)))
            if len(temp_files) == 1:
                print('Converged at step %s.' % (step_n - 1))
                break
            # Merge sets of temporary files into new (sorted) temporary files.
            os.mkdir(os.path.join(self.tempdir, str(step_n + 1)))
            files_range = range(0, len(temp_files), self.max_open)
            for i, start in enumerate(files_range):
                end = min(start + self.max_open, len(temp_files))
                to_merge = [
                    self._read_tempfile(step_n, filename)
                    for filename in temp_files[start:end]
                ]
                merged = heapq.merge(*to_merge, key=sorting_key)
                self._write_tempfile(merged, step_n + 1, i)
            # Delete previous temporary files and increment step.
            print('Done with merging step %s.' % step_n)
            shutil.rmtree(os.path.join(self.tempdir, str(step_n)))
            step_n += 1
        return step_n

    def _cleanup_step(
            self, output_file, final_step, header, remove_index, sep
        ):
        """Move the sorted file out of the temporary folder."""
        sorted_path = os.path.join(self.tempdir, str(final_step), '0.tmp')
        with open(output_file, 'w', encoding='utf-8') as output:
            if header is not None:
                output.write(header)
            with open(sorted_path, encoding='utf-8') as sorted_file:
                if remove_index:
                    for row in sorted_file:
                        output.write(row.split(sep, 1)[-1])
                else:
                    shutil.copyfileobj(sorted_file, output)
        shutil.rmtree(self.tempdir)
        print("Successfully moved the sorted file to '%s'." % output_file)

    def _write_tempfile(self, rows, step, cardinal):
        """Write an iterable of rows to a temporary file of given indexes.

        rows     : iterable returning rows to write
        step     : algorithm step reached (implying a storage subfolder)
        cardinal : cardinal (turned into a name) of the temporary file
        """
        path = os.path.join(self.tempdir, str(step), str(cardinal) + '.tmp')
        with open(path, 'w', encoding='utf-8') as temporary_file:
            temporary_file.writelines(rows)

    def _read_tempfile(self, step, cardinal):
        """Yield rows from a tempfile of given indexes."""
        path = os.path.join(self.tempdir, str(step), str(cardinal))
        with open(path, encoding='utf-8') as temporary_file:
            for row in temporary_file:
                yield row
