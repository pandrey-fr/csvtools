# coding: utf-8

"""Class to handle csv files merging."""

import os
import shutil

from yaptools import alphanum_sort, _alphanum_key, check_type_validity


class CsvMerger:
    """Class to handle csv files merging with minimum memory usage.

    Any file encoding or value separator may be used. However, text
    fields delimiter should be the double quote symbol ("), so as
    to avoid treating text-contained symbols as value separators.

    Usage:
    >>> merger = CsvMerger()
    >>> merger.stage(['file_part0.csv', 'file_part1.csv'], sep=',')
    >>> merger.merge_staged_files('final_file.csv')

    When merging two files ('A.csv' and 'B.csv') where B's columns
    all appear in A, you may also use the 'merge_files' method:
    >>> merger.merge_files('a.csv', 'b.csv')
    """

    def __init__(self, output_folder='.'):
        """Initialize the dataset.

        output_folder : reference directory when writing output files
        """
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        self.output_folder = os.path.abspath(output_folder)
        self.staged_files = []

    def stage(self, files, sep=',', encoding='utf-8'):
        """Stage files for merging, recording specified import parameters.

        files    : either a single path (str) or a list of paths
                   (a path may either point to a csv file or to
                   a folder containing csv files to stage)
        sep      : value separator of the staged file(s) (str)
        encoding : encoding of the staged file(s) (str)
        """
        def add_file_if_csv(path):
            """Add a path to staged_files if it leads to a csv file."""
            if path.endswith(os.path.extsep + 'csv'):
                self.staged_files.append((path, sep, encoding))
                print("Staged file '%s'." % path)
        # Check arguments validity.
        check_type_validity(files, (str, list), 'files')
        if isinstance(files, str):
            files = [files]
        elif not all(isinstance(item, str) for item in files):
            raise TypeError('Files list contains non-string elements.')
        # Stage files.
        for path in files:
            path = os.path.abspath(path)
            if os.path.isfile(path):
                add_file_if_csv(path)
            elif os.path.isdir(path):
                dirname = os.path.normpath(path)
                for filename in alphanum_sort(os.listdir(dirname)):
                    add_file_if_csv(os.path.join(dirname, filename))
            else:
                raise FileNotFoundError('Cannot find "%s".' % path)

    def merge_staged_files(
            self, output_file, sep=';', encoding='utf-8',
            remove_merged=True, sort_files=False
        ):
        """Merge all staged files into a new one.

        output_file   : path to the csv file to produce, relative
                        to the 'output_folder' attribute (str)
        sep           : value separator of the output file (str, default ';')
        encoding      : encoding of the output file (str, default 'utf-8')
        remove_merged : whether to remove files after merging them
                        (bool, default True)
        sort_files    : whether to sort staged files by name before
                        iterating over them (bool, default False)
        """
        # Build any necessary folder.
        output_path = os.path.join(self.output_folder, output_file)
        if not os.path.isdir(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))
        # Generate and write the output file's header.
        global_header = self.get_staged_files_header()
        with open(output_path, 'w', encoding=encoding) as outfile:
            outfile.write(sep.join(global_header) + '\n')
        # Merge staged files into the final one. Optionally remove them.
        if sort_files:
            self.staged_files.sort(key=lambda x: _alphanum_key(x[0]))
        for path, file_sep, file_encoding in self.staged_files:
            self.merge_files(
                output_path, path, sep, file_sep, encoding, file_encoding
            )
            if remove_merged:
                os.remove(path)
        # Unstage merged files.
        self.staged_files = []

    def merge_files(
            self, main_file, merged_file, main_sep=';', merged_sep=';',
            main_encoding='utf-8', merged_encoding='utf-8'
        ):
        """Merge a given csv file into another.

        The merged file's column should all appear in the main one,
        otherwise an exception will be raised. In the latter cases,
        the 'stage_files' and 'merge_stage_files' methods should be
        used.

        main_file       : path to the csv file to merge the other into which
        merged_file     : path to the csv file to merge into the main one
        main_sep        : values separator of the main csv file
        merged_sep      : values separator of the merged csv file
        main_encoding   : encoding of the main csv file
        merged_encoding : encoding of the merged csv file
        """
        # Arguments serve modularity, hence pylint: disable=too-many-arguments
        main_header = self._read_header(main_file, main_sep, main_encoding)
        with open(main_file, 'a', encoding=main_encoding) as outfile:
            with open(merged_file, encoding=merged_encoding) as infile:
                merged_header = (
                    self._parse_csv_row(infile.readline(), merged_sep)
                )
                # Check file headers' compatibility.
                missing = [
                    column for column in merged_header
                    if column not in main_header
                ]
                if missing:
                    raise ValueError(
                        "Some columns of the merged file do not appear in "
                        + "the main one: ['%s']" % "', '".join(missing)
                    )
                # Merge the second file into the main one.
                if main_header == merged_header:
                    shutil.copyfileobj(infile, outfile)
                else:
                    index = self._build_index(merged_header, main_header)
                    for row in infile:
                        row = self._sort_csv_row(row, index, merged_sep)
                        outfile.write(main_sep.join(row) + '\n')

    def get_staged_files_header(self):
        """Return a list covering the union of staged files' columns."""
        global_header = []
        headers = map(lambda args: self._read_header(*args), self.staged_files)
        for file_header in headers:
            global_header.extend([
                name for name in file_header if name not in global_header
            ])
        return global_header

    def _read_header(self, path, sep, encoding):
        """Read a csv file's header."""
        with open(path, encoding=encoding) as csv_file:
            header = csv_file.readline()
        return self._parse_csv_row(header, sep)

    @staticmethod
    def _parse_csv_row(row, sep):
        """Parse a given csv file row along its value separator."""
        row = row.strip('\n')
        protected = False
        start = 0
        fields = []
        for i, char in enumerate(row):
            if char == sep and not protected:
                fields.append(row[start:i])
                start = i + 1
            elif char == '"':
                protected = not protected
        fields.append(row[start:])
        return fields

    @staticmethod
    def _build_index(local_header, global_header):
        """Return an index list aligning records of csv files of given headers.

        local_header  : header of the csv file whose rows to sort
        global_header : header of the csv file to write rows to
        """
        index = []
        for name in global_header:
            try:
                index.append(local_header.index(name))
            except ValueError:
                index.append(None)
        return index

    def _sort_csv_row(self, row, index, sep):
        """Parse a csv row and align its values according to a given index."""
        fields = self._parse_csv_row(row, sep)
        return [fields[i] if i is not None else '' for i in index]
