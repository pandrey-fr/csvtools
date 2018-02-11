# coding: utf-8

"""Generic class to handle dynamic csv storage records flows."""

from shutil import copyfileobj
import pickle
import os
from abc import ABCMeta, abstractmethod

from yaptools.logger import LoggedObject, loggedmethod


CSV_WRITER_DOCSTRING = """
    The task resolved here is to store dynamically some data into a csv
    file (which may pre-exist or not), i.e. to format records and insert
    them in the file "as they come", keeping track of columns' order and
    handling cases where "new" data fields appear in the incoming data.

    The storage is "dynamic" in the sense that records can be fed at any
    time and pace. They are buffered by the CsvWriter until an arbitrary
    number of rows have been accumulated, at which point they are written
    to the csv file. When needed, the file's header will also be updated
    during the (user- or record-triggered) finalization of the task.

    There are two ways to pass records to an instanciated object. One is
    to use the 'handle_record' method each time a record is to be passed
    and to use the 'finish_handling_procedure' when there are no records
    left. The other is to wrap the 'handle_queue' method within either a
    threading.Thread or multiprocessing.Process, using a Queue object as
    argument; records can then be put to the queue, from which they will
    automatically be fetched and handled by the CsvWriter. The procedure
    can then be ended by putting a None to the queue.
"""

CSV_WRITER_EXAMPLE = """
    # Case 1: Passing down results as they come:
    >>> writer.handle_record(record_1)
    # [etc.]
    >>> writer.handle_record(record_n)
    >>> writer.finish_handling_procedure()

    # Case 2: Passing down results through a queue:
    >>> queue = multiprocessing.Queue()
    >>> csv_storage = multiprocessing.Process(
    ...     target=writer.handle_queue, args=(queue,)
    ... )
    >>> csv_storage.start()
    >>> queue.put(record_1)
    # [etc.]
    >>> queue.put(record_n)
    >>> queue.put(None)
"""


class AbstractCsvWriter(LoggedObject, metaclass=ABCMeta):
    """Abstract class to handle dynamic csv storage of a flow of records.
    {0}

    This class defines a generic framework, including abstract methods
    which need overriding by children classes designed to handle given
    types of data records (e.g. dict). Those abstract methods are:

      * _reset_buffer       : set self.buffer to its "empty" state
      * _add_to_buffer      : add a record to self.buffer
      * _get_buffer_columns : return the self.buffer's current column names
      * _to_csv             : dump self.buffer's contents to the csv file


    "Abstract" usage (requiring the previously listed overridings):

    # Writer instanciation (SomeCsvWriter being a hypothetical child class).
    >>> writer = SomeCsvWriter('file.csv', buffer_size=100, sep=',')
    {1}
    """
    __doc__ = __doc__.format(CSV_WRITER_DOCSTRING, CSV_WRITER_EXAMPLE)

    def __init__(self, path, buffer_size, sep=';', logger=None):
        """Set up the handler's initial state.

        path        : path to the destination csv file, which may pre-exist
        buffer_size : maximum number of rows to keep in memory before writing
                      them to the csv file (positive integer)
        sep         : values separator of the csv file (str, default ';')
        logger      : optional Logger object to use instead of the default
                      one (which logs everything to the console)
        """
        path = os.path.normpath(path)
        self._check_path_validity(path)
        self.path = path
        self.buffer_size = buffer_size
        self.buffer = None
        self._reset_buffer()
        self.number_of_rows_stored = 0
        self.sep = sep
        self.header = self._get_current_csv_header()
        self._has_changed = False
        super().__init__(logger)

    @staticmethod
    def _check_path_validity(path):
        """Check a path argument's validity as path attribute."""
        path = os.path.abspath(path)
        folder = os.path.dirname(path)
        if not os.path.exists(folder):
            raise FileNotFoundError("Directory '%s' not found." % folder)
        if not path.endswith(os.path.extsep + 'csv'):
            raise ValueError("'%s' is not a csv file." % path)

    def _get_current_csv_header(self):
        """Read the csv file's initial header, if any."""
        header = []
        if os.path.exists(self.path) and os.stat(self.path).st_size > 0:
            with open(self.path, 'r', encoding='utf-8') as csv_file:
                header = csv_file.readline().strip('\n').split(self.sep)
        return header

    def handle_queue(self, queue):
        """Handle a records flow drawn from a Queue."""
        record = queue.get()
        while record is not None:
            if isinstance(record, list):
                for _record in record:
                    self.handle_record(_record)
            else:
                self.handle_record(record)
            record = queue.get()
        self.finish_handling_procedure()

    def handle_record(self, record):
        """Bufferize a record and write the buffer to disk if it's full."""
        self._add_to_buffer(record)
        if len(self.buffer) >= self.buffer_size:
            self._write_buffer_to_disk()

    def finish_handling_procedure(self):
        """Write the buffer to disk and update the csv header if needed."""
        if self.buffer:
            self._write_buffer_to_disk()
        if self._has_changed:
            self._update_csv_header()

    def _write_buffer_to_disk(self):
        """Write buffered elements to csv. On failure, serialize them."""
        try:
            self._write_buffer()
        # Catch any exception, by design. pylint: disable=broad-except
        except Exception as exception:
            self.log(
                'Exception %s occured while writing buffered rows to csv:'
                ' %s' % (type(exception).__name__, str(exception.args)),
                level='error'
            )
            self._serialize_buffer()
        self._reset_buffer()

    def _write_buffer(self):
        """Write buffered elements to csv, tracking changes and advancement."""
        # Check columns updates.
        first_time = len(self.header) == 0
        columns = self._get_buffer_columns()
        new_columns = [name for name in columns if name not in self.header]
        self.header.extend(new_columns)
        self._has_changed = (
            self._has_changed or (len(new_columns) > 0 and not first_time)
        )
        # Write rows to csv.
        self._to_csv(first_time)
        # Log success.
        self.number_of_rows_stored += len(self.buffer)
        self.log(
            "Successfully wrote %s rows to '%s' (total: %s)." % (
                len(self.buffer), os.path.basename(self.path),
                self.number_of_rows_stored
            ),
            level='info'
        )

    def _serialize_buffer(self):
        """Serialize the current buffer list."""
        path = os.path.join(os.path.dirname(self.path), 'buffer_{0}.pickle')
        path = path.format(id(self.buffer))
        try:
            with open(path, 'wb') as pickle_file:
                pickle.dump(self.buffer, pickle_file)
        # Catch any exception, by design. pylint: disable=broad-except
        except Exception as exception:
            self.log(
                'Exception %s occured while pickling buffered rows:'
                ' %s' % (type(exception).__name__, str(exception.args)),
                level='error'
            )
        else:
            self.log(
                'Serialized %s rows under id %s.'
                % (len(self.buffer), id(self.buffer)),
                level='info'
            )

    @loggedmethod
    def _update_csv_header(self):
        """Update the csv file's header."""
        self.log('Updating the csv file\'s header...', 'info')
        tempname = self.path.rsplit('.', 1)[0] + '.temp'
        with open(self.path, 'r', encoding='utf-8') as infile:
            with open(tempname, 'w', encoding='utf-8') as outfile:
                infile.readline()
                outfile.write(self.sep.join(self.header) + '\n')
                copyfileobj(infile, outfile)
        os.remove(self.path)
        os.rename(tempname, self.path)
        self._has_changed = False
        self.log('Succesfully updated the csv file\'s header.', 'info')

    @abstractmethod
    def _reset_buffer(self):
        """Reset the buffer to its empty state."""
        raise NotImplementedError('No method defined to reset the buffer.')

    @abstractmethod
    def _add_to_buffer(self, record):
        """Bufferize a given record."""
        raise NotImplementedError('No method defined to bufferize records.')

    @abstractmethod
    def _get_buffer_columns(self):
        """Return a list of unique column names appearing in the buffer."""
        raise NotImplementedError('No method define to read buffer columns.')

    @abstractmethod
    def _to_csv(self, first_time):
        """Write buffered elements to csv."""
        raise NotImplementedError('No method defined to write buffer to csv.')
