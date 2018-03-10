# CSVTools

CSVTools is a Python package grouping utilitarian classes to perform various
tasks revolving around .csv files manipulation. One of the common objectives
of this package's class is to be scalable to large csv files and adjustable
to the quantity of RAM available, at the sole cost of dilating execution time.

These tasks this package tackles include :

- Sorting a csv file, either randomly or along one of its columns, with `CsvSorter`.
- Merging various (and heterogeneous) csv files into a single one, with `CsvMerger`.
- Writing down (heterogeneous) records to a csv file as they come, with
  `CsvWriter` subclasses, currently including support for `dict` and
   `pandas.DataFrame` records.
- Reading and transforming data stored in a large csv file, operating over
  chunks of data on multiple CPU cores, with `LargeCsvTransformer`.


### User installation

**Dependencies**

- Python 3 (>= 3.3) &nbsp;&nbsp; -- &nbsp;&nbsp; Python 2.x is **not** supported.
- Pandas (>= 0.20) &nbsp;&nbsp; -- &nbsp;&nbsp; third-party package
  distributed under BSD 3-Clause License.
- YAPTools &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
  &nbsp;&nbsp; -- &nbsp;&nbsp; a toolbox developped commonly
  with CSVTools, found [here](https://github.com/pandrey-fr/yaptools/).

**Downloading a copy of the repository**

To copy the project on your local machine, provided you have Git installed,
simply clone the repository with the following command:

```
git clone https://github.com/pandrey-fr/csvtools.git
```

**Installing the package**

**Warning**: It is advised to procede to the installation in a virtual
environment. To learn how to set up such an environment, please refer to
the `venv` [documentation](https://docs.python.org/3/library/venv.html).

To install CSVTools as a `csvtools` package, use the `setup.py` file in
the main folder :

```
python setup.py install
```

### Contributing

If you feel like some modification of CSVTools might be useful to others,
please open an issue on Github, and/or submit a Pull Request with your
modifications.

### License

CSVTools is distributed under the MIT License.
