# BMI Calculator
This project is a solution for the *Python BMI Calculator Offline Coding Challenge V7*. It calculates the BMI for a dataset containing people's weights and heights.

## Settings
The following were used in this solution:
* Python 3.9.7
* PySpark 3.2.0
* Pytest 7.0.1

The reason for using PySpark is because we'll running this for large volumes of data (see problem statement question 4 a):
> imagine this will be used in-product for 1 million patients

## Setup
Install and activate a virtual environment:
```bash
pip install virtualenv
virtualenv venv --python=python3.9
source venv/bin/activate
```

Install requirements:
```bash
pip install -r requirements.txt
```

## Logging
I've created a logger module `./customlogger.py` with a file handler that outputs log messages into the `logs` directory with the following filename: `YYYY-MM-DD_HH-SS.log`

To use the module, import it e.g.
```python
from customlogger import logger
```

Inisitiate the logger by passing a name e.g.
```python
log = logger(__name__)
```

[Excerpt from The Hitchhiker’s Guide to Python for using `__name__` for the logger name](https://docs.python-guide.org/writing/logging/#logging-in-a-library)
> Best practice when instantiating loggers in a library is to only create them using the `__name__` global variable: the logging module creates a hierarchy of loggers using dot notation, so using `__name__` ensures no name collisions.

## Code Explanation
Before running the BMI Calculator, ensure that the `profiles.json` (i.e. JSON data from the problem statement) and `categories.csv` (i.e. Table 1 from the problem statement) is in `./data/input`.

To run the BMI Calculator:
```bash
python bmi.py
```

This will call the `BMICalcultor` class followed by the `run()` method.

### Directories
Upon instantiating the class the `create_folder()` method is called and it creates the `./data` directory and the sub directories `intermediate`, `reject`, and `output`.
    * Something like `/tmp/data` can also be used if storing results temporarily.
    * `intermediate` stores data after each transformation step.
    * `reject` stores data for unmatched BMI categories. This will help see anomalies in the data e.g. negative heights or weights, unrealistic heights or weights, etc.
    * `output` stores the final results.

### Persisting Data
The reason for persisting data after each transformation step is so that we troubleshoot data quality issues if they arise. I've used the `parquet` format to write the data for efficiency in storage and querying for use in, say, Amazon Athena.

## Testing
Run unit tests via `pytest`. Within the source directory run:
```bash
pytest
```
I am using the `pyspark_test` module to compare DataFrames. In earlier versions of `PySpark` you would need to `pip install pyspark_test` but as of later versions of `PySpark` it's already included.

Due to time constraints I wasn't able to complete all the unit tests.
