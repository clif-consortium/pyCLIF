# Testing Guidelines for `clifpy`

This guide outlines some general testing principles for `clifpy` developers. The following draws upon the manually curated and proof-read tests (starting from [`test_acceptable_dose_unit_patterns`](../tests/tables/test_medication_admin_continuous.py#L417)) in [test_medication_admin_continuous.py](../tests/tables/test_medication_admin_continuous.py) as a reference implementation.

<!-- invisible-code-block: python
import pytest
import pandas as pd
from pathlib import Path
-->

## Unit Tests
In most cases, tests should be **unit tests**, meaning they should test only **one granular aspect** of a transformation -- and not lump them together -- for us to have a precise sense of the expected behavior of a function. To enable this, functions themselves need to be structured in a similarly granular fashion, exposing them for effective testing. 

In line with this principle, the current med dosing unit converter is structured around **3 steps**: 
- [`_acceptable_dose_unit_patterns`](../clifpy/tables/medication_admin_continuous.py#L116) lists all the valid combinations of dose units that the converter can handle
    - e.g. `'mcg/kg/min'` is acceptable while `'mcg/lb/hr'` is not.
- [`_normalize_dose_unit_pattern`](../clifpy/tables/medication_admin_continuous.py#L148) converts a dose unit to lower cases and removes white spaces, and uses `_acceptable_dose_unit_patterns` to check if the normalized unit is acceptable.
    - e.g. `'mcg / KG/min'` → `'mcg/kg/min'` while `'mcg/lb/hr'` would be reported in warning.
- [`convert_dose_to_limited_units`](../clifpy/tables/medication_admin_continuous.py#L213) converts doses from normalized units to one of three limited options: **mcg/min** for mass-based medications; **ml/min** for volume-based medications; **units/min** for special unit-based medications (e.g. insulin)
    - which could be then converted to user-defined units -- yet to be implemented


## Test Data as Fixtures
**Fixtures** are data (or any test set-ups in general) that are useful across multiple tests and thus should not / need not be re-generated in every test. AI agents most likely tend to create these fixtures directly by writing in the testing .py script, which may create issues in readability and stability. If the test data have a clear tabular structure, it is **desirable to store them separately in CSV files** for easy viewing and quality assurance. 

For example, the testing fixture data for testing the [`_normalize_dose_unit_pattern`](../clifpy/tables/medication_admin_continuous.py#L148) function is stored under the same name, `test_normalize_dose_unit_pattern.csv`: 

| case    | med_dose_unit    | med_dose_unit_clean |
|---------|------------------|---------------------|
| valid   | ML/HR            | ml/hr               |
| valid   | mcg / kg/ min    | mcg/kg/min          |
| invalid | tablespoon/hr    | tablespoon/hr       |

where **`med_dose_unit`** stores the input cases and **`med_dose_unit_clean`** stores the correct, expected output. 

The **'case' column** can be used to group related test scenarios: for example, `_normalize_dose_unit_pattern` handles the acceptable dose units differently from the unacceptable ones, so it is desirable to create two separate tests for this function, one for handling the acceptable unit patterns and the other for the unacceptable ones, given the difference in expected behaviors. They can take in the same test fixture data and filter by `case`, and be named [`test_normalize_dose_unit_pattern_recognized`](../tests/tables/test_medication_admin_continuous.py#L477) and [`test_normalize_dose_unit_pattern_unrecognized`](../tests/tables/test_medication_admin_continuous.py#L504) respectively.

To create a fixture, use the `@pytest.fixture` decorator:
```python
@pytest.fixture
def load_fixture_csv():
    """Helper fixture to load CSV fixture from tests/fixtures/medication_admin_continuous/"""
    def _load(filename):
        path = Path(__file__).parent.parent / 'fixtures' / 'medication_admin_continuous' / filename
        return pd.read_csv(path)
    return _load

@pytest.fixture
def normalize_dose_unit_pattern_test_data(load_fixture_csv):
    """
    Load test data for dose unit pattern normalization tests.
    
    Returns CSV data with columns:
    - case: 'valid' or 'invalid' to categorize test scenarios
    - med_dose_unit: Original dose unit string
    - med_dose_unit_clean: Expected normalized result
    """
    return load_fixture_csv('test_normalize_dose_unit_pattern.csv')
```


The easiest way to check the observed output is the same as the expected is to use `pandas`' `assert_series_equal` for fast execution. If your function reorders the input data, you can introduce a `row_index` column in your test fixture data and sort the output before performing the `assert_series_equal` check.

```python
@pytest.mark.unit_conversion
def test_normalize_dose_unit_pattern_recognized(normalize_dose_unit_pattern_test_data):
    """Test unit normalization for recognized patterns"""
    mac_obj = MedicationAdminContinuous()
    test_df = normalize_dose_unit_pattern_test_data.query("case == 'valid'")
    result_df, unrecognized = mac_obj._normalize_dose_unit_pattern(test_df[['med_dose_unit']])
    
    pd.testing.assert_series_equal(
        result_df['med_dose_unit_clean'].reset_index(drop=True),
        test_df['med_dose_unit_clean'].reset_index(drop=True),
        check_names=False
    )
    assert unrecognized is False  # All units should be recognized
```

## Test Markers
Use **pytest markers** to group related tests and run them in a batch. The general syntax is `@pytest.mark.<group_name>`. 
```python
@pytest.mark.unit_conversion  
def test_acceptable_dose_unit_patterns():
    ...

@pytest.mark.unit_conversion
def test_normalize_dose_unit_pattern_recognized(normalize_dose_unit_pattern_test_data):
    ...
```

For example, by adding the `@pytest.mark.unit_conversion` decorator to all the tests related to med dosing unit conversion, we can run all of them in one command, allowing targeted execution:
```bash
pytest -m unit_conversion -vs  # -m: run only marked tests, -v: verbose, -s: show print statements
```

## Exception Handling and Edge Cases
Make sure you always create unit tests that cover the **corner cases**, especially with regard to **exception handling**. Some common cases are:
- **Empty DataFrames**
- **Missing required columns**
- **Logging Validation**: Check that warnings and info messages are logged correctly

```python
def test_method_no_data_provided():
    """Test ValueError is raised when no data provided"""
    obj = TableClass()
    # obj.df is None since no data was provided
    with pytest.raises(ValueError, match="No data provided"):
        obj.some_method()
```

```python
def test_method_empty_dataframe():
    """Test handling of empty DataFrame"""
    obj = TableClass()
    empty_df = pd.DataFrame({'required_col': pd.Series([], dtype='object')})
    result = obj.some_method(empty_df)
    assert result.empty or result is not None  # Define expected behavior
```

```python
def test_warning_logged(caplog):
    """Test that warnings are logged for unrecognized data"""
    with caplog.at_level('WARNING'):
        obj.method_that_warns()
    assert "not recognized by the converter" in caplog.text
```

## Test Documentation

### Naming Patterns
Use **descriptive test names**: follow `test_<full_function_name>_<scenario>` pattern. (Since the test functions are never exposed to the users, we can afford to error on the side of overabundant clarity here.)

### Docstrings
Write **clear docstrings** explaining what each test validates:

```python
def test_normalize_dose_unit_pattern_unrecognized(normalize_dose_unit_pattern_test_data, caplog):
    """
    Test that the `_normalize_dose_unit_pattern` private method handles unrecognized dose units.
    
    Validates:
    1. Unrecognized units are identified and returned as a dictionary with counts
    2. Warning is logged with details about unrecognized units
    3. All invalid units from test data appear in the unrecognized dictionary
    
    Uses fixture data filtered for 'invalid' test cases.
    """
```

## Running Tests

### Full Test Suite
```bash
pytest tests/tables/test_<table_name>.py 
```

### Specific Test Functions
```bash
pytest tests/tables/test_<table_name>.py::test_specific_function 
```

### With Markers
```bash
pytest -m <marker_name>  # -m: filter by marker
```