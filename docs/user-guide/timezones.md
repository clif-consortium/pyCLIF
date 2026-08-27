# Working with Timezones

Proper timezone handling is critical when working with ICU data from multiple sources. This guide explains how CLIFpy manages timezones and best practices for your data.

## Overview

CLIFpy ensures all datetime columns are timezone-aware to:
- Prevent ambiguity in timestamp interpretation
- Enable accurate time-based calculations
- Support data from multiple time zones
- Maintain consistency across tables

## Timezone Specification

### When Loading Data

Always specify the timezone when loading data:

```python
# Specify source data timezone
table = TableClass.from_file(
    data_directory='/data',
    filetype='parquet',
    timezone='US/Central'  # Source data timezone
)

# Common US timezones
# 'US/Eastern', 'US/Central', 'US/Mountain', 'US/Pacific'
# 'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles'
```

### Using Orchestrator

The orchestrator ensures consistent timezone across all tables:

```python
orchestrator = ClifOrchestrator(
    data_directory='/data',
    filetype='parquet',
    timezone='US/Central'  # Applied to all tables
)
```

## Timezone Conversion

### During Loading

CLIFpy automatically converts datetime columns to the specified timezone:

```python
# Original data in UTC
table = TableClass.from_file('/data', 'parquet', timezone='UTC')

# Convert to Central time during loading
table = TableClass.from_file('/data', 'parquet', timezone='US/Central')
```

### After Loading

Convert between timezones using pandas:

```python
# Convert to different timezone
df = table.df.copy()
df['lab_datetime'] = df['lab_datetime'].dt.tz_convert('US/Eastern')

# Localize timezone-naive data (not recommended)
# df['datetime'] = df['datetime'].dt.tz_localize('US/Central')
```

## Common Timezone Issues

### Issue 1: Timezone-Naive Data

**Problem**: Source data lacks timezone information

```python
# This will fail validation
table.validate()
# Error: "Datetime column 'admission_date' is not timezone-aware"
```

**Solution**: Specify timezone during loading

```python
# CLIFpy will localize to specified timezone
table = TableClass.from_file(
    '/data', 
    'parquet', 
    timezone='US/Central'  # Assumes data is in Central time
)
```

### Issue 2: Mixed Timezones

**Problem**: Different tables from different timezones

```python
# Hospital A in Eastern time
labs_a = Labs.from_file('/hospital_a/data', 'parquet', timezone='US/Eastern')

# Hospital B in Pacific time  
labs_b = Labs.from_file('/hospital_b/data', 'parquet', timezone='US/Pacific')
```

**Solution**: Convert to common timezone

```python
# Convert both to UTC for analysis.
#
# Assign back through `.df`. Editing the frame `.df` hands you changes only that
# pandas copy -- the table's stored data (`.data`) is untouched, so `validate()`
# and the summary methods would still see the original timestamps.
a = labs_a.df
a['lab_datetime'] = a['lab_datetime'].dt.tz_convert('UTC')
labs_a.df = a

b = labs_b.df
b['lab_datetime'] = b['lab_datetime'].dt.tz_convert('UTC')
labs_b.df = b

# Combine datasets
combined_labs = pd.concat([labs_a.df, labs_b.df])
```

### Issue 3: Daylight Saving Time

**Problem**: Ambiguous times during DST transitions

```python
# Fall back: 2:00 AM occurs twice
# Spring forward: 2:00 AM doesn't exist
```

**Solution**: Use pytz-aware timezone names

```python
# Good - handles DST automatically
table = TableClass.from_file('/data', 'parquet', timezone='US/Central')

# Avoid - doesn't handle DST
# table = TableClass.from_file('/data', 'parquet', timezone='CST6CDT')
```

## Best Practices

### 1. Know Your Source Timezone

```python
# Document source timezone
"""
Data extracted from Hospital EHR
Timezone: US/Central (America/Chicago)
Includes DST adjustments
"""
table = TableClass.from_file('/data', 'parquet', timezone='US/Central')
```

### 2. Use Consistent Timezones

```python
# Use orchestrator for consistency
orchestrator = ClifOrchestrator('/data', 'parquet', timezone='US/Central')
orchestrator.initialize(tables=['labs', 'vitals', 'medications'])

# All tables now use same timezone
```

### 3. Validate Timezone Handling

```python
# Check timezone after loading
print(f"Lab datetime timezone: {table.df['lab_datetime'].dt.tz}")

# Verify reasonable time ranges
print(f"Earliest: {table.df['lab_datetime'].min()}")
print(f"Latest: {table.df['lab_datetime'].max()}")
```

### 4. Document Timezone Conversions

```python
# Keep audit trail of conversions
metadata = {
    'original_timezone': 'US/Eastern',
    'converted_timezone': 'UTC',
    'conversion_date': datetime.now(),
    'conversion_method': 'pandas dt.tz_convert'
}
```

## Time-based Calculations

### Duration Calculations

Timezone-aware datetimes ensure accurate duration calculations:

```python
# Calculate length of stay
los = adt.df['out_dttm'] - adt.df['in_dttm']
los_hours = los.dt.total_seconds() / 3600

# Time since admission
current_time = pd.Timestamp.now(tz='US/Central')
time_since = current_time - hosp.df['admission_dttm']
```

### Filtering by Time

```python
# Get data from last 24 hours
cutoff = pd.Timestamp.now(tz='US/Central') - pd.Timedelta(hours=24)
recent = table.df[table.df['datetime_column'] >= cutoff]

# Filter to specific date (timezone-aware)
date = pd.Timestamp('2023-01-01', tz='US/Central')
day_data = table.df[table.df['datetime_column'].dt.date == date.date()]
```

### Aggregating by Time

```python
# Hourly aggregation
hourly = table.df.set_index('datetime_column').resample('H').mean()

# Daily aggregation (timezone affects day boundaries!)
daily = table.df.set_index('datetime_column').resample('D').count()
```

## Multi-site Considerations

When combining data from multiple sites:

```python
# Strategy 1: Convert all to UTC
sites = ['site_a', 'site_b', 'site_c']
site_timezones = {
    'site_a': 'US/Eastern',
    'site_b': 'US/Central', 
    'site_c': 'US/Pacific'
}

all_data = []
for site in sites:
    table = Labs.from_file(f'/data/{site}', 'parquet', 
                          timezone=site_timezones[site])
    # Convert to UTC. Bind the pandas view once and edit that -- these rows are
    # going into `all_data`, so there is no need to write them back to `table`.
    df = table.df
    df['lab_datetime'] = df['lab_datetime'].dt.tz_convert('UTC')
    df['site'] = site
    all_data.append(df)

combined = pd.concat(all_data)
```

```python
# Strategy 2: Use site's local time with site column
# Keep original timezone but track source
for site in sites:
    table = Labs.from_file(f'/data/{site}', 'parquet',
                          timezone=site_timezones[site])
    df = table.df
    df['site'] = site
    df['source_timezone'] = site_timezones[site]
    table.df = df   # assign back, or the columns live only on the discarded copy
```

## Timezone Reference

Common medical facility timezones:

```python
US_TIMEZONES = {
    'Eastern': 'US/Eastern',     # NYC, Boston, Atlanta
    'Central': 'US/Central',     # Chicago, Houston, Dallas
    'Mountain': 'US/Mountain',   # Denver, Phoenix
    'Pacific': 'US/Pacific',     # LA, Seattle, San Francisco
    'Toronto': 'America/Toronto' # Toronto, Canada
}
```

## Troubleshooting

### Check Current Timezone

```python
# For a datetime column
print(table.df['datetime_column'].dt.tz)

# For a single timestamp
print(table.df['datetime_column'].iloc[0].tzinfo)
```

### Fix Timezone Issues

```python
# If validation fails due to timezone
if not table.isvalid():
    tz_errors = [e for e in table.errors if 'timezone' in str(e)]
    if tz_errors:
        # Reload with proper timezone
        table = TableClass.from_file('/data', 'parquet', 
                                   timezone='US/Central')
```

## Next Steps

- Review [validation guide](validation.md) for timezone validation
- See [examples]() of timezone handling
- Learn about [multi-site analysis]()