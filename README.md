# Oemof Pipe

This package simplifies creating and updating `oemof.tabular` datapackages.

## Installation

You can install the package using `uv`:

```bash
uv pip install -e .
```

Or using `pip`:

```bash
pip install -e .
```

## Usage

The package is run via `main.py` and provides two main commands: `blueprint` and `scenario`.

### Creating a Base Datapackage (Blueprint)

To create a new datapackage based on a blueprint:

```bash
python main.py blueprint <blueprint_name>
```

This will look for a file named `<blueprint_name>.yaml` in the `blueprints/` directory and create a corresponding datapackage in the `datapackages/` directory.

### Creating a Scenario

To create a scenario based on an existing datapackage:

```bash
python main.py scenario <datapackage_name> <scenario_name>
```

This will duplicate the existing datapackage `<datapackage_name>` into a new one named `<datapackage_name>_<scenario_name>` and apply the changes defined in `scenarios/<scenario_name>.yaml`.

---

## Blueprints

Blueprints define the structure and static elements of your energy system model. They are stored as YAML files in the `blueprints/` directory.
Creating a blueprint results in an oemof.tabular datapackage stored in `datapackages/` directory.

### Blueprint Structure

A blueprint consists of several key sections:

- `timeindex`: Defines the temporal scope.
  - `start`: Start date and time (e.g., `2050-01-01 00:00:00`).
  - `periods`: Number of hourly steps.
- `regions`: (Optional) A list of regions. If provided, elements can be automatically duplicated for each region.
- `elements`: Defines the components of the energy system (e.g., loads, buses, generators).
- `sequences`: (Optional) Explicitly defines additional sequence resources.

### Element Definition

Each element in the `elements` section has the following options:

- `component`: The type of component (e.g., `load`, `bus`, `volatile`). These must match YAML files in the `components/` directory.
- `attributes`: A list of attributes to include in the output CSV. If omitted, all attributes defined in the component's YAML are used.
- `regions`: (Optional) Override the global `regions` list for this specific element.
- `instances`: A list of actual component instances.
  - Each instance must have a `name`.
  - Other attributes (like `bus`, `amount`) should match the used attributes.

### Example Blueprint

```yaml
timeindex:
  start: 2050-01-01 00:00:00
  periods: 8760

regions:
  - DE
  - FR

elements:
  electricity_bus:
    component: bus
    instances:
      - name: electricity

  demand:
    component: load
    attributes:
      - name
      - bus
      - amount
    instances:
      - name: electricity-load
        bus: electricity
        amount: 5000
```

If regions are defined, the `electricity-load` will be created for both `DE` and `FR`, with names like `DE-electricity-load` and `FR-electricity-load`.

---

## Scenarios

Scenarios allow you to modify an existing datapackage by overriding values or adding new data from external sources. They are stored in the `scenarios/` directory.

### Scenario Structure

- `raw_dir`: (Optional) The directory where raw data files are located. Defaults to `raw/`.
- `elements`: A list of CSV files to apply to element resources.
  - `path`: Path to the CSV file (relative to `raw_dir`).
  - `scenario`: The value in the `scenario_column` to filter for. Scenario "ALL" is always used.
  - `scenario_column`: The column name in the CSV used for filtering (default: `scenario`).
- `sequences`: A list of CSV files to apply to sequence resources (e.g., time series).
  - `path`: Path to the CSV file.
  - `sequence_name`: The name of the sequence resource to update.
  - `scenario`: Filter value.
  - `scenario_column`: Filter column.

### Data Application Logic

The scenario command uses DuckDB to join raw data with existing datapackage resources.
For elements, it matches based on the `name` column.
For sequences, it updates columns matching the names in the raw data.

**Element Datasets**

Element datasets can either contain one value per row or multiple values per row, which is applied to matching element instance.

In case of a single value per row, name of the attribute is looked up by `var_name_col` (defaults to `var_name`) and the value by `var_value_col` (defaults to `var_value`).
Example dataset:
```csv
id;scenario;name;var_name;carrier;region;tech;type;var_value;var_unit;source;comment
0;ALL;l1;amount;biomass;AD;gt;conversion;10;MW;;Green field assumption
```
Here, an `amount` of `10` would be set for component `l1`, if attribute `amount` is present in target element.

In case of multiple attributes per row, attributes are simply given as headers and values are stored in related cells.
Example dataset:
```csv
id;scenario;name;efficiency;loss_rate;other
2;ALL;liion;0.9;0.1;AD
```
Here, if present in target element `liion`, attributes `efficiency`, `loss_rate` and `other` would be applied.

**Sequence Datasets**

Sequence datasets can either be applied column- or rowwise to matching sequence data.

In the case of columnwise given data. Each column given a header, which is also present in the target sequence, is transferred to the target sequence.
Example dataset:
```csv
"timeindex";"efficiency";"loss_rate"
"2016-01-01 00:00:00";1;2
"2016-01-01 01:00:00";2;2
"2016-01-01 02:00:00";3;2
```
Here, if present in target sequence, columns `efficiency` and `loss_rate` would be transfered.

In the case of rowwise given data, timeseries data is stored in a cell referenced by `series_col` (defaults to `series`) and
transferred to target sequence column referenced in `var_name_col` (defaults to `var_name`).
Example dataset:
```csv
id_ts;scenario_key;name;var_name;carrier;region;tech;type;timeindex_start;timeindex_stop;timeindex_resolution;series;var_unit;source;comment
0;2050-el_eff;;BB-d1-profile;;AD;;;2050-01-01 00:00:00;2050-12-31 23:00:00;h;[4.586600128650665, 4.047, 3.3725000000000005];MW;raw;
```
Here, timeseries stored under `series` is transfered to column `BB-d1-profile` if this column is present in target sequence.

### Example Scenario

```yaml
raw_dir: raw_adlershof

elements:
  - path: scalars/costs.csv
    scenario: high_cost_2050
    scenario_column: scenario_name

sequences:
  - path: time_series/wind_profile.csv
    sequence_name: wind_profile
    scenario: windy_year
```
