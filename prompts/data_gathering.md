I want to build a new module in pacakge oemof_pipe which gets names of one or multiple datapackages.
From each datapackage, it shall scan the components (rows) of each element CSV in folder data/elements.
For each component it shall transpose the columns and bring it into format like [single.csv](../raw/single.csv) where:
- scenario is name of the datapackage
- name is name of the component
- type column is neglegcted
- all other columns are transposed so that column name becomes var_name and value becomes var_value
- carrier, region and tech column shall be copied
- var_unit, source and comment columns shall be empty
