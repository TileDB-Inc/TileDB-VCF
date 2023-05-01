# Quarto Documentation

This directory contains the source for the Quarto documentation. The documentation is built using the [Quarto](https://quarto.org) document format and toolchain.


## Execute Notebooks

> **NOTE** - Executing the example notebooks is not required to render the documentation. Rendering will use the pre-rendered notebook outputs in the `examples` directory.

Jupyter notebooks are located in the `examples` directory in the root of the repository. 

Executing the notebooks requires a Python environment with all notebook dependencies installed. Run the following command to add the notebook dependencies to an existing `tiledbvcf-py` Python environment:

```
pip install -r requirements-doc.txt
```

Run the following command from the root of the repository to execute the example notebooks and update the contents of the notebook cell outputs:
```
make notebooks
```

> **NOTE** - The example notebooks run task graphs on TileDB Cloud, which requires a TileDB Cloud account and a valid API key.


## Render Documentation
Run the following command from the root of the repository to render the documentation:

```
make docs
```
