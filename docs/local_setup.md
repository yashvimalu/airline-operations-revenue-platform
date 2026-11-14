# Local Development Setup Guide

## Airline Operations & Revenue Intelligence Platform

This guide explains how to configure the local development environment, prepare reference datasets, generate synthetic flight operations data, and execute automated tests.

**Environment:** Windows PowerShell  
**Python:** Python 3.11 recommended  
**Java:** JDK 17  
**Spark:** PySpark 3.5.3

## 1. Prerequisites

Install the following:

- Python 3.11 and pip
- Java Development Kit (JDK) 17
- Git
- A Python IDE such as VS Code or PyCharm

Verify installations:

```powershell
python --version
java -version
git --version
```

Ensure Java is accessible through `JAVA_HOME` and `PATH` if required by your local Spark installation.

## 2. Clone the Repository

```powershell
git clone <your-repository-url>
cd airline-operations-revenue-platform
```

Replace the placeholder with the actual repository URL.

## 3. Create the Python Virtual Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Verify that the virtual environment is active:

```powershell
python -c "import sys; print(sys.executable)"
```

The interpreter should point to the `.venv` directory.

## 4. Install Dependencies

If the project has a maintained `requirements.txt`, install dependencies using:

```powershell
python -m pip install -r requirements.txt
```

The local workflow requires PyYAML, pytest, and PySpark. Coverage reporting additionally requires pytest-cov.

If these dependencies are not yet declared in `requirements.txt`, add them before treating the setup as reproducible.

## 5. Project Configuration

Development settings are maintained in:

`config/dev.yaml`

The configuration controls:

- Environment name
- Synthetic flight record count
- Random seed
- Generation date range
- Airport reference data path
- Raw and processed data paths
- Spark execution settings

The generator currently supports CSV output.

Do not commit credentials or secrets to YAML configuration files.

## 6. Prepare Airport Reference Data

Download the OurAirports reference dataset:

```powershell
Invoke-WebRequest `
  -Uri "https://davidmegginson.github.io/ourairports-data/airports.csv" `
  -OutFile "$env:TEMP\airports.csv"
```

Prepare the filtered airport dataset:

```powershell
python -m ingestion.sources.prepare_airports "$env:TEMP\airports.csv"
```

Expected output file:

`data/sample/airports_reference.csv`

This dataset supplies real airport identifiers and geographic coordinates used by the synthetic flight generator.

## 7. Generate Flight Operations Data

From the project root, execute:

```powershell
python -m generators.flight_generator
```

The generator reads configuration through `scripts.config_loader`, loads airport reference data, and writes synthetic flight records.

Expected output file:

`data/raw/flights.csv`

The number of generated records depends on the `flight_count` setting in `config/dev.yaml`.

## 8. Validate Generated Data

Load the generated CSV in PowerShell:

```powershell
$flights = Import-Csv data/raw/flights.csv
$flights.Count
```

Inspect sample records:

```powershell
$flights | Select-Object -First 5 | Format-Table
```

Check flight statuses:

```powershell
$flights |
    Group-Object flight_status |
    Select-Object Name, Count
```

Check uniqueness:

```powershell
($flights.flight_id | Sort-Object -Unique).Count
```

The total record count should match the number of unique flight IDs.

## 9. Execute Automated Tests

Run all available unit tests:

```powershell
python -m pytest tests/unit -v
```

Run flight generator tests separately:

```powershell
python -m pytest tests/unit/test_flight_generator.py -v
```

Measure generator coverage:

```powershell
python -m pytest tests/unit/test_flight_generator.py `
    --cov=generators.flight_generator `
    --cov-report=term-missing
```

## 10. Verify Local Spark

Run the Spark smoke test:

```powershell
python spark/jobs/spark_smoke_test.py
```

This verifies that the local Spark environment can initialize and execute a basic workload.

## 11. Troubleshooting

### ModuleNotFoundError

Run Python modules from the repository root:

```powershell
python -m generators.flight_generator
```

Avoid launching the generator directly by its file path when imports depend on the project root.

### Missing Configuration File

Confirm that `config/dev.yaml` exists and that the configuration loader resolves its location correctly.

### Missing Airport Reference Data

Verify that `data/sample/airports_reference.csv` exists. If it does not, repeat the reference data preparation step.

### PySpark Java Errors

Confirm that Java 17 is installed and available to Spark.

```powershell
java -version
```

Review `JAVA_HOME` if Spark cannot locate Java.

### Python Worker Connection Errors

Verify that the active Python interpreter is compatible with the installed PySpark version. Use Python 3.11 with PySpark 3.5.3 for this documented local environment.

### Test Import Errors

Run tests from the repository root with:

```powershell
python -m pytest tests/unit -v
```

Confirm that imported functions exist in the expected modules.

## 12. Generated Files and Version Control

Generated flight datasets in `data/raw/` should not be committed to Git.

The filtered airport reference file in `data/sample/` may be version-controlled if the repository's `.gitignore` rules permit it.

Keep configuration files, source code, data contracts, tests, and documentation under version control.

## 13. Development Workflow

1. Update development configuration.
2. Prepare airport reference data when necessary.
3. Generate synthetic flight records.
4. Validate generated data.
5. Execute unit tests.
6. Run the Spark smoke test.
7. Commit verified source and documentation changes.

The project will later extend this local workflow to GCS, Dataproc, BigQuery, and Cloud Composer.