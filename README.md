# Article Data Processing Script

## Overview

This Python script processes article numbers, sends them to a third-party API along with a Data Supplier ID, and stores the returned structured data into separate CSV files.

The returned data includes:

* Brands
* Articles
* References
* Vehicles
* Attributes

## How It Works

1. The script retrieves all article numbers from the source data.
2. Each article number is sent one by one to the third-party API.
3. The API response is parsed and categorized.
4. Data is saved into multiple CSV files based on its type.

## Project Structure

```
project/
│
├── main.py                 # Main script entry point
├── techdoc_export.py      # Processes and structures API response
├── output/
│   ├── brands.csv
│   ├── articles.csv
│   ├── references.csv
│   ├── vehicles.csv
│   └── attributes.csv
└── README.md
```

## Requirements

* Python 3.8+
* requests
* pandas (optional, if used for CSV handling)

Install dependencies:

```bash
pip install requests pandas
```

## Configuration

Update the following values in the script before running:

* API Endpoint
* Data Supplier ID
* Input source for article numbers

## How to Run

```bash
python main.py
```

## Output

After execution, the script generates CSV files inside the `output/` folder:

* `brands.csv`
* `articles.csv`
* `references.csv`
* `vehicles.csv`
* `attributes.csv`

## Notes

* Each article is processed individually to ensure accurate API responses.
* The script is designed for structured data extraction and separation.
* Ensure stable internet connection for API calls.

## Author

Generated for internal data processing workflow.
