# Public Datasets

This directory contains public Parquet datasets for testing. Files are downloaded
during CI or manually using the download script.

## Directory Structure

```
public-datasets/
├── nyc-taxi/           # NYC Taxi trip data
│   ├── green_tripdata_2025-10.parquet
│   ├── fhv_tripdata_2025-10.parquet
│   ├── yellow_tripdata_2025-10.parquet (CI only)
│   └── fhvhv_tripdata_2025-10.parquet (CI only)
├── tpch-sf1/           # TPC-H benchmark data (SF1)
│   ├── nation.parquet
│   ├── region.parquet
│   ├── supplier.parquet
│   ├── lineitem.parquet (CI only)
│   ├── orders.parquet (CI only)
│   ├── customer.parquet (CI only)
│   ├── part.parquet (CI only)
│   └── partsupp.parquet (CI only)
├── clickbench/         # ClickBench web analytics data (CI only)
│   ├── hits_0.parquet
│   ├── hits_1.parquet
│   └── hits_2.parquet
└── <future-dataset>/   # More datasets can be added
```

## Datasets

### NYC Taxi Data

Source: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

| File | Size | CI Only |
|------|------|---------|
| `green_tripdata_2025-10.parquet` | ~1MB | No |
| `fhv_tripdata_2025-10.parquet` | ~25MB | No |
| `yellow_tripdata_2025-10.parquet` | ~50MB | Yes |
| `fhvhv_tripdata_2025-10.parquet` | ~400MB | Yes |

### TPC-H SF1

Source: [TPC-H Benchmark](https://www.tpc.org/tpch/) (generated via DuckDB's TPC-H extension)

The de facto standard benchmark for analytical database systems. 8 tables with a
normalized supply chain schema. Provides diverse types including DECIMAL(15,2) and
DATE columns, plus 49 row groups in the lineitem table.

| File | Size | CI Only |
|------|------|---------|
| `nation.parquet` | ~2KB | No |
| `region.parquet` | ~1KB | No |
| `supplier.parquet` | ~771KB | No |
| `lineitem.parquet` | ~197MB | Yes |
| `orders.parquet` | ~53MB | Yes |
| `customer.parquet` | ~12MB | Yes |
| `part.parquet` | ~6MB | Yes |
| `partsupp.parquet` | ~40MB | Yes |

### ClickBench

Source: [ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench)

Real-world web analytics data with 105 columns covering diverse types. The dataset
is "intentionally dirty" with no bloom filters or proper logical types, making it
excellent for stress-testing edge cases.

| File | Size | CI Only |
|------|------|---------|
| `hits_0.parquet` | ~150MB | Yes |
| `hits_1.parquet` | ~150MB | Yes |
| `hits_2.parquet` | ~150MB | Yes |

## Download

Run the download script from the repository root:

```bash
# Download small files only (for local testing)
./scripts/download-public-datasets.sh

# Download all files including large ones (for CI)
./scripts/download-public-datasets.sh --all
```

## Adding New Datasets

1. Add a new `download_<dataset>()` function to `scripts/download-public-datasets.sh`
2. Call it from the main section of the script
3. Create corresponding tests in `src/public_datasets_testing.zig`
4. Update this README with dataset documentation
