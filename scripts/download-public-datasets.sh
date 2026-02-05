#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
DEST_DIR="$REPO_ROOT/testdata/public-datasets"

download_file() {
    local url="$1"
    local dest="$2"

    if [[ -f "$dest" ]]; then
        echo "  Already exists: $(basename "$dest")"
        return
    fi

    echo "  Downloading: $(basename "$dest")"
    curl -fSL "$url" -o "$dest"
}

# =============================================================================
# NYC Taxi Dataset
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# =============================================================================
download_nyc_taxi() {
    local mode="$1"
    local base_url="https://d37ci6vzurychx.cloudfront.net/trip-data"
    local dest="$DEST_DIR/nyc-taxi"
    mkdir -p "$dest"

    echo "=== NYC Taxi Dataset ==="

    local small_files=(
        "green_tripdata_2025-10.parquet"
        "fhv_tripdata_2025-10.parquet"
    )

    local big_files=(
        "yellow_tripdata_2025-10.parquet"
        "fhvhv_tripdata_2025-10.parquet"
    )

    echo "Downloading small files..."
    for file in "${small_files[@]}"; do
        download_file "$base_url/$file" "$dest/$file"
    done

    if [[ "$mode" == "all" ]]; then
        echo "Downloading big files..."
        for file in "${big_files[@]}"; do
            download_file "$base_url/$file" "$dest/$file"
        done
    fi
}

# =============================================================================
# ClickBench Dataset
# Source: https://github.com/ClickHouse/ClickBench
# =============================================================================
download_clickbench() {
    local mode="$1"
    local base_url="https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
    local dest="$DEST_DIR/clickbench"
    mkdir -p "$dest"

    echo "=== ClickBench Dataset ==="

    if [[ "$mode" == "all" ]]; then
        echo "Downloading ClickBench partitioned files (CI only)..."
        for i in 0 1 2; do
            download_file "$base_url/hits_$i.parquet" "$dest/hits_$i.parquet"
        done
    else
        echo "Skipping ClickBench (CI only, use --all to download)"
    fi
}

# =============================================================================
# Add more datasets here following the same pattern
# =============================================================================

usage() {
    echo "Usage: $0 [--small|--all]"
    echo ""
    echo "Download public Parquet datasets for testing."
    echo ""
    echo "Options:"
    echo "  --small    Download only small files (default)"
    echo "  --all      Download all files including large ones"
    exit 0
}

MODE="small"

while [[ $# -gt 0 ]]; do
    case $1 in
        --small)
            MODE="small"
            shift
            ;;
        --all)
            MODE="all"
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

mkdir -p "$DEST_DIR"

download_nyc_taxi "$MODE"
download_clickbench "$MODE"

echo ""
echo "Done!"
