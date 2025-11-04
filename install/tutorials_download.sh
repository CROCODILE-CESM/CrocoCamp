#!/usr/bin/env bash

set -euo pipefail

echo "Downloading datasets for tutorial notebooks..."
echo "    Downloading CrocoLake - PHY..."
download_db -t PHY -d CrocoLake --destination ../CrocoLake/
echo "    downloaded."
echo "done."
