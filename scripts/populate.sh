#!/bin/bash

# Get directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Default to localhost if not set
export DATABASE_URL=${DATABASE_URL:-"postgresql://testuser:testpassword@localhost:54320/gis_dev"}

echo "🚀 Populating demo data in DynaStore..."
python3 "$DIR/populate_demo_data.py" populate
echo "✅ Population complete!"
