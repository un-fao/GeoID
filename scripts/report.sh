#!/bin/bash

# ==============================================================================
# CONFIGURATION
# ==============================================================================
HTML_FILE="Engineering_Report_$(date +%Y-%m-%d).html"

# Detect Author and Project
AUTHOR=$(git config user.name)
PROJECT_NAME=${PWD##*/}

# Config files to track for "Dependency/Infra Work"
DEP_PATTERN="package.json|requirements.txt|pom.xml|build.gradle|go.mod|Cargo.toml|Gemfile|Dockerfile|docker-compose.yml|*.csproj|*.sln"

echo "📊 Starting Deep Audit for $AUTHOR in $PROJECT_NAME..."

# ==============================================================================
# 1. TIME & EFFORT METRICS (Man-Days Calculator)
# ==============================================================================
echo "   - Calculating estimated man-hours..."

# Get all commit timestamps for the author (Unix epoch)
# Logic: Group by Day. For each day, Time = (LastCommit - FirstCommit) + 1 Hour Buffer.
# This accounts for the span of work, not just the instant of the commit.
WORK_STATS=$(git log --author="$AUTHOR" --format="%at" --no-merges | sort -n | awk '
BEGIN {
    total_seconds = 0
    active_days = 0
    current_day = -1
    day_start = 0
    day_end = 0
}
{
    # Convert epoch to day number (integer division by 86400 seconds)
    day = int($1 / 86400)
    
    if (day != current_day) {
        if (current_day != -1) {
            # Add duration of previous day + 1 hour buffer for context switching
            total_seconds += (day_end - day_start) + 3600
        }
        current_day = day
        day_start = $1
        day_end = $1
        active_days++
    } else {
        day_end = $1
    }
}
END {
    # Add the last day
    if (current_day != -1) {
        total_seconds += (day_end - day_start) + 3600
    }
    
    hours = int(total_seconds / 3600)
    man_days = hours / 8
    printf "%d %d %d", active_days, hours, man_days
}
')

ACTIVE_DAYS=$(echo $WORK_STATS | awk '{print $1}')
EST_HOURS=$(echo $WORK_STATS | awk '{print $2}')
MAN_DAYS=$(echo $WORK_STATS | awk '{print $3}')

# ==============================================================================
# 2. CODEBASE GROWTH (Cumulative History)
# ==============================================================================
echo "   - Generating codebase growth history..."

# Generate data points: YYYY-MM, NetLinesAdded
# We use a temporary file to store monthly deltas
GROWTH_TMP=$(mktemp)
git log --author="$AUTHOR" --pretty=tformat:"%ad" --date=format:"%Y-%m" --numstat --no-merges | \
awk 'NF==3 {sum[$1] += ($2 - $3)} END {for (d in sum) print d, sum[d]}' | sort > "$GROWTH_TMP"

# Convert to JS Arrays for Chart (Cumulative Sum)
GROWTH_LABELS=""
GROWTH_DATA=""
CUMULATIVE=0

while read date delta; do
    CUMULATIVE=$((CUMULATIVE + delta))
    GROWTH_LABELS+="'$date',"
    GROWTH_DATA+="$CUMULATIVE,"
done < "$GROWTH_TMP"
rm "$GROWTH_TMP"

# ==============================================================================
# 3. FULL COMMIT HISTORY (Monthly Velocity)
# ==============================================================================
echo "   - analyzing full commit history..."
HISTORY_LABELS=""
HISTORY_DATA=""
# Get commits per month
while read count month; do
    HISTORY_LABELS="'$month', $HISTORY_LABELS"
    HISTORY_DATA="$count, $HISTORY_DATA"
done < <(git log --author="$AUTHOR" --date=format:'%Y-%m' --pretty=format:'%ad' | sort | uniq -c)

# ==============================================================================
# 4. DEPENDENCY & INFRASTRUCTURE CHURN
# ==============================================================================
echo "   - analyzing infrastructure maintenance..."
DEP_CHANGES=$(git log --author="$AUTHOR" --oneline --name-only | grep -E "$DEP_PATTERN" | wc -l)

# ==============================================================================
# 5. CONTRIBUTORS TABLE
# ==============================================================================
CONTRIBUTORS_HTML=""
while read count name; do
    CONTRIBUTORS_HTML+="<tr><td>$name</td><td style='text-align:right'>$count</td></tr>"
done < <(git shortlog -sn --all | head -n 5)

# ==============================================================================
# 6. FILE EXTENSION DISTRIBUTION (Cleaned)
# ==============================================================================
echo "   - Cleaning file statistics..."
# Get raw counts
RAW_EXT=$(git log --author="$AUTHOR" --pretty="format:" --name-only | sed 's/.*\.//' | sort | uniq -c | sort -nr)

# Top 5 extensions, group rest as "Other"
FT_LABELS=""
FT_DATA=""
OTHER_COUNT=0
COUNTER=0

while read count ext; do
    if [ -z "$ext" ]; then continue; fi
    if [ $COUNTER -lt 5 ]; then
        FT_LABELS+="'$ext',"
        FT_DATA+="$count,"
        ((COUNTER++))
    else
        OTHER_COUNT=$((OTHER_COUNT + count))
    fi
done <<< "$RAW_EXT"

if [ $OTHER_COUNT -gt 0 ]; then
    FT_LABELS+="'Others',"
    FT_DATA+="$OTHER_COUNT,"
fi

# ==============================================================================
# 7. HOURLY DISTRIBUTION
# ==============================================================================
HOURS_DATA=""
for i in {0..23}; do
    count=$(git log --author="$AUTHOR" --format='%H' | grep -c "^$(printf "%02d" $i)")
    HOURS_DATA+="$count,"
done

# Standard Stats
TOTAL_COMMITS=$(git rev-list --count --author="$AUTHOR" --all --no-merges HEAD)
LINES_ADDED=$(git log --author="$AUTHOR" --pretty=tformat: --numstat | awk '{ add += $1 } END { print add }')
LINES_REMOVED=$(git log --author="$AUTHOR" --pretty=tformat: --numstat | awk '{ subs += $2 } END { print subs }')
NET_IMPACT=$((LINES_ADDED - LINES_REMOVED))

# ==============================================================================
# HTML GENERATION
# ==============================================================================
echo "🎨 Generating Advanced Dashboard..."

cat <<EOF > "$HTML_FILE"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Engineering Report - $PROJECT_NAME</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --bg: #0b1120;
            --card-bg: #1e293b;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --accent: #38bdf8;
            --success: #4ade80;
            --danger: #f87171;
            --warning: #fbbf24;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg);
            color: var(--text-main);
            margin: 0;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        
        /* Header */
        .header { display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 40px; padding-bottom: 20px; border-bottom: 1px solid #334155; }
        .header h1 { margin: 0; font-size: 2.5rem; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .meta-tag { background: #334155; padding: 5px 12px; border-radius: 20px; font-size: 0.8rem; color: #cbd5e1; margin-left: 10px; }
        
        /* KPI Grid */
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card { background: var(--card-bg); padding: 25px; border-radius: 16px; border: 1px solid #334155; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.5); }
        .card h3 { margin: 0 0 10px 0; color: var(--text-muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1.2px; }
        .card .value { font-size: 2.2rem; font-weight: 700; margin-bottom: 5px; }
        .card .sub { font-size: 0.85rem; opacity: 0.7; }
        
        /* Charts Layout */
        .row { display: grid; grid-template-columns: 2fr 1fr; gap: 20px; margin-bottom: 20px; }
        .row.reverse { grid-template-columns: 1fr 2fr; }
        .chart-box { background: var(--card-bg); padding: 20px; border-radius: 16px; border: 1px solid #334155; height: 400px; position: relative; }
        .full-width { grid-column: 1 / -1; }
        
        /* Contributors Table */
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #334155; font-size: 0.9rem; }
        th { color: var(--text-muted); font-weight: 600; }
        
        @media (max-width: 1000px) { .row, .row.reverse { grid-template-columns: 1fr; } }
        @media print { body { background: white; color: black; } .card, .chart-box { background: white; border: 1px solid #ddd; } }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1>Engineering Audit Report</h1>
                <div style="margin-top: 10px; display: flex; align-items: center;">
                    <span style="font-size: 1.2rem; font-weight: 500;">$PROJECT_NAME</span>
                    <span class="meta-tag">Generated $(date +%Y-%m-%d)</span>
                    <span class="meta-tag">Author: $AUTHOR</span>
                </div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 0.9rem; color: var(--text-muted);">Audit Scope</div>
                <div style="font-weight: bold;">Full Project History</div>
            </div>
        </div>

        <!-- Section 1: Effort & Volume -->
        <div class="kpi-grid">
            <div class="card">
                <h3>Est. Man-Hours</h3>
                <div class="value" style="color: var(--accent)">$EST_HOURS h</div>
                <div class="sub">~$MAN_DAYS Man-Days (@8h)</div>
            </div>
            <div class="card">
                <h3>Active Days</h3>
                <div class="value">$ACTIVE_DAYS</div>
                <div class="sub">Days with code pushes</div>
            </div>
            <div class="card">
                <h3>Total Commits</h3>
                <div class="value">$TOTAL_COMMITS</div>
                <div class="sub">Discrete units of work</div>
            </div>
            <div class="card">
                <h3>Net Growth</h3>
                <div class="value" style="color: var(--success)">+$NET_IMPACT</div>
                <div class="sub">Lines of Code (Net)</div>
            </div>
        </div>

        <!-- Section 2: Timeline & Growth -->
        <div class="row">
            <div class="chart-box">
                <canvas id="growthChart"></canvas>
            </div>
            <div class="chart-box">
                <canvas id="historyChart"></canvas>
            </div>
        </div>

        <!-- Section 3: Tech Stack & Infra -->
        <div class="row reverse">
            <div class="chart-box">
                <h3>Top Contributors</h3>
                <table>
                    <thead><tr><th>User</th><th style="text-align:right">Commits</th></tr></thead>
                    <tbody>
                        $CONTRIBUTORS_HTML
                    </tbody>
                </table>
                <div style="margin-top: 30px; padding: 15px; background: rgba(56, 189, 248, 0.1); border-radius: 8px;">
                    <h4 style="margin: 0 0 5px 0; color: var(--accent);">Infrastructure Stats</h4>
                    <p style="margin: 0; font-size: 0.9rem; opacity: 0.8;">
                        Config/Dependency files modified <strong>$DEP_CHANGES times</strong>.
                        <br>
                        (package.json, docker, build files, etc.)
                    </p>
                </div>
            </div>
            <div class="chart-box">
                <canvas id="pieChart"></canvas>
            </div>
        </div>

        <!-- Section 4: Work Habits -->
        <div class="chart-box full-width" style="height: 300px;">
            <canvas id="hoursChart"></canvas>
        </div>
        
        <div style="text-align: center; margin-top: 40px; opacity: 0.5; font-size: 0.8rem;">
            CONFIDENTIAL - GENERATED BY AUTOMATED GIT AUDIT TOOL
        </div>
    </div>

    <script>
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.borderColor = '#334155';
        Chart.defaults.font.family = "'Segoe UI', sans-serif";

        // 1. Codebase Growth (Line Chart) - THE "GROC" Chart
        new Chart(document.getElementById('growthChart'), {
            type: 'line',
            data: {
                labels: [$GROWTH_LABELS],
                datasets: [{
                    label: 'Cumulative Lines of Code (Growth)',
                    data: [$GROWTH_DATA],
                    borderColor: '#4ade80',
                    backgroundColor: 'rgba(74, 222, 128, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { 
                    title: { display: true, text: 'Codebase Growth Trajectory', align: 'start', font: {size: 16, weight: 'bold'} },
                    tooltip: { mode: 'index', intersect: false }
                },
                scales: { y: { beginAtZero: true } }
            }
        });

        // 2. Full History (Bar Chart)
        new Chart(document.getElementById('historyChart'), {
            type: 'bar',
            data: {
                labels: [$HISTORY_LABELS],
                datasets: [{
                    label: 'Commits / Month',
                    data: [$HISTORY_DATA],
                    backgroundColor: '#38bdf8',
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { title: { display: true, text: 'Monthly Velocity (Full History)', align: 'start', font: {size: 14} } },
                scales: { x: { display: false } } // Hide dense labels
            }
        });

        // 3. Tech Stack (Pie Chart)
        new Chart(document.getElementById('pieChart'), {
            type: 'doughnut',
            data: {
                labels: [$FT_LABELS],
                datasets: [{
                    data: [$FT_DATA],
                    backgroundColor: ['#38bdf8', '#818cf8', '#c084fc', '#f472b6', '#fb7185', '#94a3b8'],
                    borderWidth: 0,
                    hoverOffset: 10
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '60%',
                plugins: { 
                    title: { display: true, text: 'Tech Stack Distribution (Files)', align: 'start', font: {size: 16} },
                    legend: { position: 'right', labels: { usePointStyle: true, boxWidth: 10 } }
                }
            }
        });

        // 4. Hours (Bar)
        new Chart(document.getElementById('hoursChart'), {
            type: 'bar',
            data: {
                labels: ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23'],
                datasets: [{
                    label: 'Commit Frequency by Hour',
                    data: [$HOURS_DATA],
                    backgroundColor: (ctx) => {
                        const v = ctx.raw;
                        return v > 5 ? '#f472b6' : '#38bdf8'; // Highlight overtime
                    },
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { title: { display: true, text: 'Work Intensity by Hour (24h)', align: 'start', font: {size: 14} } },
                scales: { x: { grid: { display: false } } }
            }
        });
    </script>
</body>
</html>
EOF

echo "✅ Generated Enhanced Report: $HTML_FILE"

# ==============================================================================
# GOURCE VISUALIZATION (Live or Video)
# ==============================================================================
if command -v gource &> /dev/null; then
    echo ""
    echo "========================================================"
    echo "🎥 GOURCE VISUALIZATION"
    echo "========================================================"
    echo "1) View Live (Quick Preview)"
    echo "2) Render to MP4 Video (Requires ffmpeg, High Quality)"
    echo "3) Skip"
    read -p "Select option [1-3]: " -n 1 -r REPLY_OPT
    echo ""

    # Gource Settings
    # --seconds-per-day 0.4 (Slower)
    # --font-size 30 (Larger text for visibility)
    # --bloom-intensity 0.4 (Crisper nodes, less blur)
    GOURCE_ARGS=(
        --title "Project Evolution: $PROJECT_NAME"
        --seconds-per-day 0.4
        --auto-skip-seconds 1
        --multi-sampling
        --highlight-users
        --hide filenames
        --file-idle-time 0
        --font-size 30
        --dir-name-depth 3
        --date-format "%Y-%m-%d %H:%M"
        --bloom-multiplier 0.8
        --bloom-intensity 0.4
        --background-colour 0b1120
    )

    if [[ "$REPLY_OPT" == "1" ]]; then
        echo "Launching live view..."
        gource "${GOURCE_ARGS[@]}" --fullscreen &
        
    elif [[ "$REPLY_OPT" == "2" ]]; then
        if command -v ffmpeg &> /dev/null; then
            OUTPUT_VIDEO="project_evolution.mp4"
            echo "🎬 Rendering video to '$OUTPUT_VIDEO'..."
            echo "   (This may take a while. Press Ctrl+C to stop)"
            
            # Pipe Gource PPM output -> FFmpeg H.264
            gource "${GOURCE_ARGS[@]}" -1280x720 -o - | \
            ffmpeg -y -r 60 -f image2pipe -vcodec ppm -i - -vcodec libx264 -preset medium -pix_fmt yuv420p -crf 18 "$OUTPUT_VIDEO"
            
            echo "✅ Video saved successfully: $OUTPUT_VIDEO"
        else
            echo "❌ Error: 'ffmpeg' is not installed. Cannot render video."
            echo "   Launching live view instead..."
            gource "${GOURCE_ARGS[@]}" --fullscreen &
        fi
    fi
else
    echo "ℹ️  Gource not found. Install 'gource' (and 'ffmpeg') to generate the visualization video."
fi