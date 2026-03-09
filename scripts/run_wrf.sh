#!/bin/bash
# =============================================================================
# WRF Execution Script
# =============================================================================
# Runs the complete WRF chain: real.exe -> wrf.exe
# =============================================================================

set -e

# Get script directory and load utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/utils.sh"

# Load configuration
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
source <(tr -d '\r' < "${CONFIG_FILE:-$PROJECT_DIR/config.env}")

# Fix paths: process substitution breaks BASH_SOURCE -> PROJECT_DIR=/dev/fd
export PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
export GFS_DATA_DIR="${PROJECT_DIR}/GFS_DATA"
export WORKSPACE_DIR="${PROJECT_DIR}/workspace"
export OUTPUT_DIR="${PROJECT_DIR}/workspace/output"
export LOG_FILE="${PROJECT_DIR}/workspace/logs/wrf_forecast.log"

# Set workspaces
WPS_WORKSPACE="${WORKSPACE_DIR}/wps"
WRF_WORKSPACE="${WORKSPACE_DIR}/wrf"

# =============================================================================
log_step "WRF EXECUTION - WRF-FWI Portugal"
# =============================================================================

echo "Configuration:"
echo "  WRF_DIR:       $WRF_DIR"
echo "  WPS Workspace: $WPS_WORKSPACE"
echo "  WRF Workspace: $WRF_WORKSPACE"
echo "  Output Dir:    $OUTPUT_DIR"
echo "  Processors:    ${NUM_PROCESSORS:-1}"
echo "  Use MPI:       ${USE_MPI:-false}"
echo ""

# -----------------------------------------------------------------------------
# Step 0: Verify installations and prerequisites
# -----------------------------------------------------------------------------
log_step "Step 0: Verifying installations"

check_wrf_installation "$WRF_DIR" || exit 1

# Check for met_em files
MET_COUNT=$(ls "$WPS_WORKSPACE"/met_em.d*.nc 2>/dev/null | wc -l)
if [[ $MET_COUNT -eq 0 ]]; then
    log_error "No met_em files found in $WPS_WORKSPACE"
    log_info "Run WPS first: ./scripts/run_wps.sh"
    exit 1
fi
log_success "Found $MET_COUNT met_em files"

# -----------------------------------------------------------------------------
# Step 1: Setup workspace
# -----------------------------------------------------------------------------
log_step "Step 1: Setting up workspace"

mkdir -p "$WRF_WORKSPACE"
mkdir -p "$OUTPUT_DIR"
cd "$WRF_WORKSPACE"

# Clean previous runs if requested
if [[ "${CLEAN_WORKSPACE:-false}" == "true" ]]; then
    clean_wrf_workspace "$WRF_WORKSPACE"
fi

# Link WRF files
link_wrf_files "$WRF_DIR" "$WRF_WORKSPACE"

# Link met_em files
link_met_em_files "$WPS_WORKSPACE" "$WRF_WORKSPACE"

# Copy namelist (should already be generated)
if [[ -f "$WRF_WORKSPACE/namelist.input" ]]; then
    log_info "Using existing namelist.input"
elif [[ -f "$PROJECT_DIR/workspace/wrf/namelist.input" ]]; then
    cp "$PROJECT_DIR/workspace/wrf/namelist.input" .
    log_info "Copied namelist.input from workspace"
else
    log_error "namelist.input not found. Run generate_namelists.py first."
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 2: Run real.exe
# -----------------------------------------------------------------------------
log_step "Step 2: Running real.exe"

START_TIME=$(date +%s)

if [[ "${USE_MPI:-false}" == "true" ]] && command -v mpirun &> /dev/null; then
    log_info "Running real.exe with MPI (${NUM_PROCESSORS:-1} processors)"
    mpirun -np ${NUM_PROCESSORS:-1} ./real.exe >& log.real
else
    log_info "Running real.exe (serial)"
    ./real.exe >& log.real
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# Check for success
if [[ -f "rsl.error.0000" ]]; then
    check_wrf_success "rsl.error.0000" "real.exe" || exit 1
else
    # Check log file instead
    if grep -q "SUCCESS" log.real 2>/dev/null || \
       [[ -f "wrfinput_d01" && -f "wrfbdy_d01" ]]; then
        log_success "real.exe completed"
    else
        log_error "real.exe may have failed. Check log.real"
        tail -30 log.real
        exit 1
    fi
fi

log_info "real.exe completed in $(format_duration $DURATION)"

# Verify output files
for dom in $(seq 1 ${MAX_DOM:-1}); do
    input_file="wrfinput_d$(printf '%02d' $dom)"
    if [[ -f "$input_file" ]]; then
        log_success "Created $input_file"
    else
        log_error "Missing $input_file"
        exit 1
    fi
done

if [[ -f "wrfbdy_d01" ]]; then
    log_success "Created wrfbdy_d01"
else
    log_error "Missing wrfbdy_d01"
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 3: Run wrf.exe
# -----------------------------------------------------------------------------
log_step "Step 3: Running wrf.exe"

log_warning "This may take a long time depending on domain size and duration..."

START_TIME=$(date +%s)

if [[ "${USE_MPI:-false}" == "true" ]] && command -v mpirun &> /dev/null; then
    log_info "Running wrf.exe with MPI (${NUM_PROCESSORS:-1} processors)"
    mpirun -np ${NUM_PROCESSORS:-1} ./wrf.exe >& log.wrf &
    WRF_PID=$!
else
    log_info "Running wrf.exe (serial)"
    ./wrf.exe >& log.wrf &
    WRF_PID=$!
fi

# Monitor progress
log_info "WRF running with PID: $WRF_PID"
log_info "Monitoring progress (check rsl.error.0000 or rsl.out.0000 for details)..."

# Wait for completion with periodic status updates
while kill -0 $WRF_PID 2>/dev/null; do
    sleep 60
    ELAPSED=$(($(date +%s) - START_TIME))
    
    # Show latest timing info from RSL file
    if [[ -f "rsl.error.0000" ]]; then
        LATEST=$(grep "Timing for main" rsl.error.0000 2>/dev/null | tail -1)
        if [[ -n "$LATEST" ]]; then
            echo -e "${CYAN}[$(format_duration $ELAPSED)]${NC} $LATEST"
        fi
    fi
done

# Get exit status
wait $WRF_PID
WRF_EXIT=$?

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

if [[ $WRF_EXIT -ne 0 ]]; then
    log_error "wrf.exe exited with code $WRF_EXIT"
fi

# Check for success
if [[ -f "rsl.error.0000" ]]; then
    check_wrf_success "rsl.error.0000" "wrf.exe" || exit 1
else
    if grep -q "SUCCESS" log.wrf 2>/dev/null; then
        log_success "wrf.exe completed"
    else
        log_error "wrf.exe may have failed. Check log.wrf"
        tail -30 log.wrf
        exit 1
    fi
fi

log_info "wrf.exe completed in $(format_duration $DURATION)"

# -----------------------------------------------------------------------------
# Step 4: Collect output files
# -----------------------------------------------------------------------------
log_step "Step 4: Collecting output files"

# Move output files to output directory
OUTPUT_COUNT=0
for dom in $(seq 1 ${MAX_DOM:-1}); do
    for outfile in wrfout_d$(printf '%02d' $dom)_*; do
        if [[ -f "$outfile" ]]; then
            mv "$outfile" "$OUTPUT_DIR/"
            ((OUTPUT_COUNT++))
        fi
    done
done

log_success "Moved $OUTPUT_COUNT output files to $OUTPUT_DIR"

# -------------------------------------------------------------------------
# Rename output files to reflect forecast offset if START_DATE used hour>=24
# E.g. wrfout_d01_2026-02-17_00:00:00 -> wrfout_d01_2026-02-16_24:00:00
# This preserves the user's original offset-based naming convention.
# -------------------------------------------------------------------------
orig_sd="${ORIGINAL_START_DATE:-$START_DATE}"
orig_date_part="${orig_sd%%_*}"
orig_time_part="${orig_sd#*_}"
orig_hour="${orig_time_part%%:*}"

if [[ "$orig_hour" -ge 24 ]]; then
    log_info "Renaming output files to reflect forecast offset (base: ${orig_date_part}, hour offset: ${orig_hour}+)"
    base_epoch=$(date -d "${orig_date_part} 00:00:00" +%s)

    for outfile in "$OUTPUT_DIR"/wrfout_d*; do
        [[ -f "$outfile" ]] || continue
        fname=$(basename "$outfile")

        # Extract domain part (e.g. d01) and datetime from filename
        # Format: wrfout_d01_2026-02-17_00:00:00
        domain=$(echo "$fname" | grep -oP 'd\d+')
        file_datetime=$(echo "$fname" | sed 's/wrfout_d[0-9]*_//')
        file_datetime_parsed="${file_datetime//_/ }"

        # Calculate offset hours from the original base date midnight
        file_epoch=$(date -d "$file_datetime_parsed" +%s)
        offset_seconds=$((file_epoch - base_epoch))
        offset_hours=$((offset_seconds / 3600))
        offset_minutes=$(( (offset_seconds % 3600) / 60 ))
        offset_secs=$((offset_seconds % 60))

        new_name="wrfout_${domain}_${orig_date_part}_$(printf '%03d' $offset_hours):$(printf '%02d' $offset_minutes):$(printf '%02d' $offset_secs)"

        if [[ "$fname" != "$new_name" ]]; then
            mv "$OUTPUT_DIR/$fname" "$OUTPUT_DIR/$new_name"
            log_info "  Renamed: $fname -> $new_name"
        fi
    done
fi

# List outputs
echo ""
echo "Output files:"
ls -lh "$OUTPUT_DIR"/wrfout_* 2>/dev/null | head -20

# =============================================================================
log_step "WRF COMPLETED SUCCESSFULLY"
# =============================================================================

echo ""
echo "📊 Summary:"
echo "   Output directory: $OUTPUT_DIR"
echo "   Output files:     $(ls "$OUTPUT_DIR"/wrfout_* 2>/dev/null | wc -l)"
echo "   Total runtime:    $(format_duration $DURATION)"
echo ""
echo "🔥 Next steps for FWI calculation:"
echo "   1. Extract variables: T2, Q2, U10, V10, RAINNC, RAINC, PSFC"
echo "   2. Calculate relative humidity from Q2 and PSFC"
echo "   3. Calculate wind speed from U10 and V10"
echo "   4. Compute FWI indices"
echo ""
