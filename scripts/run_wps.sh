#!/bin/bash
# =============================================================================
# WPS Execution Script
# =============================================================================
# Runs the complete WPS chain: geogrid -> ungrib -> metgrid
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

# Set workspace
WPS_WORKSPACE="${WORKSPACE_DIR:-$PROJECT_DIR/workspace}/wps"

# =============================================================================
log_step "WPS EXECUTION - WRF-FWI Portugal"
# =============================================================================

echo "Configuration:"
echo "  WPS_DIR:        $WPS_DIR"
echo "  GEOG_DATA_PATH: $GEOG_DATA_PATH"
echo "  GFS_DATA_DIR:   $GFS_DATA_DIR"
echo "  Workspace:      $WPS_WORKSPACE"
echo ""

# -----------------------------------------------------------------------------
# Step 0: Verify installations
# -----------------------------------------------------------------------------
log_step "Step 0: Verifying installations"

check_wps_installation "$WPS_DIR" || exit 1
check_geog_data "$GEOG_DATA_PATH" || exit 1
check_directory_exists "$GFS_DATA_DIR" "GFS data directory" || exit 1

# -----------------------------------------------------------------------------
# Step 1: Setup workspace
# -----------------------------------------------------------------------------
log_step "Step 1: Setting up workspace"

mkdir -p "$WPS_WORKSPACE"
cd "$WPS_WORKSPACE"

# Clean previous runs if requested
if [[ "${CLEAN_WORKSPACE:-false}" == "true" ]]; then
    clean_wps_workspace "$WPS_WORKSPACE"
fi


# Link WPS files
link_wps_files "$WPS_DIR" "$WPS_WORKSPACE"

# Copy namelist (should already be generated)
if [[ -f "$WPS_WORKSPACE/namelist.wps" ]]; then
    log_info "Using existing namelist.wps"
elif [[ -f "$PROJECT_DIR/workspace/wps/namelist.wps" ]]; then
    cp "$PROJECT_DIR/workspace/wps/namelist.wps" .
    log_info "Copied namelist.wps from workspace"
else
    log_error "namelist.wps not found. Run generate_namelists.py first."
    exit 1
fi

# Copy Vtable for GFS data (copying is safer than linking across filesystems)
log_info "Copying Vtable for GFS"
if [[ -f "$PROJECT_DIR/templates/Vtable.GFS.custom" ]]; then
    cp "$PROJECT_DIR/templates/Vtable.GFS.custom" Vtable
    log_info "Using custom Vtable from templates"
else
    cp "$WPS_DIR/ungrib/Variable_Tables/Vtable.GFS" Vtable
    log_info "Using standard GFS Vtable"
fi

# -----------------------------------------------------------------------------
# Step 2: Run geogrid
# -----------------------------------------------------------------------------
log_step "Step 2: Running geogrid.exe"

START_TIME=$(date +%s)

# Run geogrid with MPI if available (supports parallel since WPS 4.0)
# Geogrid doesn't scale well beyond 8 cores, so cap it
if [[ "${USE_MPI:-false}" == "true" ]] && command -v mpirun &> /dev/null && [[ ${NUM_PROCESSORS:-1} -gt 1 ]]; then
    GEOGRID_PROCS=$((NUM_PROCESSORS < 8 ? NUM_PROCESSORS : 4))
    log_info "Running geogrid.exe with MPI ($GEOGRID_PROCS processors)"
    mpirun -np $GEOGRID_PROCS ./geogrid.exe >& log.geogrid
else
    log_info "Running geogrid.exe (serial)"
    ./geogrid.exe >& log.geogrid
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

check_wps_success "log.geogrid" "geogrid" || exit 1
log_info "geogrid completed in $(format_duration $DURATION)"

# Verify output
for dom in $(seq 1 ${MAX_DOM:-1}); do
    if [[ -f "geo_em.d$(printf '%02d' $dom).nc" ]]; then
        log_success "Created geo_em.d$(printf '%02d' $dom).nc"
    else
        log_error "Missing geo_em.d$(printf '%02d' $dom).nc"
        exit 1
    fi
done

# -----------------------------------------------------------------------------
# Step 3: Link GFS data and run ungrib
# -----------------------------------------------------------------------------
log_step "Step 3: Linking GFS data and running ungrib.exe"

link_gfs_data "$GFS_DATA_DIR" "$WPS_WORKSPACE" || exit 1

START_TIME=$(date +%s)

./ungrib.exe >& log.ungrib

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

check_wps_success "log.ungrib" "ungrib" || exit 1
log_info "ungrib completed in $(format_duration $DURATION)"

# Verify output
FILE_COUNT=$(ls FILE:* 2>/dev/null | wc -l)
if [[ $FILE_COUNT -gt 0 ]]; then
    log_success "Created $FILE_COUNT intermediate files"
else
    log_error "No FILE:* files created"
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 4: Run metgrid
# -----------------------------------------------------------------------------
log_step "Step 4: Running metgrid.exe"

START_TIME=$(date +%s)

./metgrid.exe >& log.metgrid

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

check_wps_success "log.metgrid" "metgrid" || exit 1
log_info "metgrid completed in $(format_duration $DURATION)"

# Verify output
for dom in $(seq 1 ${MAX_DOM:-1}); do
    MET_COUNT=$(ls met_em.d$(printf '%02d' $dom).*.nc 2>/dev/null | wc -l)
    if [[ $MET_COUNT -gt 0 ]]; then
        log_success "Created $MET_COUNT met_em files for domain $dom"
    else
        log_error "No met_em files created for domain $dom"
        exit 1
    fi
done

# =============================================================================
log_step "WPS COMPLETED SUCCESSFULLY"
# =============================================================================

echo "Output files:"
ls -la met_em.d*.nc | head -10
echo ""
echo "Next step: Run the WRF script (run_wrf.sh)"
