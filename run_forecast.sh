#!/bin/bash
# =============================================================================
# WRF-FWI Portugal: Main Entry Point
# =============================================================================
# This script orchestrates the complete WRF forecast workflow:
#   1. Generate namelists from configuration
#   2. Run WPS (geogrid, ungrib, metgrid)
#   3. Run WRF (real.exe, wrf.exe)
#
# Usage:
#   ./run_forecast.sh [OPTIONS]
#
# Options:
#   --config FILE    Use alternative config file (default: config.env)
#   --skip-wps       Skip WPS steps (use existing met_em files)
#   --skip-wrf       Skip WRF steps (only run WPS)
#   --clean          Clean workspaces before running (selective cleanup)
#   --clean-all      Purge ALL files from wps/, wrf/, and output/ folders
#   --dry-run        Show what would be done without executing
#   --help           Show this help message
# =============================================================================

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default options
CONFIG_FILE="$SCRIPT_DIR/config.env"
SKIP_WPS=false
SKIP_WRF=false
CLEAN_WORKSPACE=false
CLEAN_ALL=false
DRY_RUN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --skip-wps)
            SKIP_WPS=true
            shift
            ;;
        --skip-wrf)
            SKIP_WRF=true
            shift
            ;;
        --clean)
            CLEAN_WORKSPACE=true
            shift
            ;;
        --clean-all)
            CLEAN_ALL=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            head -30 "$0" | tail -25
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Source utilities
source "$SCRIPT_DIR/scripts/utils.sh"

# =============================================================================
log_step "WRF-FWI Portugal Forecast System"
# =============================================================================

echo "
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   🌍 WRF-FWI Portugal                                         ║
║   Portable Downscaling Wrapper for Fire Weather Index         ║
║                                                               ║
║   Triple-Nested Domain Configuration:                         ║
║     d01: 27km - Synoptic (Europe/Atlantic)                    ║
║     d02:  9km - Regional (Iberian Peninsula)                  ║
║     d03:  3km - Local (Portugal High-Resolution)              ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
"

# Load configuration
if [[ ! -f "$CONFIG_FILE" ]]; then
    log_error "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

log_info "Loading configuration from: $CONFIG_FILE"
source "$CONFIG_FILE"

# Calculate END_DATE from FORECAST_DURATION_HOURS if set
if [[ -n "${FORECAST_DURATION_HOURS:-}" ]]; then
    # Parse START_DATE (format: YYYY-MM-DD_HH:MM:SS)
    start_date_part="${START_DATE%%_*}"
    start_time_part="${START_DATE#*_}"
    start_hour="${start_time_part%%:*}"
    
    # Calculate end date using date command
    END_DATE=$(date -d "${start_date_part} ${start_hour}:00:00 ${FORECAST_DURATION_HOURS} hours" "+%Y-%m-%d_%H:00:00")
    export END_DATE
    
    log_info "Forecast Mode: ${FORECAST_DURATION_HOURS} hours"
    log_info "Calculated END_DATE: ${END_DATE}"
fi

# Export clean workspace setting
export CLEAN_WORKSPACE="$CLEAN_WORKSPACE"

# Purge all workspaces if --clean-all is specified
if [[ "$CLEAN_ALL" == "true" ]]; then
    log_warning "--clean-all specified: Purging ALL workspace files"
    purge_all_workspaces "${WORKSPACE_DIR:-$SCRIPT_DIR/workspace}"
fi

# Display configuration summary
echo ""
echo "Configuration Summary:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  WRF Installation:    $WRF_DIR"
echo "  WPS Installation:    $WPS_DIR"
echo "  Geographic Data:     $GEOG_DATA_PATH"
echo "  GFS Data:            $GFS_DATA_DIR"
echo ""
echo "  Simulation Period:   $START_DATE to $END_DATE"
if [[ -n "${FORECAST_DURATION_HOURS:-}" ]]; then
    echo "  Forecast Duration:   ${FORECAST_DURATION_HOURS} hours"
fi
echo "  Number of Domains:   $MAX_DOM"
echo ""
echo "  Resolutions:"
echo "    d01: ${D01_DX}m ($(echo "scale=0; ${D01_DX}/1000" | bc)km)"
d02_dx=$((D01_DX / D02_PARENT_GRID_RATIO))
d03_dx=$((d02_dx / D03_PARENT_GRID_RATIO))
echo "    d02: ${d02_dx}m ($(echo "scale=0; ${d02_dx}/1000" | bc)km)"
echo "    d03: ${d03_dx}m ($(echo "scale=1; ${d03_dx}/1000" | bc)km)"
echo ""
echo "  Options:"
echo "    Skip WPS:    $SKIP_WPS"
echo "    Skip WRF:    $SKIP_WRF"
echo "    Clean:       $CLEAN_WORKSPACE"
echo "    Clean All:   $CLEAN_ALL"
echo "    Dry Run:     $DRY_RUN"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    log_warning "DRY RUN MODE - No commands will be executed"
fi

# Confirm before proceeding
read -p "Proceed with forecast? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "Aborted by user"
    exit 0
fi

TOTAL_START=$(date +%s)

# =============================================================================
# Step 1: Generate Namelists
# =============================================================================
log_step "Step 1: Generating Namelists"

if [[ "$DRY_RUN" == "true" ]]; then
    log_info "[DRY RUN] Would run: python3 scripts/generate_namelists.py"
else
    cd "$SCRIPT_DIR"
    python3 scripts/generate_namelists.py --config "$CONFIG_FILE"
fi

# =============================================================================
# Step 2: Run WPS
# =============================================================================
if [[ "$SKIP_WPS" == "false" ]]; then
    log_step "Step 2: Running WPS"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run: scripts/run_wps.sh"
    else
        cd "$SCRIPT_DIR"
        bash scripts/run_wps.sh
    fi
else
    log_info "Skipping WPS (--skip-wps specified)"
fi

# =============================================================================
# Step 3: Run WRF
# =============================================================================
if [[ "$SKIP_WRF" == "false" ]]; then
    log_step "Step 3: Running WRF"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run: scripts/run_wrf.sh"
    else
        cd "$SCRIPT_DIR"
        bash scripts/run_wrf.sh
    fi
else
    log_info "Skipping WRF (--skip-wrf specified)"
fi

# =============================================================================
# Summary
# =============================================================================
TOTAL_END=$(date +%s)
TOTAL_DURATION=$((TOTAL_END - TOTAL_START))

log_step "FORECAST COMPLETE"

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    FORECAST SUMMARY                           ║"
echo "╠═══════════════════════════════════════════════════════════════╣"
printf "║  Total Runtime:    %-40s ║\n" "$(format_duration $TOTAL_DURATION)"
printf "║  Output Location:  %-40s ║\n" "${OUTPUT_DIR:-workspace/output}"
echo "║                                                               ║"
echo "║  Output Files:                                                ║"

if [[ -d "${OUTPUT_DIR:-$SCRIPT_DIR/workspace/output}" ]]; then
    for dom in 1 2 3; do
        count=$(ls "${OUTPUT_DIR:-$SCRIPT_DIR/workspace/output}"/wrfout_d$(printf '%02d' $dom)_* 2>/dev/null | wc -l)
        if [[ $count -gt 0 ]]; then
            printf "║    Domain %d: %-47s ║\n" "$dom" "$count files"
        fi
    done
fi

echo "║                                                               ║"
echo "║  🔥 Ready for FWI Post-Processing                             ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
