#!/bin/bash
# =============================================================================
# Common Utilities for WRF-FWI Portugal
# =============================================================================
# Source this file in other scripts: source "$(dirname "$0")/utils.sh"
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# -----------------------------------------------------------------------------
# Logging functions
# -----------------------------------------------------------------------------

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "\n${CYAN}========================================${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}========================================${NC}\n"
}

# -----------------------------------------------------------------------------
# Check functions
# -----------------------------------------------------------------------------

check_executable() {
    local exe_path="$1"
    local exe_name="$2"
    
    if [[ ! -x "$exe_path" ]]; then
        log_error "$exe_name not found or not executable: $exe_path"
        return 1
    fi
    log_info "Found $exe_name: $exe_path"
    return 0
}

check_file_exists() {
    local file_path="$1"
    local file_desc="$2"
    
    if [[ ! -f "$file_path" ]]; then
        log_error "$file_desc not found: $file_path"
        return 1
    fi
    return 0
}

check_directory_exists() {
    local dir_path="$1"
    local dir_desc="$2"
    
    if [[ ! -d "$dir_path" ]]; then
        log_error "$dir_desc not found: $dir_path"
        return 1
    fi
    return 0
}

# -----------------------------------------------------------------------------
# WRF/WPS specific checks
# -----------------------------------------------------------------------------

check_wrf_installation() {
    local wrf_dir="$1"
    
    log_info "Checking WRF installation at: $wrf_dir"
    
    check_directory_exists "$wrf_dir" "WRF directory" || return 1
    check_executable "$wrf_dir/main/real.exe" "real.exe" || return 1
    check_executable "$wrf_dir/main/wrf.exe" "wrf.exe" || return 1
    check_directory_exists "$wrf_dir/run" "WRF run directory" || return 1
    
    log_success "WRF installation verified"
    return 0
}

check_wps_installation() {
    local wps_dir="$1"
    
    log_info "Checking WPS installation at: $wps_dir"
    
    check_directory_exists "$wps_dir" "WPS directory" || return 1
    check_executable "$wps_dir/geogrid.exe" "geogrid.exe" || return 1
    check_executable "$wps_dir/ungrib.exe" "ungrib.exe" || return 1
    check_executable "$wps_dir/metgrid.exe" "metgrid.exe" || return 1
    check_file_exists "$wps_dir/link_grib.csh" "link_grib.csh" || return 1
    
    log_success "WPS installation verified"
    return 0
}

check_geog_data() {
    local geog_path="$1"
    
    log_info "Checking geographical data at: $geog_path"
    
    if [[ ! -d "$geog_path" ]]; then
        log_error "Geographical data directory not found: $geog_path"
        log_info "Download from: https://www2.mmm.ucar.edu/wrf/users/download/get_sources_wps_geog.html"
        return 1
    fi
    
    # Check for essential datasets
    local essential_dirs=("topo_30s" "landuse_30s" "soiltype_top_30s")
    for dir in "${essential_dirs[@]}"; do
        if [[ ! -d "$geog_path/$dir" ]]; then
            log_warning "Missing recommended dataset: $dir"
        fi
    done
    
    log_success "Geographical data directory found"
    return 0
}

# -----------------------------------------------------------------------------
# Log file checking
# -----------------------------------------------------------------------------

check_wps_success() {
    local log_file="$1"
    local program_name="$2"
    
    if grep -q "Successful completion of $program_name" "$log_file" 2>/dev/null; then
        log_success "$program_name completed successfully"
        return 0
    else
        log_error "$program_name failed. Check $log_file for details"
        echo "Last 20 lines of log:"
        tail -20 "$log_file"
        return 1
    fi
}

check_wrf_success() {
    local rsl_file="$1"
    local program_name="$2"
    
    if grep -q "SUCCESS COMPLETE" "$rsl_file" 2>/dev/null; then
        log_success "$program_name completed successfully"
        return 0
    elif grep -q "SUCCESS" "$rsl_file" 2>/dev/null; then
        log_success "$program_name appears to have completed"
        return 0
    else
        log_error "$program_name may have failed. Check $rsl_file for details"
        echo "Last 30 lines of log:"
        tail -30 "$rsl_file"
        return 1
    fi
}

# -----------------------------------------------------------------------------
# Cleanup functions
# -----------------------------------------------------------------------------

clean_wps_workspace() {
    local workspace="$1"
    
    log_info "Cleaning WPS workspace: $workspace"
    
    cd "$workspace" || return 1
    
    # Remove intermediate files
    rm -f FILE:* GRIBFILE.* PFILE:* 2>/dev/null
    rm -f geo_em.d*.nc met_em.d*.nc 2>/dev/null
    rm -f log.geogrid log.ungrib log.metgrid 2>/dev/null
    rm -f Vtable 2>/dev/null
    rm -f namelist.wps.backup 2>/dev/null
    
    log_success "WPS workspace cleaned"
}

clean_wrf_workspace() {
    local workspace="$1"
    
    log_info "Cleaning WRF workspace: $workspace"
    
    cd "$workspace" || return 1
    
    # Remove intermediate and output files
    rm -f met_em.d*.nc 2>/dev/null
    rm -f wrfinput_d* wrfbdy_d* wrfout_d* wrfrst_d* 2>/dev/null
    rm -f rsl.out.* rsl.error.* 2>/dev/null
    rm -f log.real log.wrf 2>/dev/null
    rm -f namelist.input.backup 2>/dev/null
    
    log_success "WRF workspace cleaned"
}

purge_wps_workspace() {
    local workspace="$1"
    
    log_warning "Purging ALL files from WPS workspace: $workspace"
    
    if [[ -d "$workspace" ]]; then
        rm -rf "${workspace:?}"/*
        log_success "WPS workspace purged completely"
    else
        log_info "WPS workspace does not exist: $workspace"
    fi
}

purge_wrf_workspace() {
    local workspace="$1"
    
    log_warning "Purging ALL files from WRF workspace: $workspace"
    
    if [[ -d "$workspace" ]]; then
        rm -rf "${workspace:?}"/*
        log_success "WRF workspace purged completely"
    else
        log_info "WRF workspace does not exist: $workspace"
    fi
}

purge_all_workspaces() {
    local workspace_dir="$1"
    
    log_warning "PURGING ALL WORKSPACE FILES"
    
    purge_wps_workspace "${workspace_dir}/wps"
    purge_wrf_workspace "${workspace_dir}/wrf"
    
    # Also clean output directory
    if [[ -d "${workspace_dir}/output" ]]; then
        log_warning "Purging output directory: ${workspace_dir}/output"
        rm -rf "${workspace_dir:?}/output"/*
        log_success "Output directory purged"
    fi
    
    log_success "All workspaces purged completely"
}

# -----------------------------------------------------------------------------
# File linking functions
# -----------------------------------------------------------------------------

link_wps_files() {
    local wps_dir="$1"
    local workspace="$2"
    
    log_info "Linking WPS files to workspace"
    
    cd "$workspace" || return 1
    
    # Link executables
    ln -sf "$wps_dir/geogrid.exe" .
    ln -sf "$wps_dir/ungrib.exe" .
    ln -sf "$wps_dir/metgrid.exe" .
    ln -sf "$wps_dir/link_grib.csh" .
    
    # Link utility directories
    ln -sf "$wps_dir/geogrid" .
    ln -sf "$wps_dir/ungrib" .
    ln -sf "$wps_dir/metgrid" .
    
    log_success "WPS files linked"
}

link_wrf_files() {
    local wrf_dir="$1"
    local workspace="$2"
    
    log_info "Linking WRF files to workspace"
    
    cd "$workspace" || return 1
    
    # Link executables
    ln -sf "$wrf_dir/main/real.exe" .
    ln -sf "$wrf_dir/main/wrf.exe" .
    
    # Link all necessary runtime files from WRF run directory
    for file in "$wrf_dir/run/"*; do
        if [[ -f "$file" ]]; then
            base=$(basename "$file")
            # Skip namelists and output files
            if [[ "$base" != "namelist.input" && "$base" != "namelist.output" && \
                  "$base" != rsl.* && "$base" != wrfout* && "$base" != wrfrst* ]]; then
                ln -sf "$file" . 2>/dev/null
            fi
        fi
    done
    
    log_success "WRF files linked"
}



link_gfs_data() {
    local gfs_dir="$1"
    local workspace="$2"
    
    log_info "Linking GFS data from: $gfs_dir"
    
    cd "$workspace" || return 1
    
    # Determine the correct subdirectory based on START_DATE
    # START_DATE format: YYYY-MM-DD_HH:MM:SS -> extract YYYYMMDD for folder name
    local start_ymd=$(echo "${START_DATE}" | sed 's/[-_:]//g' | cut -c1-8)
    
    # Calculate max forecast hour needed from FORECAST_DURATION_HOURS or END_DATE
    local max_fhr="${FORECAST_DURATION_HOURS:-72}"
    
    local gfs_subdir="${gfs_dir}/${start_ymd}"
    
    if [[ -d "$gfs_subdir" ]]; then
        log_info "Using GFS data from date folder: ${start_ymd}/"
        local search_dir="$gfs_subdir"
    else
        log_warning "Date folder ${start_ymd}/ not found, searching all of $gfs_dir"
        local search_dir="$gfs_dir"
    fi
    
    # Count GRIB files in the target directory (non-recursive for date folder)
    local grib_count
    if [[ "$search_dir" == "$gfs_subdir" ]]; then
        grib_count=$(find "$search_dir" -maxdepth 1 -name "*.grib2" -o -name "*.grb2" | wc -l)
    else
        grib_count=$(find "$search_dir" -name "*.grib2" -o -name "*.grb2" | wc -l)
    fi
    
    if [[ $grib_count -eq 0 ]]; then
        log_error "No GRIB2 files found in $search_dir"
        return 1
    fi
    
    log_info "Found $grib_count GRIB2 files in $search_dir"
    
    # Filter to only the forecast hours we need (f000 through f${max_fhr})
    # Build a list of needed files
    local needed_files=()
    local fhr=0
    local interval=3  # GFS interval in hours
    
    while [[ $fhr -le $max_fhr ]]; do
        local fhr_str=$(printf "f%03d" $fhr)
        local pattern="*${fhr_str}*"
        
        while IFS= read -r -d '' f; do
            needed_files+=("$f")
        done < <(find "$search_dir" -maxdepth 1 -type f \( -name "*.grib2" -o -name "*.grb2" \) -name "$pattern" -print0)
        
        ((fhr += interval))
    done
    
    if [[ ${#needed_files[@]} -eq 0 ]]; then
        log_warning "No files matched forecast hours 0-${max_fhr}, falling back to all files in $search_dir"
        while IFS= read -r -d '' f; do
            needed_files+=("$f")
        done < <(find "$search_dir" -maxdepth 1 -type f \( -name "*.grib2" -o -name "*.grb2" \) -print0 | sort -z)
    fi
    
    log_info "Selected ${#needed_files[@]} files for forecast hours 0-${max_fhr}h"
    
    # Remove old GRIBFILE links
    rm -f GRIBFILE.* 2>/dev/null
    
    # Alphabet array for creating AAA, AAB, AAC... suffixes
    local alphabet=(A B C D E F G H I J K L M N O P Q R S T U V W X Y Z)
    
    # Sort the needed files and create links
    local file_index=0
    
    while IFS= read -r grib_file; do
        if [[ -f "$grib_file" ]]; then
            # Calculate letter indices (base-26)
            local i1=$((file_index / 676))       # First letter
            local i2=$(((file_index / 26) % 26)) # Second letter
            local i3=$((file_index % 26))        # Third letter
            
            # Build the suffix
            local suffix="${alphabet[$i1]}${alphabet[$i2]}${alphabet[$i3]}"
            
            ln -sf "$grib_file" "GRIBFILE.$suffix"
            ((file_index++))
        fi
    done < <(printf '%s\n' "${needed_files[@]}" | sort)
    
    # Verify links were created
    local link_count=$(ls GRIBFILE.* 2>/dev/null | wc -l)
    if [[ $link_count -eq 0 ]]; then
        log_error "Failed to link GRIB files"
        return 1
    fi
    
    # Show first and last file for verification
    local first_file=$(ls GRIBFILE.* 2>/dev/null | head -1)
    local last_file=$(ls GRIBFILE.* 2>/dev/null | tail -1)
    log_success "Linked $link_count GRIB files ($first_file ... $last_file)"
}

link_met_em_files() {
    local wps_workspace="$1"
    local wrf_workspace="$2"
    
    log_info "Linking met_em files to WRF workspace"
    
    cd "$wrf_workspace" || return 1
    
    local met_count=$(ls "$wps_workspace"/met_em.d*.nc 2>/dev/null | wc -l)
    
    if [[ $met_count -eq 0 ]]; then
        log_error "No met_em files found in $wps_workspace"
        return 1
    fi
    
    ln -sf "$wps_workspace"/met_em.d*.nc .
    
    log_success "Linked $met_count met_em files"
}

# -----------------------------------------------------------------------------
# Time calculation functions
# -----------------------------------------------------------------------------

format_duration() {
    local seconds=$1
    local hours=$((seconds / 3600))
    local minutes=$(((seconds % 3600) / 60))
    local secs=$((seconds % 60))
    
    printf "%02d:%02d:%02d" $hours $minutes $secs
}

# =============================================================================
# END OF UTILITIES
# =============================================================================
