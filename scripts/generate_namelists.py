#!/usr/bin/env python3
"""
WRF Namelist Generator
======================
Reads configuration from config.env and generates namelist.wps and namelist.input
from templates with proper nesting calculations.

Usage:
    python generate_namelists.py [--config CONFIG_FILE] [--output-dir OUTPUT_DIR]
"""

import os
import sys
import re
import argparse
from datetime import datetime, timedelta
from pathlib import Path


class NamelistGenerator:
    """Generate WPS and WRF namelists from templates and configuration."""
    
    def __init__(self, config_file: str, output_dir: str = None):
        self.config = {}
        self.project_dir = Path(config_file).parent.resolve()
        
        # Load configuration first
        self.load_config(config_file)
        
        # Determine output directory (priority: arg > config > default)
        if output_dir:
            self.output_dir = Path(output_dir)
        elif 'WORKSPACE_DIR' in self.config:
            self.output_dir = Path(self.config['WORKSPACE_DIR'])
        else:
            self.output_dir = self.project_dir / "workspace"
        
        # Calculate derived values
        self.calculate_derived_values()
        
        # Generate dynamic domain arrays for namelists
        self.generate_domain_arrays()
        
        # Validate nesting
        self.validate_nesting()
    
    def load_config(self, config_file: str):
        """Load configuration from shell-style config file."""
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        
        # Pre-populate PROJECT_DIR with the actual resolved path
        self.config['PROJECT_DIR'] = str(self.project_dir)
        
        with open(config_path, 'r') as f:
            for line in f:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Handle export statements
                if line.startswith('export '):
                    line = line[7:]
                
                # Parse KEY=VALUE
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    
                    # Remove inline comments (everything after # in value)
                    if '#' in value:
                        value = value.split('#')[0].strip()
                    
                    # Skip PROJECT_DIR from config file - use our calculated one
                    if key == 'PROJECT_DIR':
                        continue
                    
                    # Handle variable expansion
                    value = self.expand_variables(value)
                    
                    # Store in config
                    self.config[key] = value
        
        print(f"✅ Loaded configuration from {config_file}")
    
    def expand_variables(self, value: str) -> str:
        """Expand shell variables like ${HOME} and ${VAR}."""
        # Expand ${VAR} style
        pattern = r'\$\{([^}]+)\}'
        
        def replacer(match):
            var_name = match.group(1)
            # Check our config first, then environment
            return self.config.get(var_name, os.environ.get(var_name, match.group(0)))
        
        # Multiple passes to handle nested variables
        for _ in range(3):
            new_value = re.sub(pattern, replacer, value)
            if new_value == value:
                break
            value = new_value
        
        # Also expand $HOME
        value = value.replace('$HOME', os.environ.get('HOME', '~'))
        value = os.path.expanduser(value)
        
        return value
    
    def parse_datetime(self, date_str: str) -> datetime:
        """Parse a date string in various formats to datetime object.
        
        Supports hour offsets >= 24 (e.g. 2026-02-16_24:00:00) which are
        normalized by adding the extra hours to midnight of the given date.
        This allows splitting forecasts into chunks:
            2026-02-16_00:00:00  -> Feb 16 00Z
            2026-02-16_24:00:00  -> Feb 17 00Z
            2026-02-16_48:00:00  -> Feb 18 00Z
        """
        if not date_str:
            return None
        
        # Check for hour >= 24 offset notation (e.g. 2026-02-16_24:00:00)
        if '_' in date_str:
            parts = date_str.split('_', 1)
            time_part = parts[1]
            time_components = time_part.split(':')
            hour = int(time_components[0])
            if hour >= 24:
                base_date = datetime.strptime(parts[0], '%Y-%m-%d')
                minutes = int(time_components[1]) if len(time_components) > 1 else 0
                seconds = int(time_components[2]) if len(time_components) > 2 else 0
                return base_date + timedelta(hours=hour, minutes=minutes, seconds=seconds)
        
        # Handle standard date formats
        for fmt in ['%Y-%m-%d_%H:%M:%S', '%Y-%m-%d_%H', '%Y-%m-%d']:
            try:
                if '_' in date_str and fmt == '%Y-%m-%d_%H:%M:%S':
                    parts = date_str.split('_')
                    if len(parts) >= 2:
                        normalized = parts[0] + '_' + parts[1]
                        if len(parts[1]) == 2:  # Just hour
                            normalized = parts[0] + '_' + parts[1] + ':00:00'
                        return datetime.strptime(normalized, '%Y-%m-%d_%H:%M:%S')
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Last resort: just parse the date portion
        return datetime.strptime(date_str[:10], '%Y-%m-%d')
    
    def calculate_derived_values(self):
        """Calculate values that depend on other configuration."""
        
        # Parse start date
        start_str = self.config.get('START_DATE', '2025-12-02_00:00:00')
        self.original_start_str = start_str  # Preserve for logging
        start_dt = self.parse_datetime(start_str)
        
        # Check for forecast duration mode
        forecast_duration_hours = self.config.get('FORECAST_DURATION_HOURS', '')
        
        if forecast_duration_hours and forecast_duration_hours.strip():
            # Forecast mode: calculate END_DATE from START_DATE + FORECAST_DURATION_HOURS
            try:
                duration_hours = int(forecast_duration_hours)
                end_dt = start_dt + timedelta(hours=duration_hours)
                # Show original notation if it used hour >= 24 offset
                if self.original_start_str != start_dt.strftime('%Y-%m-%d_%H:%M:%S'):
                    print(f"🔮 Forecast Mode: {duration_hours} hours (offset notation: {self.original_start_str})")
                    print(f"   Normalized start: {start_dt}")
                else:
                    print(f"🔮 Forecast Mode: {duration_hours} hours from {start_dt}")
            except ValueError:
                raise ValueError(f"Invalid FORECAST_DURATION_HOURS: {forecast_duration_hours}")
        else:
            # Traditional mode: use explicit END_DATE
            end_str = self.config.get('END_DATE', '')
            if not end_str:
                raise ValueError("Either END_DATE or FORECAST_DURATION_HOURS must be specified")
            end_dt = self.parse_datetime(end_str)
        
        # Store parsed dates
        self.start_dt = start_dt
        self.end_dt = end_dt
        
        # Store forecast duration for potential use by other tools
        duration = end_dt - start_dt
        total_hours = int(duration.total_seconds() // 3600)
        self.config['FORECAST_DURATION_HOURS'] = str(total_hours)
        
        # Calculate run duration
        run_days = duration.days
        run_hours = duration.seconds // 3600
        
        # Date components for namelist.input
        self.config['START_YEAR'] = str(start_dt.year)
        self.config['START_MONTH'] = f"{start_dt.month:02d}"
        self.config['START_DAY'] = f"{start_dt.day:02d}"
        self.config['START_HOUR'] = f"{start_dt.hour:02d}"
        
        self.config['END_YEAR'] = str(end_dt.year)
        self.config['END_MONTH'] = f"{end_dt.month:02d}"
        self.config['END_DAY'] = f"{end_dt.day:02d}"
        self.config['END_HOUR'] = f"{end_dt.hour:02d}"
        
        self.config['RUN_DAYS'] = str(run_days)
        self.config['RUN_HOURS'] = str(run_hours)
        
        # Full date strings for WPS
        self.config['START_DATE'] = start_dt.strftime('%Y-%m-%d_%H:%M:%S')
        self.config['END_DATE'] = end_dt.strftime('%Y-%m-%d_%H:%M:%S')
        
        # Calculate child domain resolutions
        d01_dx = int(self.config.get('D01_DX', 27000))
        d01_dy = int(self.config.get('D01_DY', 27000))
        d02_ratio = int(self.config.get('D02_PARENT_GRID_RATIO', 3))
        d03_ratio = int(self.config.get('D03_PARENT_GRID_RATIO', 3))
        
        d02_dx = d01_dx // d02_ratio
        d02_dy = d01_dy // d02_ratio
        d03_dx = d02_dx // d03_ratio
        d03_dy = d02_dy // d03_ratio
        
        # Workspace paths
        self.config['WORKSPACE_WPS'] = str(self.output_dir / 'wps')
        self.config['WORKSPACE_WRF'] = str(self.output_dir / 'wrf')
        if end_dt is not None:
            print(f"📅 Simulation period: {start_dt} to {end_dt}")
        print(f"⏱️  Duration: {run_days} days")
        
        # Display resolutions based on MAX_DOM
        max_dom = int(self.config.get('MAX_DOM', 3))
        if max_dom == 1:
            print(f"🗺️  Resolution: d01={d01_dx/1000:.0f}km (single domain)")
        elif max_dom == 2:
            print(f"🗺️  Resolutions: d01={d01_dx/1000:.0f}km, d02={d02_dx/1000:.0f}km")
        else:
            print(f"🗺️  Resolutions: d01={d01_dx/1000:.0f}km, d02={d02_dx/1000:.0f}km, d03={d03_dx/1000:.0f}km")

    def generate_domain_arrays(self):
        """Generate dynamic arrays for namelist entries based on MAX_DOM."""
        max_dom = int(self.config.get('MAX_DOM', 3))
        
        print(f"🔧 Generating configuration for {max_dom} domain(s)")
        
        # Helper function to create comma-separated value strings
        def make_array(values, count):
            """Create a comma-separated string with the first 'count' values."""
            return ', '.join(str(v) for v in values[:count])
        
        # Get configuration values
        d01_dx = int(self.config.get('D01_DX', 27000))
        d01_dy = int(self.config.get('D01_DY', 27000))
        d02_ratio = int(self.config.get('D02_PARENT_GRID_RATIO', 3))
        d03_ratio = int(self.config.get('D03_PARENT_GRID_RATIO', 3))
        
        d02_dx = d01_dx // d02_ratio
        d02_dy = d01_dy // d02_ratio
        d03_dx = d02_dx // d03_ratio
        d03_dy = d02_dy // d03_ratio
        
        # =========================================================================
        # WPS NAMELIST ARRAYS
        # =========================================================================
        
        # &share section
        start_date = self.config.get('START_DATE', '2025-12-02_00:00:00')
        end_date = self.config.get('END_DATE', '2025-12-03_00:00:00')
        self.config['START_DATE_ARRAY'] = make_array([f"'{start_date}'"] * 3, max_dom)
        self.config['END_DATE_ARRAY'] = make_array([f"'{end_date}'"] * 3, max_dom)
        
        # &geogrid section
        parent_ids = [1, 1, 2]
        parent_ratios = [1, d02_ratio, d03_ratio]
        i_parent_starts = [1, int(self.config.get('D02_I_PARENT_START', 35)), int(self.config.get('D03_I_PARENT_START', 40))]
        j_parent_starts = [1, int(self.config.get('D02_J_PARENT_START', 25)), int(self.config.get('D03_J_PARENT_START', 35))]
        e_we = [int(self.config.get('D01_E_WE', 120)), int(self.config.get('D02_E_WE', 151)), int(self.config.get('D03_E_WE', 202))]
        e_sn = [int(self.config.get('D01_E_SN', 100)), int(self.config.get('D02_E_SN', 181)), int(self.config.get('D03_E_SN', 301))]
        
        self.config['PARENT_ID_ARRAY'] = make_array(parent_ids, max_dom)
        self.config['PARENT_GRID_RATIO_ARRAY'] = make_array(parent_ratios, max_dom)
        self.config['I_PARENT_START_ARRAY'] = make_array(i_parent_starts, max_dom)
        self.config['J_PARENT_START_ARRAY'] = make_array(j_parent_starts, max_dom)
        self.config['E_WE_ARRAY'] = make_array(e_we, max_dom)
        self.config['E_SN_ARRAY'] = make_array(e_sn, max_dom)
        self.config['GEOG_DATA_RES_ARRAY'] = make_array(["'default'"] * 3, max_dom)
        
        # =========================================================================
        # WRF NAMELIST ARRAYS
        # =========================================================================
        
        # &time_control section
        start_year = self.config.get('START_YEAR', '2025')
        start_month = self.config.get('START_MONTH', '12')
        start_day = self.config.get('START_DAY', '02')
        start_hour = self.config.get('START_HOUR', '00')
        end_year = self.config.get('END_YEAR', '2025')
        end_month = self.config.get('END_MONTH', '12')
        end_day = self.config.get('END_DAY', '03')
        end_hour = self.config.get('END_HOUR', '00')
        
        self.config['START_YEAR_ARRAY'] = make_array([start_year] * 3, max_dom)
        self.config['START_MONTH_ARRAY'] = make_array([start_month] * 3, max_dom)
        self.config['START_DAY_ARRAY'] = make_array([start_day] * 3, max_dom)
        self.config['START_HOUR_ARRAY'] = make_array([start_hour] * 3, max_dom)
        self.config['END_YEAR_ARRAY'] = make_array([end_year] * 3, max_dom)
        self.config['END_MONTH_ARRAY'] = make_array([end_month] * 3, max_dom)
        self.config['END_DAY_ARRAY'] = make_array([end_day] * 3, max_dom)
        self.config['END_HOUR_ARRAY'] = make_array([end_hour] * 3, max_dom)
        
        history_interval = self.config.get('HISTORY_INTERVAL', '60')
        frames_per_outfile = self.config.get('FRAMES_PER_OUTFILE', '24')
        self.config['HISTORY_INTERVAL_ARRAY'] = make_array([history_interval] * 3, max_dom)
        self.config['FRAMES_PER_OUTFILE_ARRAY'] = make_array([frames_per_outfile] * 3, max_dom)
        self.config['INPUT_FROM_FILE_ARRAY'] = make_array(['.true.'] * 3, max_dom)
        self.config['AUXINPUT4_INTERVAL_ARRAY'] = make_array([360] * 3, max_dom)
        
        # &domains section
        e_vert = self.config.get('E_VERT', '45')
        dx_values = [d01_dx, d02_dx, d03_dx]
        dy_values = [d01_dy, d02_dy, d03_dy]
        grid_ids = [1, 2, 3]
        parent_ids_wrf = [0, 1, 2]
        
        self.config['E_VERT_ARRAY'] = make_array([e_vert] * 3, max_dom)
        self.config['DX_ARRAY'] = make_array(dx_values, max_dom)
        self.config['DY_ARRAY'] = make_array(dy_values, max_dom)
        self.config['GRID_ID_ARRAY'] = make_array(grid_ids, max_dom)
        self.config['PARENT_ID_WRF_ARRAY'] = make_array(parent_ids_wrf, max_dom)
        self.config['PARENT_TIME_STEP_RATIO_ARRAY'] = make_array(parent_ratios, max_dom)
        
        # &physics section - per-domain physics options
        mp_physics = self.config.get('MP_PHYSICS', '8')
        ra_lw = self.config.get('RA_LW_PHYSICS', '4')
        ra_sw = self.config.get('RA_SW_PHYSICS', '4')
        radt = self.config.get('RADT', '15')
        bl_pbl = self.config.get('BL_PBL_PHYSICS', '1')
        sf_sfclay = self.config.get('SF_SFCLAY_PHYSICS', '1')
        sf_surface = self.config.get('SF_SURFACE_PHYSICS', '4')
        
        # Cumulus: Usually on for coarse domains, off for fine resolution
        cu_d01 = self.config.get('CU_PHYSICS_D01', '1')
        cu_d02 = self.config.get('CU_PHYSICS_D02', '1')
        cu_d03 = self.config.get('CU_PHYSICS_D03', '0')
        cu_values = [cu_d01, cu_d02, cu_d03]
        
        self.config['MP_PHYSICS_ARRAY'] = make_array([mp_physics] * 3, max_dom)
        self.config['CU_PHYSICS_ARRAY'] = make_array(cu_values, max_dom)
        self.config['RA_LW_PHYSICS_ARRAY'] = make_array([ra_lw] * 3, max_dom)
        self.config['RA_SW_PHYSICS_ARRAY'] = make_array([ra_sw] * 3, max_dom)
        self.config['RADT_ARRAY'] = make_array([radt] * 3, max_dom)
        self.config['BL_PBL_PHYSICS_ARRAY'] = make_array([bl_pbl] * 3, max_dom)
        self.config['SF_SFCLAY_PHYSICS_ARRAY'] = make_array([sf_sfclay] * 3, max_dom)
        self.config['SF_SURFACE_PHYSICS_ARRAY'] = make_array([sf_surface] * 3, max_dom)
        self.config['BLDT_ARRAY'] = make_array([0] * 3, max_dom)
        self.config['SF_URBAN_PHYSICS_ARRAY'] = make_array([0] * 3, max_dom)
        
        # &dynamics section
        self.config['DIFF_OPT_ARRAY'] = make_array([2] * 3, max_dom)
        self.config['KM_OPT_ARRAY'] = make_array([4] * 3, max_dom)
        self.config['DIFF_6TH_OPT_ARRAY'] = make_array([0] * 3, max_dom)
        self.config['DIFF_6TH_FACTOR_ARRAY'] = make_array([0.12] * 3, max_dom)
        self.config['ZDAMP_ARRAY'] = make_array([5000.] * 3, max_dom)
        self.config['DAMPCOEF_ARRAY'] = make_array([0.2] * 3, max_dom)
        self.config['KHDIF_ARRAY'] = make_array([0] * 3, max_dom)
        self.config['KVDIF_ARRAY'] = make_array([0] * 3, max_dom)
        self.config['NON_HYDROSTATIC_ARRAY'] = make_array(['.true.'] * 3, max_dom)
        self.config['MOIST_ADV_OPT_ARRAY'] = make_array([1] * 3, max_dom)
        self.config['SCALAR_ADV_OPT_ARRAY'] = make_array([1] * 3, max_dom)
        self.config['EPSSM_ARRAY'] = make_array([0.1] * 3, max_dom)
        
        # GWD: typically only for outer domain
        gwd_values = [1, 0, 0]
        self.config['GWD_OPT_ARRAY'] = make_array(gwd_values, max_dom)
        
        # I/O quilting: when enabled (1), use configured tasks/groups;
        # when disabled (0), force nio_tasks=0 and nio_groups=1.
        io_quilting = int(self.config.get('IO_QUILTING', '0'))
        if io_quilting == 0:
            self.config['NUM_IO_TASKS'] = '0'
            self.config['NUM_IO_GROUPS'] = '1'
    
    def validate_nesting(self):
        """Validate that nesting configuration is mathematically correct."""
        errors = []
        warnings = []
        
        max_dom = int(self.config.get('MAX_DOM', 1))
        
        if max_dom < 2:
            print("ℹ️  Single domain configuration - no nesting validation needed")
            return
        
        # Get domain parameters
        d01_e_we = int(self.config.get('D01_E_WE', 100))
        d01_e_sn = int(self.config.get('D01_E_SN', 100))
        
        d02_e_we = int(self.config.get('D02_E_WE', 100))
        d02_e_sn = int(self.config.get('D02_E_SN', 100))
        d02_i_start = int(self.config.get('D02_I_PARENT_START', 1))
        d02_j_start = int(self.config.get('D02_J_PARENT_START', 1))
        d02_ratio = int(self.config.get('D02_PARENT_GRID_RATIO', 3))
        
        # Validate d02 grid ratio rule: (e_we - 1) % ratio == 0
        if (d02_e_we - 1) % d02_ratio != 0:
            errors.append(f"d02: (e_we-1)={d02_e_we-1} not divisible by ratio={d02_ratio}")
            suggested = ((d02_e_we - 1) // d02_ratio + 1) * d02_ratio + 1
            warnings.append(f"  Suggested d02 e_we: {suggested}")
        
        if (d02_e_sn - 1) % d02_ratio != 0:
            errors.append(f"d02: (e_sn-1)={d02_e_sn-1} not divisible by ratio={d02_ratio}")
            suggested = ((d02_e_sn - 1) // d02_ratio + 1) * d02_ratio + 1
            warnings.append(f"  Suggested d02 e_sn: {suggested}")
        
        # Validate d02 fits within d01
        d02_end_i = d02_i_start + (d02_e_we - 1) // d02_ratio
        d02_end_j = d02_j_start + (d02_e_sn - 1) // d02_ratio
        
        if d02_end_i > d01_e_we:
            errors.append(f"d02 extends beyond d01 in i-direction: {d02_end_i} > {d01_e_we}")
        if d02_end_j > d01_e_sn:
            errors.append(f"d02 extends beyond d01 in j-direction: {d02_end_j} > {d01_e_sn}")
        
        # Validate d03 if present
        if max_dom >= 3:
            d03_e_we = int(self.config.get('D03_E_WE', 100))
            d03_e_sn = int(self.config.get('D03_E_SN', 100))
            d03_i_start = int(self.config.get('D03_I_PARENT_START', 1))
            d03_j_start = int(self.config.get('D03_J_PARENT_START', 1))
            d03_ratio = int(self.config.get('D03_PARENT_GRID_RATIO', 3))
            
            if (d03_e_we - 1) % d03_ratio != 0:
                errors.append(f"d03: (e_we-1)={d03_e_we-1} not divisible by ratio={d03_ratio}")
                suggested = ((d03_e_we - 1) // d03_ratio + 1) * d03_ratio + 1
                warnings.append(f"  Suggested d03 e_we: {suggested}")
            
            if (d03_e_sn - 1) % d03_ratio != 0:
                errors.append(f"d03: (e_sn-1)={d03_e_sn-1} not divisible by ratio={d03_ratio}")
                suggested = ((d03_e_sn - 1) // d03_ratio + 1) * d03_ratio + 1
                warnings.append(f"  Suggested d03 e_sn: {suggested}")
            
            # d03 must fit within d02
            d03_end_i = d03_i_start + (d03_e_we - 1) // d03_ratio
            d03_end_j = d03_j_start + (d03_e_sn - 1) // d03_ratio
            
            if d03_end_i > d02_e_we:
                errors.append(f"d03 extends beyond d02 in i-direction: {d03_end_i} > {d02_e_we}")
            if d03_end_j > d02_e_sn:
                errors.append(f"d03 extends beyond d02 in j-direction: {d03_end_j} > {d02_e_sn}")
        
        # Report results
        if errors:
            print("\n❌ NESTING VALIDATION ERRORS:")
            for err in errors:
                print(f"   {err}")
            for warn in warnings:
                print(f"   {warn}")
            raise ValueError("Nesting configuration is invalid. Please fix errors above.")
        else:
            print("✅ Nesting configuration validated successfully")
    
    def render_template(self, template_path: str) -> str:
        """Render a template file with configuration values."""
        with open(template_path, 'r') as f:
            content = f.read()
        
        # Replace {{VARIABLE}} placeholders
        def replacer(match):
            var_name = match.group(1)
            value = self.config.get(var_name, match.group(0))
            return str(value)
        
        content = re.sub(r'\{\{(\w+)\}\}', replacer, content)
        
        return content
    
    def generate(self, templates_dir: str = None):
        """Generate both namelist files."""
        if templates_dir is None:
            templates_dir = self.project_dir / "templates"
        else:
            templates_dir = Path(templates_dir)
        
        # Create output directories
        wps_dir = self.output_dir / "wps"
        wrf_dir = self.output_dir / "wrf"
        wps_dir.mkdir(parents=True, exist_ok=True)
        wrf_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate namelist.wps
        wps_template = templates_dir / "namelist.wps.template"
        if wps_template.exists():
            wps_content = self.render_template(wps_template)
            wps_output = wps_dir / "namelist.wps"
            with open(wps_output, 'w') as f:
                f.write(wps_content)
            print(f"📄 Generated: {wps_output}")
        else:
            print(f"⚠️  Template not found: {wps_template}")
        
        # Generate namelist.input
        input_template = templates_dir / "namelist.input.template"
        if input_template.exists():
            input_content = self.render_template(input_template)
            input_output = wrf_dir / "namelist.input"
            with open(input_output, 'w') as f:
                f.write(input_content)
            print(f"📄 Generated: {input_output}")
        else:
            print(f"⚠️  Template not found: {input_template}")
        
        return wps_dir, wrf_dir


def main():
    parser = argparse.ArgumentParser(
        description="Generate WRF namelists from templates and configuration"
    )
    parser.add_argument(
        '--config', '-c',
        default='config.env',
        help='Path to configuration file (default: config.env)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory for generated namelists'
    )
    parser.add_argument(
        '--templates', '-t',
        help='Templates directory'
    )
    
    args = parser.parse_args()
    
    # Find config file
    config_path = Path(args.config)
    if not config_path.exists():
        # Try relative to script location
        script_dir = Path(__file__).parent.parent
        config_path = script_dir / args.config
    
    if not config_path.exists():
        print(f"❌ Configuration file not found: {args.config}")
        sys.exit(1)
    
    try:
        generator = NamelistGenerator(str(config_path), args.output_dir)
        generator.generate(args.templates)
        print("\n✅ Namelist generation complete!")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
