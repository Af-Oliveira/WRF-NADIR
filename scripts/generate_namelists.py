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
import math


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
        """Parse a date string in various formats to datetime object."""
        if not date_str:
            return None
            
        # Handle different date formats
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
        start_dt = self.parse_datetime(start_str)
        
        # Check for forecast duration mode
        forecast_duration_hours = self.config.get('FORECAST_DURATION_HOURS', '')
        
        if forecast_duration_hours and forecast_duration_hours.strip():
            # Forecast mode: calculate END_DATE from START_DATE + FORECAST_DURATION_HOURS
            try:
                duration_hours = int(forecast_duration_hours)
                end_dt = start_dt + timedelta(hours=duration_hours)
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
        
        self.config['D02_DX'] = str(d02_dx)
        self.config['D02_DY'] = str(d02_dy)
        self.config['D03_DX'] = str(d03_dx)
        self.config['D03_DY'] = str(d03_dy)
        
        # Workspace paths
        self.config['WORKSPACE_WPS'] = str(self.output_dir / 'wps')
        self.config['WORKSPACE_WRF'] = str(self.output_dir / 'wrf')
        
        print(f"📅 Simulation period: {start_dt} to {end_dt}")
        print(f"⏱️  Duration: {run_days} days, {run_hours} hours")
        print(f"🗺️  Resolutions: d01={d01_dx/1000:.0f}km, d02={d02_dx/1000:.0f}km, d03={d03_dx/1000:.0f}km")
    
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
