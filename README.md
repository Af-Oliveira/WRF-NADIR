# WRF-NADIR

**Portable WRF Downscaling Wrapper **

A streamlined workflow for running WRF (Weather Research and Forecasting) simulations with a focus on Portugal and the Iberian Peninsula. Designed for operational weather forecasting.

---

## 📁 Project Structure

```
WRF-NADIR/
├── config.env                    # Main configuration file (user-editable)
├── run_forecast.sh               # Main entry point - orchestrates the full workflow
├── WRF_INSTALL_2025_MANUAL.sh    # WRF installation guide/script
│
├── scripts/
│   ├── download_gfs.py           # GFS data downloader (NCEP 0.25° from UCAR GDEX)
│   ├── generate_namelists.py     # Generates WPS/WRF namelists from templates
│   ├── run_wps.sh                # Executes WPS chain (geogrid → ungrib → metgrid)
│   ├── run_wrf.sh                # Executes WRF chain (real.exe → wrf.exe)
│   └── utils.sh                  # Common utilities and logging functions
│
├── templates/
│   ├── namelist.wps.template     # WPS namelist template
│   └── namelist.input.template   # WRF namelist template
│
├── GFS_DATA/                     # Downloaded GFS input data (organized by date)
│   └── YYYYMMDD/                 # e.g., 20251202/
│       └── gfs.0p25.*.grib2      # 0.25° resolution GRIB2 files
│
└── workspace/                    # Runtime workspace (auto-generated)
    ├── wps/                      # WPS working directory
    ├── wrf/                      # WRF working directory
    └── output/                   # Final WRF output files (wrfout_*)
```

---

## 🌍 Domain Configuration

The system supports a **triple-nested domain** configuration centered on Portugal. The number of active domains is controlled by `MAX_DOM` in [config.env](config.env).

### Available Domains

| Domain | Resolution | Coverage | Grid Size | Description |
|--------|------------|----------|-----------|-------------|
| **d01** | 27 km | Europe/Atlantic | 120 × 100 | Synoptic scale - captures large-scale weather patterns |
| **d02** | 9 km | Iberian Peninsula | 151 × 181 | Regional scale - resolves mesoscale features |
| **d03** | 3 km | Portugal | 202 × 301 | High-resolution - explicitly resolves convection |

### Domain Selection

Configure the number of domains in `config.env`:

```bash
# Run all 3 domains (27km → 9km → 3km)
export MAX_DOM=3

# Run 2 domains (27km → 9km) - Recommended for forecasts > 48h
export MAX_DOM=2

# Run 1 domain (27km only) - Fastest, for testing
export MAX_DOM=1
```

> **⚠️ Note:** Domain d03 (3km) is computationally expensive. For forecasts longer than 48 hours, consider using `MAX_DOM=2`.

### Map Projection

- **Projection:** Lambert Conformal Conic
- **Center:** 39.5°N, 8.0°W (Portugal)
- **True latitudes:** 35°N and 45°N

---

## 🚀 Capabilities

### Core Features

| Feature | Description |
|---------|-------------|
| **Automated Workflow** | Single command runs the complete forecast chain |
| **GFS Data Download** | Automatic download from UCAR GDEX (0.25° resolution) |
| **Dynamic Namelists** | Generates WPS/WRF namelists from configuration |
| **MPI Parallelization** | Supports parallel execution with MPICH |
| **Flexible Periods** | Define by explicit dates or forecast duration |
| **Clean Workspaces** | Options to clean/purge working directories |

### Workflow Steps

```
1. Generate Namelists  → Creates namelist.wps and namelist.input
2. Run WPS             → geogrid.exe → ungrib.exe → metgrid.exe
3. Run WRF             → real.exe → wrf.exe
4. Output              → wrfout_d0X_* files in workspace/output/
```

---

## 📤 Output

### Output Files

WRF produces NetCDF output files with the naming convention:

```
wrfout_d01_YYYY-MM-DD_HH:00:00    # Domain 1 (27km)
wrfout_d02_YYYY-MM-DD_HH:00:00    # Domain 2 (9km)  - if MAX_DOM ≥ 2
wrfout_d03_YYYY-MM-DD_HH:00:00    # Domain 3 (3km)  - if MAX_DOM = 3
```

### Output Settings (configurable in `config.env`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `HISTORY_INTERVAL` | 60 min | Output frequency (hourly) |
| `FRAMES_PER_OUTFILE` | 24 | Timesteps per file (1 day/file) |
| `RESTART_INTERVAL` | 1440 min | Restart file frequency (daily) |

### Key Variables for Fire Weather Index

The WRF output contains all variables needed for FWI computation:

- **T2** - 2-meter temperature (K)
- **Q2** - 2-meter specific humidity (kg/kg)
- **U10, V10** - 10-meter wind components (m/s)
- **RAINNC, RAINC** - Accumulated precipitation (mm)
- **PSFC** - Surface pressure (Pa)
- **SWDOWN** - Downward shortwave radiation (W/m²)

---

## 📖 Usage

### Quick Start

```bash
# 1. Edit configuration
nano config.env

# 2. Download GFS data (if not already available)
python3 scripts/download_gfs.py --start 2025-12-02 --end 2025-12-04 --hour 00

# 3. Run the complete forecast
./run_forecast.sh
```

### Command Options

```bash
./run_forecast.sh [OPTIONS]

Options:
  --config FILE    Use alternative config file (default: config.env)
  --skip-wps       Skip WPS steps (use existing met_em files)
  --skip-wrf       Skip WRF steps (only run WPS)
  --clean          Clean workspaces before running
  --clean-all      Purge ALL files from wps/, wrf/, and output/
  --dry-run        Show what would be done without executing
  --help           Show help message
```

### Examples

```bash
# Run with fresh workspace
./run_forecast.sh --clean

# Re-run WRF only (skip WPS preprocessing)
./run_forecast.sh --skip-wps

# Test configuration without executing
./run_forecast.sh --dry-run

# Use custom configuration
./run_forecast.sh --config my_custom_config.env
```

---

## ⚙️ Configuration Reference

### Simulation Period

```bash
# Option 1: Explicit dates
export START_DATE="2025-12-02_00:00:00"
export END_DATE="2025-12-04_00:00:00"

# Option 2: Duration-based (recommended)
export START_DATE="2025-12-02_00:00:00"
export FORECAST_DURATION_HOURS=48
```

### Physics Options (optimized for Fire Weather)

| Parameter | Value | Scheme |
|-----------|-------|--------|
| `MP_PHYSICS` | 8 | Thompson microphysics |
| `RA_LW_PHYSICS` | 4 | RRTMG longwave |
| `RA_SW_PHYSICS` | 4 | RRTMG shortwave |
| `SF_SURFACE_PHYSICS` | 4 | Noah-MP land surface |
| `BL_PBL_PHYSICS` | 1 | YSU PBL scheme |
| `CU_PHYSICS` | 1/1/0 | Kain-Fritsch (off for d03) |

### Parallel Execution

```bash
export NUM_PROCESSORS=8     # Number of MPI processes
export USE_MPI=true         # Enable MPI
export NUM_TILES_X=4        # Domain decomposition X
export NUM_TILES_Y=2        # Domain decomposition Y
```

---

## 📋 Requirements

- **WRF v4.5+** with WPS v4.5+
- **MPICH** for parallel execution
- **Python 3.6+** for scripts
- **WPS Geographic Data** (static terrain data)
- **GFS Data** (downloaded automatically or manually)

---

## 📝 License

This project is provided as-is for research and operational use.

---

## 🔗 References

- [WRF Model](https://www.mmm.ucar.edu/models/wrf)
- [WRF Users Guide](https://www2.mmm.ucar.edu/wrf/users/)
- [GFS Data - UCAR GDEX](https://rda.ucar.edu/datasets/ds084.1/)
- [WRF Instalation](https://github.com/HathewayWill/WRF-MOSIT)
