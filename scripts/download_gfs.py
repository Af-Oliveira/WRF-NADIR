#!/usr/bin/env python3
"""
GFS Data Downloader for WRF Simulations
========================================
Downloads NCEP GFS 0.25° data from UCAR GDEX (ds084.1) for WRF preprocessing.

Features:
- Automatic URL generation based on date range
- Progress bar with download speed
- Resume capability for interrupted downloads
- Parallel downloads for faster retrieval
- Automatic retry on failure
- Organizes files into date-based directories

Usage:
    python gdex-download.py --start 2025-12-02 --end 2025-12-04 --hour 00
    python gdex-download.py --start 2025-12-02 --end 2025-12-04 --hour 00 --output ~/Models/WRF_TUTORIAL/GFS_DATA

Author: WRF Project
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ============================================
# CONFIGURATION
# ============================================
BASE_URL = "https://osdf-director.osg-htc.org/ncar/gdex/d084001"
DATASET_ID = "d084001"  # NCEP GFS 0.25 Degree Global Forecast

# Default forecast hours to download (3-hourly for WRF)
DEFAULT_FORECAST_HOURS = list(range(0, 25, 3))  # f000, f003, f006, ..., f024

# Download settings
CHUNK_SIZE = 1024 * 1024  # 1 MB chunks
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_PARALLEL_DOWNLOADS = 2

# Lock for thread-safe printing
print_lock = threading.Lock()


def safe_print(*args, **kwargs):
    """Thread-safe print function."""
    with print_lock:
        print(*args, **kwargs)


def format_size(size_bytes):
    """Format bytes into human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def format_speed(bytes_per_sec):
    """Format download speed."""
    return f"{format_size(bytes_per_sec)}/s"


def format_time(seconds):
    """Format seconds into human-readable time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
    else:
        return f"{seconds // 3600:.0f}h {(seconds % 3600) // 60:.0f}m"


def generate_urls(start_date, end_date, init_hour, forecast_hours):
    """
    Generate download URLs for GFS data.
    
    Args:
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
        init_hour: Model initialization hour (0, 6, 12, or 18)
        forecast_hours: List of forecast hours to download
    
    Returns:
        List of (url, local_filename, date_str) tuples
    """
    urls = []
    current_date = start_date
    
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        year = current_date.strftime("%Y")
        
        for fhr in forecast_hours:
            filename = f"gfs.0p25.{date_str}{init_hour:02d}.f{fhr:03d}.grib2"
            url = f"{BASE_URL}/{year}/{date_str}/{filename}"
            urls.append((url, filename, date_str))
        
        current_date += timedelta(days=1)
    
    return urls


def get_file_size(url):
    """Get the size of a remote file."""
    try:
        request = Request(url, method='HEAD')
        response = urlopen(request, timeout=30)
        size = response.headers.get('Content-Length')
        return int(size) if size else None
    except Exception:
        return None


def download_file(url, output_path, show_progress=True):
    """
    Download a single file with progress tracking and resume capability.
    
    Args:
        url: URL to download
        output_path: Local file path to save to
        show_progress: Whether to show progress bar
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    output_path = Path(output_path)
    temp_path = output_path.with_suffix('.grib2.part')
    
    # Check if file already exists and is complete
    if output_path.exists():
        remote_size = get_file_size(url)
        local_size = output_path.stat().st_size
        if remote_size and local_size == remote_size:
            return True, "Already downloaded ✓ Verified"
        elif remote_size:
            # File exists but size is wrong - delete and re-download
            safe_print(f"  ⚠️  Existing file has wrong size ({format_size(local_size)} vs {format_size(remote_size)}), re-downloading...")
            output_path.unlink()
    
    # Check for partial download
    start_byte = 0
    if temp_path.exists():
        start_byte = temp_path.stat().st_size
    
    headers = {}
    if start_byte > 0:
        headers['Range'] = f'bytes={start_byte}-'
    
    for attempt in range(MAX_RETRIES):
        try:
            request = Request(url, headers=headers)
            response = urlopen(request, timeout=60)
            
            # Get total size
            if start_byte > 0:
                content_range = response.headers.get('Content-Range')
                if content_range:
                    total_size = int(content_range.split('/')[-1])
                else:
                    total_size = None
            else:
                total_size = response.headers.get('Content-Length')
                total_size = int(total_size) if total_size else None
            
            # Download with progress
            mode = 'ab' if start_byte > 0 else 'wb'
            downloaded = start_byte
            start_time = time.time()
            
            with open(temp_path, mode) as f:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    if show_progress and total_size:
                        elapsed = time.time() - start_time
                        speed = (downloaded - start_byte) / elapsed if elapsed > 0 else 0
                        percent = (downloaded / total_size) * 100
                        eta = (total_size - downloaded) / speed if speed > 0 else 0
                        
                        progress = f"\r  [{percent:5.1f}%] {format_size(downloaded)}/{format_size(total_size)} "
                        progress += f"@ {format_speed(speed)} ETA: {format_time(eta)}  "
                        sys.stdout.write(progress)
                        sys.stdout.flush()
            
            if show_progress:
                sys.stdout.write("\n")
            
            # Rename temp file to final name
            temp_path.rename(output_path)
            
            # Verify downloaded file size matches expected size
            if total_size:
                final_size = output_path.stat().st_size
                if final_size != total_size:
                    safe_print(f"\n  ⚠️  Size mismatch: got {format_size(final_size)}, expected {format_size(total_size)}")
                    output_path.unlink()  # Delete incomplete file
                    if attempt < MAX_RETRIES - 1:
                        safe_print(f"  Retrying download ({attempt + 2}/{MAX_RETRIES})...")
                        time.sleep(RETRY_DELAY)
                        start_byte = 0  # Start fresh
                        continue
                    else:
                        return False, f"Size verification failed after {MAX_RETRIES} attempts"
            
            elapsed = time.time() - start_time
            avg_speed = (downloaded - start_byte) / elapsed if elapsed > 0 else 0
            return True, f"Downloaded {format_size(downloaded)} @ {format_speed(avg_speed)} ✓ Verified"
            
        except (URLError, HTTPError) as e:
            if attempt < MAX_RETRIES - 1:
                safe_print(f"\n  Retry {attempt + 1}/{MAX_RETRIES} after error: {e}")
                time.sleep(RETRY_DELAY)
                start_byte = 0  # Reset to download from beginning
            else:
                return False, f"Failed after {MAX_RETRIES} attempts: {e}"
        except Exception as e:
            return False, f"Error: {e}"
    
    return False, "Unknown error"


def download_worker(task, output_dir, show_progress):
    """Worker function for parallel downloads."""
    url, filename, date_str = task
    
    # Create date-based subdirectory
    date_dir = output_dir / date_str
    date_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = date_dir / filename
    
    safe_print(f"📥 Downloading: {filename}")
    success, message = download_file(url, output_path, show_progress=show_progress)
    
    if success:
        safe_print(f"✅ {filename}: {message}")
    else:
        safe_print(f"❌ {filename}: {message}")
    
    return success, filename, message


def main():
    parser = argparse.ArgumentParser(
        description="Download GFS data from UCAR GDEX for WRF simulations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download 1 day of data (Dec 2, 2025, 00Z initialization)
  python download_gfs.py --start 2025-12-02 --end 2025-12-02

  # Download using forecast duration (48-hour forecast from single init time)
  python download_gfs.py --start 2025-12-02 --duration 48

  # Download 3 days for a Portugal FWI simulation
  python download_gfs.py --start 2025-12-02 --end 2025-12-04 --hour 00

  # Download to specific directory with custom forecast hours
  python download_gfs.py --start 2025-12-02 --end 2025-12-02 \\
      --output ~/Models/WRF_TUTORIAL/GFS_DATA \\
      --forecast-hours 0 3 6 9 12

  # Use 12Z initialization with 72-hour duration
  python download_gfs.py --start 2025-12-02 --hour 12 --duration 72
        """
    )
    
    parser.add_argument(
        '--start', '-s',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--end', '-e',
        type=str,
        required=False,
        help='End date (YYYY-MM-DD format). Required unless --duration is specified.'
    )
    
    parser.add_argument(
        '--duration', '-d',
        type=int,
        default=None,
        help='Forecast duration in hours (alternative to --end). E.g., 48 for 2-day forecast.'
    )
    
    parser.add_argument(
        '--hour', '-H',
        type=int,
        default=0,
        choices=[0, 6, 12, 18],
        help='Model initialization hour (default: 0)'
    )
    
    parser.add_argument(
        '--forecast-hours', '-f',
        type=int,
        nargs='+',
        default=DEFAULT_FORECAST_HOURS,
        help=f'Forecast hours to download (default: {DEFAULT_FORECAST_HOURS})'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='./GFS_DATA',
        help='Output directory (default: ./GFS_DATA)'
    )
    
    parser.add_argument(
        '--parallel', '-p',
        type=int,
        default=MAX_PARALLEL_DOWNLOADS,
        help=f'Number of parallel downloads (default: {MAX_PARALLEL_DOWNLOADS})'
    )
    
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress bars (useful for logging)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show files to download without downloading'
    )
    
    parser.add_argument(
        '--list-urls',
        action='store_true',
        help='Print URLs only (for use with wget/curl)'
    )
    
    args = parser.parse_args()
    
    # Parse start date
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
    except ValueError as e:
        print(f"❌ Invalid start date format: {e}")
        print("   Use YYYY-MM-DD format (e.g., 2025-12-02)")
        sys.exit(1)
    
    # Determine end date: either from --end or calculate from --duration
    if args.duration is not None:
        # Duration mode: calculate forecast hours from duration
        # For single initialization date, we need forecast files f000 to f{duration}
        end_date = start_date  # Same init date
        # Override forecast hours to cover the duration
        args.forecast_hours = list(range(0, args.duration + 1, 3))  # 3-hourly steps
        print(f"🔮 Forecast Mode: {args.duration} hours")
        print(f"   Forecast hours: f000 to f{args.duration:03d} (3-hourly)")
    elif args.end:
        # Traditional mode with explicit end date
        try:
            end_date = datetime.strptime(args.end, '%Y-%m-%d')
        except ValueError as e:
            print(f"❌ Invalid end date format: {e}")
            print("   Use YYYY-MM-DD format (e.g., 2025-12-04)")
            sys.exit(1)
    else:
        print("❌ Either --end or --duration must be specified")
        sys.exit(1)
    
    if end_date < start_date:
        print("❌ End date must be after or equal to start date")
        sys.exit(1)
    
    # Generate URLs
    tasks = generate_urls(start_date, end_date, args.hour, args.forecast_hours)
    
    if not tasks:
        print("❌ No files to download")
        sys.exit(1)
    
    # List URLs only
    if args.list_urls:
        for url, filename, date_str in tasks:
            print(url)
        sys.exit(0)
    
    # Print summary
    print("=" * 60)
    print("🌍 GFS Data Downloader for WRF")
    print("=" * 60)
    if args.duration is not None:
        print(f"📅 Start date: {args.start}")
        print(f"⏱️  Forecast duration: {args.duration} hours")
    else:
        print(f"📅 Date range: {args.start} to {args.end}")
    print(f"🕐 Initialization hour: {args.hour:02d}Z")
    print(f"📊 Forecast hours: {args.forecast_hours}")
    print(f"📁 Output directory: {args.output}")
    print(f"📦 Total files: {len(tasks)}")
    print("=" * 60)
    
    if args.dry_run:
        print("\n📋 Files to download:")
        for url, filename, date_str in tasks:
            print(f"  - {date_str}/{filename}")
        print(f"\nTotal: {len(tasks)} files")
        sys.exit(0)
    
    # Create output directory
    output_dir = Path(args.output).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n🚀 Starting download of {len(tasks)} files...")
    print(f"   Using {args.parallel} parallel connections\n")
    
    # Download files
    start_time = time.time()
    successful = 0
    failed = []
    
    if args.parallel > 1:
        # Parallel downloads
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(download_worker, task, output_dir, not args.no_progress): task
                for task in tasks
            }
            
            for future in as_completed(futures):
                success, filename, message = future.result()
                if success:
                    successful += 1
                else:
                    failed.append(filename)
    else:
        # Sequential downloads
        for task in tasks:
            success, filename, message = download_worker(task, output_dir, not args.no_progress)
            if success:
                successful += 1
            else:
                failed.append(filename)
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("📊 Download Summary")
    print("=" * 60)
    print(f"✅ Successful: {successful}/{len(tasks)}")
    print(f"❌ Failed: {len(failed)}")
    print(f"⏱️  Total time: {format_time(elapsed)}")
    print(f"📁 Files saved to: {output_dir}")
    
    if failed:
        print(f"\n⚠️  Failed files:")
        for f in failed:
            print(f"   - {f}")
    
    # WRF integration instructions
    print("\n" + "=" * 60)
    print("🔗 WRF Integration")
    print("=" * 60)
    print("To use with WPS, run these commands in your WPS directory:")
    print(f"\n  cd ~/Models/WRF_TUTORIAL/WPS-4.5")
    print(f"  ln -sf ungrib/Variable_Tables/Vtable.GFS Vtable")
    print(f"  ./link_grib.csh {output_dir}/*/*.grib2")
    print(f"  ./ungrib.exe")
    print("=" * 60)
    
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
