#!/usr/bin/env python3
"""
GFS Data Downloader for WRF Simulations (AWS S3 Source)
========================================================
Downloads NOAA GFS 0.25° data from the Registry of Open Data on AWS
(noaa-gfs-bdp-pds S3 bucket) for WRF preprocessing.

Features:
- Downloads from AWS S3 public bucket (no credentials required)
- Progress bar with download speed
- Resume capability for interrupted downloads
- Parallel downloads for faster retrieval
- Automatic retry on failure
- Organizes files into date-based directories

Usage:
    python download.py --start 2025-12-02 --end 2025-12-04 --hour 00
    python download.py --start 2025-12-02 --duration 48
    python download.py --start 2025-12-02 --end 2025-12-04 --hour 00 --output ~/Models/WRF_TUTORIAL/GFS_DATA

Author: WRF Project
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
except ImportError:
    print("❌ boto3 is required. Install it with: pip install boto3")
    sys.exit(1)

# ============================================
# CONFIGURATION
# ============================================
GFS_BUCKET_NAME = "noaa-gfs-bdp-pds"

# GFS model parameters
FORECAST_MODEL = "atmos"
FILE_TYPE = "pgrb2"
GRID_RESOLUTION = "0p25"

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


def create_s3_client():
    """Create an anonymous S3 client for accessing the public GFS bucket."""
    return boto3.client('s3', config=Config(signature_version=UNSIGNED))


def generate_tasks(start_date, end_date, init_hour, forecast_hours):
    """
    Generate download tasks for GFS data from AWS S3.

    Args:
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
        init_hour: Model initialization hour (0, 6, 12, or 18)
        forecast_hours: List of forecast hours to download

    Returns:
        List of (s3_key, local_filename, date_str) tuples
    """
    tasks = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        cycle = f"{init_hour:02d}"

        for fhr in forecast_hours:
            # S3 key: gfs.YYYYMMDD/CC/atmos/gfs.tCCz.pgrb2.0p25.fFFF
            object_key = f"gfs.t{cycle}z.{FILE_TYPE}.{GRID_RESOLUTION}.f{fhr:03d}"
            s3_key = f"gfs.{date_str}/{cycle}/{FORECAST_MODEL}/{object_key}"

            # Local filename matches the original download_gfs.py convention
            local_filename = f"gfs.0p25.{date_str}{cycle}.f{fhr:03d}.grib2"
            tasks.append((s3_key, local_filename, date_str))

        current_date += timedelta(days=1)

    return tasks


def get_s3_object_size(s3_client, bucket, key):
    """Get the size of an S3 object."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception:
        return None


def download_file_s3(s3_client, s3_key, output_path, show_progress=True):
    """
    Download a single file from S3 with progress tracking and resume capability.

    Args:
        s3_client: boto3 S3 client
        s3_key: S3 object key
        output_path: Local file path to save to
        show_progress: Whether to show progress bar

    Returns:
        Tuple of (success: bool, message: str)
    """
    output_path = Path(output_path)
    temp_path = output_path.with_suffix('.grib2.part')

    for attempt in range(MAX_RETRIES):
        try:
            # Get remote file size
            remote_size = get_s3_object_size(s3_client, GFS_BUCKET_NAME, s3_key)

            if remote_size is None:
                return False, f"S3 object not found: {s3_key}"

            # Check if file already exists and is complete
            if output_path.exists():
                local_size = output_path.stat().st_size
                if local_size == remote_size:
                    return True, f"Already downloaded ({format_size(local_size)}) ✓ Verified"
                else:
                    safe_print(f"  ⚠️  Existing file has wrong size ({format_size(local_size)} vs {format_size(remote_size)}), re-downloading...")
                    output_path.unlink()

            # Check for partial download and determine resume point
            start_byte = 0
            if temp_path.exists():
                start_byte = temp_path.stat().st_size
                if start_byte >= remote_size:
                    # Partial file is larger or equal, start fresh
                    temp_path.unlink()
                    start_byte = 0

            # Download with progress tracking
            downloaded = start_byte
            start_time = time.time()
            mode = 'ab' if start_byte > 0 else 'wb'

            # Use range get for resume
            extra_args = {}
            if start_byte > 0:
                extra_args['Range'] = f'bytes={start_byte}-'
                safe_print(f"  ↪ Resuming from {format_size(start_byte)}")

            # Use get_object for streaming with progress
            get_kwargs = {'Bucket': GFS_BUCKET_NAME, 'Key': s3_key}
            get_kwargs.update(extra_args)
            response = s3_client.get_object(**get_kwargs)
            body = response['Body']

            with open(temp_path, mode) as f:
                while True:
                    chunk = body.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
                    downloaded += len(chunk)

                    if show_progress and remote_size:
                        elapsed = time.time() - start_time
                        speed = (downloaded - start_byte) / elapsed if elapsed > 0 else 0
                        percent = (downloaded / remote_size) * 100
                        eta = (remote_size - downloaded) / speed if speed > 0 else 0

                        progress = f"\r  [{percent:5.1f}%] {format_size(downloaded)}/{format_size(remote_size)} "
                        progress += f"@ {format_speed(speed)} ETA: {format_time(eta)}  "
                        sys.stdout.write(progress)
                        sys.stdout.flush()

            if show_progress:
                sys.stdout.write("\n")

            # Verify downloaded file size
            final_size = temp_path.stat().st_size
            if final_size != remote_size:
                safe_print(f"  ⚠️  Size mismatch: got {format_size(final_size)}, expected {format_size(remote_size)}")
                temp_path.unlink()
                if attempt < MAX_RETRIES - 1:
                    safe_print(f"  Retrying download ({attempt + 2}/{MAX_RETRIES})...")
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    return False, f"Size verification failed after {MAX_RETRIES} attempts"

            # Rename temp file to final name
            temp_path.rename(output_path)

            elapsed = time.time() - start_time
            avg_speed = (downloaded - start_byte) / elapsed if elapsed > 0 else 0
            return True, f"Downloaded {format_size(downloaded)} @ {format_speed(avg_speed)} ✓ Verified"

        except s3_client.exceptions.NoSuchKey:
            return False, f"S3 object not found: {s3_key}"
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                safe_print(f"\n  Retry {attempt + 1}/{MAX_RETRIES} after error: {e}")
                time.sleep(RETRY_DELAY)
                start_byte = 0  # Reset for clean retry
            else:
                return False, f"Failed after {MAX_RETRIES} attempts: {e}"

    return False, "Unknown error"


def download_worker(s3_client, task, output_dir, show_progress):
    """Worker function for parallel downloads."""
    s3_key, filename, date_str = task

    # Create date-based subdirectory
    date_dir = output_dir / date_str
    date_dir.mkdir(parents=True, exist_ok=True)

    output_path = date_dir / filename

    safe_print(f"📥 Downloading: {filename}")
    success, message = download_file_s3(s3_client, s3_key, output_path, show_progress=show_progress)

    if success:
        safe_print(f"✅ {filename}: {message}")
    else:
        safe_print(f"❌ {filename}: {message}")

    return success, filename, message


def list_available_files(s3_client, date_str, init_hour):
    """List available GFS files on S3 for a given date and init hour."""
    cycle = f"{init_hour:02d}"
    prefix = f"gfs.{date_str}/{cycle}/{FORECAST_MODEL}/"

    try:
        response = s3_client.list_objects_v2(
            Bucket=GFS_BUCKET_NAME,
            Prefix=prefix,
            Delimiter='/'
        )

        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append(obj['Key'])

        # Handle pagination
        while response.get('IsTruncated', False):
            response = s3_client.list_objects_v2(
                Bucket=GFS_BUCKET_NAME,
                Prefix=prefix,
                Delimiter='/',
                ContinuationToken=response['NextContinuationToken']
            )
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append(obj['Key'])

        return files
    except Exception as e:
        safe_print(f"⚠️  Error listing S3 objects: {e}")
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Download GFS data from AWS S3 (noaa-gfs-bdp-pds) for WRF simulations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download 1 day of data (Dec 2, 2025, 00Z initialization)
  python download.py --start 2025-12-02 --end 2025-12-02

  # Download using forecast duration (48-hour forecast from single init time)
  python download.py --start 2025-12-02 --duration 48

  # Download 3 days for a Portugal FWI simulation
  python download.py --start 2025-12-02 --end 2025-12-04 --hour 00

  # Download to specific directory with custom forecast hours
  python download.py --start 2025-12-02 --end 2025-12-02 \\
      --output ~/Models/WRF_TUTORIAL/GFS_DATA \\
      --forecast-hours 0 3 6 9 12

  # Use 12Z initialization with 72-hour duration
  python download.py --start 2025-12-02 --hour 12 --duration 72

  # List available files on S3 for a specific date
  python download.py --start 2025-12-02 --list-available
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
        '--list-keys',
        action='store_true',
        help='Print S3 keys only (for debugging)'
    )

    parser.add_argument(
        '--list-available',
        action='store_true',
        help='List all available files on S3 for the given date and init hour'
    )

    args = parser.parse_args()

    # Parse start date
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
    except ValueError as e:
        print(f"❌ Invalid start date format: {e}")
        print("   Use YYYY-MM-DD format (e.g., 2025-12-02)")
        sys.exit(1)

    # Create S3 client
    s3_client = create_s3_client()

    # List available files mode
    if args.list_available:
        date_str = start_date.strftime("%Y%m%d")
        print(f"📋 Available GFS files on S3 for {date_str} {args.hour:02d}Z:\n")
        files = list_available_files(s3_client, date_str, args.hour)
        if files:
            for f in files:
                print(f"  {f}")
            print(f"\nTotal: {len(files)} files")
        else:
            print("  No files found.")
        sys.exit(0)

    # Determine end date: either from --end or calculate from --duration
    if args.duration is not None:
        end_date = start_date  # Same init date
        args.forecast_hours = list(range(0, args.duration + 1, 3))  # 3-hourly steps
        print(f"🔮 Forecast Mode: {args.duration} hours")
        print(f"   Forecast hours: f000 to f{args.duration:03d} (3-hourly)")
    elif args.end:
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

    # Generate tasks
    tasks = generate_tasks(start_date, end_date, args.hour, args.forecast_hours)

    if not tasks:
        print("❌ No files to download")
        sys.exit(1)

    # List S3 keys only
    if args.list_keys:
        for s3_key, filename, date_str in tasks:
            print(f"s3://{GFS_BUCKET_NAME}/{s3_key}")
        sys.exit(0)

    # Print summary
    print("=" * 60)
    print("🌍 GFS Data Downloader for WRF (AWS S3 Source)")
    print("=" * 60)
    print(f"☁️  Source: s3://{GFS_BUCKET_NAME}/")
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
        for s3_key, filename, date_str in tasks:
            print(f"  - {date_str}/{filename}  (s3://.../{s3_key})")
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
        # Parallel downloads — each thread gets its own S3 client
        def parallel_worker(task):
            thread_s3 = create_s3_client()
            return download_worker(thread_s3, task, output_dir, not args.no_progress)

        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(parallel_worker, task): task
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
            success, filename, message = download_worker(s3_client, task, output_dir, not args.no_progress)
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
