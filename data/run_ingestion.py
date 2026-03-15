#!/usr/bin/env python3
"""
CLI entry point for the ARGO data ingestion pipeline.

Usage:
    python run_ingestion.py                         # Full ingestion
    python run_ingestion.py --limit 100             # Ingest first 100 files only
    python run_ingestion.py --batch-size 250        # Smaller batches
    python run_ingestion.py --workers 2             # Fewer workers
    python run_ingestion.py --db floatchat_test      # Different database name
"""

import argparse
import logging
import sys

from data.ingestion import ArgoIngestionPipeline
from data.config import BASE_DATA_DIR, BATCH_SIZE, NUM_WORKERS, DATABASE_NAME


def main():
    parser = argparse.ArgumentParser(
        description='ARGO NetCDF Data Ingestion Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_ingestion.py                        # Full ingestion (~100K files)
  python run_ingestion.py --limit 500            # Quick test with 500 files
  python run_ingestion.py --batch-size 250       # Smaller batches for low memory  
  python run_ingestion.py --workers 2            # Fewer parallel workers
  python run_ingestion.py --db floatchat_test    # Use a test database
        """
    )
    
    parser.add_argument(
        '--data-dir', type=str, default=BASE_DATA_DIR,
        help=f'Path to ARGO data directory (default: {BASE_DATA_DIR})'
    )
    parser.add_argument(
        '--batch-size', type=int, default=BATCH_SIZE,
        help=f'Number of files per batch (default: {BATCH_SIZE})'
    )
    parser.add_argument(
        '--workers', type=int, default=NUM_WORKERS,
        help=f'Number of parallel workers (default: {NUM_WORKERS})'
    )
    parser.add_argument(
        '--limit', type=int, default=None,
        help='Limit total files to process (for testing)'
    )
    parser.add_argument(
        '--db', type=str, default=DATABASE_NAME,
        help=f'MongoDB database name (default: {DATABASE_NAME})'
    )
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║       FloatChat-AI: ARGO Data Ingestion Pipeline           ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()
    print(f"  Data directory:   {args.data_dir}")
    print(f"  Database:         {args.db}")
    print(f"  Batch size:       {args.batch_size}")
    print(f"  Workers:          {args.workers}")
    print(f"  Limit:            {args.limit or 'None (full ingestion)'}")
    print(f"  Log level:        {args.log_level}")
    print()
    
    # Run pipeline
    pipeline = ArgoIngestionPipeline(
        data_dir=args.data_dir,
        batch_size=args.batch_size,
        num_workers=args.workers,
        limit=args.limit,
        db_name=args.db,
    )
    
    try:
        pipeline.run()
    except KeyboardInterrupt:
        print("\n\nIngestion interrupted by user. Partial data may have been inserted.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
