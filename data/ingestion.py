"""
ARGO Data Ingestion Pipeline — Orchestrates parsing and MongoDB insertion.

Features:
  - Discovers all NC files across float directories
  - Parallel processing with multiprocessing.Pool
  - Batch upserts via pymongo bulk_write
  - Builds aggregated floats collection after profile insertion
  - Robust error handling (per-file, never halts the whole pipeline)
  - Progress tracking via tqdm
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count
from collections import defaultdict

import pymongo
from pymongo import UpdateOne
from tqdm import tqdm

from data.config import (
    MONGO_URI, DATABASE_NAME,
    PROFILES_COLLECTION, BGC_PROFILES_COLLECTION, FLOATS_COLLECTION,
    BASE_DATA_DIR, BATCH_SIZE, NUM_WORKERS, BGC_PARAMS,
)
from data.nc_parser import ArgoNCParser

logger = logging.getLogger(__name__)


# ─── Worker function (must be top-level for multiprocessing) ─────────────────

def parse_single_file(filepath):
    """
    Parse a single NetCDF file and return (file_type, profiles, error).
    This runs in a worker process.
    """
    try:
        parser = ArgoNCParser(filepath)
        profiles = parser.parse()
        return (parser.file_type, profiles, None)
    except Exception as e:
        return (None, [], str(e))


# ─── Main Pipeline ───────────────────────────────────────────────────────────

class ArgoIngestionPipeline:
    """
    Orchestrates the full ingestion pipeline:
      1. Discover all NC files
      2. Parse them in parallel batches
      3. Upsert into MongoDB (profiles / bgc_profiles)
      4. Build aggregated floats collection
    """

    def __init__(self, data_dir=None, batch_size=None, num_workers=None,
                 limit=None, db_name=None):
        self.data_dir = Path(data_dir or BASE_DATA_DIR)
        self.batch_size = batch_size or BATCH_SIZE
        self.num_workers = min(num_workers or NUM_WORKERS, cpu_count())
        self.limit = limit
        self.db_name = db_name or DATABASE_NAME
        
        # Connect to MongoDB
        self.client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.db_name]
        
        # Stats
        self.stats = {
            'files_discovered': 0,
            'files_processed': 0,
            'files_errored': 0,
            'profiles_inserted': 0,
            'bgc_profiles_inserted': 0,
            'start_time': None,
            'end_time': None,
        }
        self.errors = []

    def run(self):
        """Execute the full ingestion pipeline."""
        self.stats['start_time'] = datetime.utcnow()
        
        # Step 1: Check MongoDB connection
        self._check_connection()
        
        # Step 2: Create indexes
        self._create_indexes()
        
        # Step 3: Discover files
        all_files = self._discover_files()
        self.stats['files_discovered'] = len(all_files)
        logger.info(f"Discovered {len(all_files)} NetCDF files")
        
        if self.limit:
            all_files = all_files[:self.limit]
            logger.info(f"Limited to {len(all_files)} files")
        
        # Step 4: Process in batches
        self._process_files(all_files)
        
        # Step 5: Build floats collection
        self._build_floats_collection()
        
        self.stats['end_time'] = datetime.utcnow()
        self._print_summary()

    def _check_connection(self):
        """Verify MongoDB is reachable."""
        try:
            self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB at {MONGO_URI}")
        except pymongo.errors.ServerSelectionTimeoutError:
            logger.error(
                f"Cannot connect to MongoDB at {MONGO_URI}. "
                "Please ensure MongoDB is running."
            )
            sys.exit(1)

    def _create_indexes(self):
        """Create MongoDB indexes for efficient queries."""
        logger.info("Creating indexes...")
        
        for coll_name in [PROFILES_COLLECTION, BGC_PROFILES_COLLECTION]:
            coll = self.db[coll_name]
            coll.create_index([("geo_location", pymongo.GEOSPHERE)])
            coll.create_index("platform_number")
            coll.create_index("timestamp")
            coll.create_index([("platform_number", 1), ("cycle_number", 1)])
            coll.create_index([("latitude", 1), ("longitude", 1)])
            coll.create_index("data_mode")
        
        self.db[FLOATS_COLLECTION].create_index(
            "platform_number", unique=True
        )
        self.db[FLOATS_COLLECTION].create_index("has_bgc")
        
        logger.info("Indexes created successfully")

    def _discover_files(self):
        """Discover all NetCDF files in the data directory."""
        all_files = []
        
        if not self.data_dir.exists():
            logger.error(f"Data directory not found: {self.data_dir}")
            sys.exit(1)
        
        for float_dir in sorted(self.data_dir.iterdir()):
            if not float_dir.is_dir():
                continue
            profiles_dir = float_dir / 'profiles'
            if not profiles_dir.is_dir():
                continue
            
            for nc_file in sorted(profiles_dir.glob('*.nc')):
                all_files.append(str(nc_file))
        
        return all_files

    def _process_files(self, all_files):
        """Process all files in batches with parallel workers."""
        total_batches = (len(all_files) + self.batch_size - 1) // self.batch_size
        
        logger.info(
            f"Processing {len(all_files)} files in {total_batches} batches "
            f"({self.batch_size} files/batch, {self.num_workers} workers)"
        )
        
        pbar = tqdm(total=len(all_files), desc="Ingesting", unit="files")
        
        for batch_idx in range(total_batches):
            start = batch_idx * self.batch_size
            end = min(start + self.batch_size, len(all_files))
            batch_files = all_files[start:end]
            
            # Parse batch in parallel
            core_docs = []
            bgc_docs = []
            
            with Pool(processes=self.num_workers) as pool:
                results = pool.map(parse_single_file, batch_files)
            
            for filepath, (file_type, profiles, error) in zip(batch_files, results):
                if error:
                    self.stats['files_errored'] += 1
                    self.errors.append((filepath, error))
                    logger.debug(f"Error in {filepath}: {error}")
                else:
                    self.stats['files_processed'] += 1
                    for doc in profiles:
                        if file_type == 'synthetic_bgc':
                            bgc_docs.append(doc)
                        else:
                            core_docs.append(doc)
            
            # Bulk upsert to MongoDB
            if core_docs:
                inserted = self._bulk_upsert(
                    PROFILES_COLLECTION, core_docs
                )
                self.stats['profiles_inserted'] += inserted
            
            if bgc_docs:
                inserted = self._bulk_upsert(
                    BGC_PROFILES_COLLECTION, bgc_docs
                )
                self.stats['bgc_profiles_inserted'] += inserted
            
            pbar.update(len(batch_files))
        
        pbar.close()

    def _bulk_upsert(self, collection_name, docs):
        """
        Bulk upsert documents into MongoDB.
        Uses UpdateOne with upsert=True for idempotent ingestion.
        Returns count of successful operations.
        """
        if not docs:
            return 0
        
        coll = self.db[collection_name]
        operations = []
        
        for doc in docs:
            doc_id = doc.pop('_id', None)
            if doc_id is None:
                # Generate an ID if missing
                doc_id = f"{doc.get('platform_number', 'UNK')}_{doc.get('cycle_number', 0):03d}"
            
            operations.append(
                UpdateOne(
                    {'_id': doc_id},
                    {'$set': doc},
                    upsert=True
                )
            )
        
        try:
            result = coll.bulk_write(operations, ordered=False)
            return result.upserted_count + result.modified_count
        except pymongo.errors.BulkWriteError as bwe:
            # Some documents may have succeeded
            logger.warning(
                f"Bulk write partial error in {collection_name}: "
                f"{bwe.details.get('nInserted', 0)} inserted, "
                f"{len(bwe.details.get('writeErrors', []))} errors"
            )
            return bwe.details.get('nInserted', 0)
        except Exception as e:
            logger.error(f"Bulk write error in {collection_name}: {e}")
            return 0

    def _build_floats_collection(self):
        """
        Build the aggregated floats collection by querying profiles.
        One document per unique platform_number.
        """
        logger.info("Building floats collection...")
        
        # Aggregate from profiles
        pipeline = [
            {
                '$group': {
                    '_id': '$platform_number',
                    'project_name': {'$first': '$project_name'},
                    'pi_name': {'$first': '$pi_name'},
                    'platform_type': {'$first': '$platform_type'},
                    'wmo_inst_type': {'$first': '$wmo_inst_type'},
                    'data_centre': {'$first': '$data_centre'},
                    'total_cycles': {'$sum': 1},
                    'first_date': {'$min': '$timestamp'},
                    'last_date': {'$max': '$timestamp'},
                    'min_lat': {'$min': '$latitude'},
                    'max_lat': {'$max': '$latitude'},
                    'min_lon': {'$min': '$longitude'},
                    'max_lon': {'$max': '$longitude'},
                    'data_modes_used': {'$addToSet': '$data_mode'},
                }
            }
        ]
        
        float_docs = {}
        
        # From core profiles
        for doc in self.db[PROFILES_COLLECTION].aggregate(pipeline):
            platform = doc['_id']
            float_docs[platform] = {
                '_id': platform,
                'platform_number': platform,
                'project_name': doc['project_name'],
                'pi_name': doc['pi_name'],
                'platform_type': doc['platform_type'],
                'wmo_inst_type': doc['wmo_inst_type'],
                'data_centre': doc['data_centre'],
                'total_cycles': doc['total_cycles'],
                'has_bgc': False,
                'bgc_parameters': [],
                'first_date': doc['first_date'],
                'last_date': doc['last_date'],
                'geo_bounding_box': {
                    'min_lat': doc['min_lat'],
                    'max_lat': doc['max_lat'],
                    'min_lon': doc['min_lon'],
                    'max_lon': doc['max_lon'],
                },
                'data_modes_used': doc['data_modes_used'],
            }
        
        # Merge BGC info
        bgc_pipeline = [
            {
                '$group': {
                    '_id': '$platform_number',
                    'bgc_cycles': {'$sum': 1},
                    'bgc_parameters': {'$first': '$bgc_parameters'},
                    'min_lat': {'$min': '$latitude'},
                    'max_lat': {'$max': '$latitude'},
                    'min_lon': {'$min': '$longitude'},
                    'max_lon': {'$max': '$longitude'},
                    'first_date': {'$min': '$timestamp'},
                    'last_date': {'$max': '$timestamp'},
                }
            }
        ]
        
        for doc in self.db[BGC_PROFILES_COLLECTION].aggregate(bgc_pipeline):
            platform = doc['_id']
            if platform in float_docs:
                float_docs[platform]['has_bgc'] = True
                float_docs[platform]['bgc_parameters'] = doc.get('bgc_parameters', [])
                float_docs[platform]['bgc_cycles'] = doc['bgc_cycles']
                # Extend bounding box
                bb = float_docs[platform]['geo_bounding_box']
                if doc['min_lat'] is not None:
                    bb['min_lat'] = min(filter(None, [bb['min_lat'], doc['min_lat']]))
                if doc['max_lat'] is not None:
                    bb['max_lat'] = max(filter(None, [bb['max_lat'], doc['max_lat']]))
                if doc['min_lon'] is not None:
                    bb['min_lon'] = min(filter(None, [bb['min_lon'], doc['min_lon']]))
                if doc['max_lon'] is not None:
                    bb['max_lon'] = max(filter(None, [bb['max_lon'], doc['max_lon']]))
            else:
                # Float only in BGC collection
                float_docs[platform] = {
                    '_id': platform,
                    'platform_number': platform,
                    'project_name': None,
                    'pi_name': None,
                    'platform_type': None,
                    'wmo_inst_type': None,
                    'data_centre': None,
                    'total_cycles': doc['bgc_cycles'],
                    'has_bgc': True,
                    'bgc_parameters': doc.get('bgc_parameters', []),
                    'bgc_cycles': doc['bgc_cycles'],
                    'first_date': doc['first_date'],
                    'last_date': doc['last_date'],
                    'geo_bounding_box': {
                        'min_lat': doc['min_lat'],
                        'max_lat': doc['max_lat'],
                        'min_lon': doc['min_lon'],
                        'max_lon': doc['max_lon'],
                    },
                    'data_modes_used': [],
                }
        
        # Upsert all float docs
        if float_docs:
            operations = []
            for fid, fdoc in float_docs.items():
                doc_id = fdoc.pop('_id')
                operations.append(
                    UpdateOne(
                        {'_id': doc_id},
                        {'$set': fdoc},
                        upsert=True
                    )
                )
            
            try:
                result = self.db[FLOATS_COLLECTION].bulk_write(operations, ordered=False)
                logger.info(
                    f"Floats collection: {result.upserted_count} inserted, "
                    f"{result.modified_count} updated"
                )
            except Exception as e:
                logger.error(f"Error building floats collection: {e}")
        
        logger.info(f"Floats collection built with {len(float_docs)} floats")

    def _print_summary(self):
        """Print final ingestion summary."""
        elapsed = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        print("\n" + "=" * 70)
        print("  ARGO DATA INGESTION — SUMMARY")
        print("=" * 70)
        print(f"  Database:              {self.db_name}")
        print(f"  Files discovered:      {self.stats['files_discovered']:,}")
        print(f"  Files processed:       {self.stats['files_processed']:,}")
        print(f"  Files errored:         {self.stats['files_errored']:,}")
        print(f"  Core profiles:         {self.stats['profiles_inserted']:,}")
        print(f"  BGC profiles:          {self.stats['bgc_profiles_inserted']:,}")
        print(f"  Time elapsed:          {elapsed:.1f} seconds ({elapsed/60:.1f} min)")
        if self.stats['files_processed'] > 0:
            rate = self.stats['files_processed'] / elapsed
            print(f"  Processing rate:       {rate:.1f} files/sec")
        print("=" * 70)
        
        # Collection stats
        print(f"\n  MongoDB Collection Counts:")
        print(f"    profiles:            {self.db[PROFILES_COLLECTION].count_documents({}):,}")
        print(f"    bgc_profiles:        {self.db[BGC_PROFILES_COLLECTION].count_documents({}):,}")
        print(f"    floats:              {self.db[FLOATS_COLLECTION].count_documents({}):,}")
        print()
        
        if self.errors:
            print(f"  First 10 errors:")
            for path, err in self.errors[:10]:
                print(f"    {Path(path).name}: {err}")
            if len(self.errors) > 10:
                print(f"    ... and {len(self.errors) - 10} more")
            print()
