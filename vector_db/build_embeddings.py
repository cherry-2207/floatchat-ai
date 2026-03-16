#!/usr/bin/env python3
"""
Build Embeddings — CLI tool to generate ChromaDB embeddings from MongoDB data.

Reads profiles and floats from MongoDB, generates text summaries,
and stores them as embeddings in ChromaDB for semantic search.

Usage:
    python -m vector_db.build_embeddings                   # Full run
    python -m vector_db.build_embeddings --limit 100       # Test with 100 profiles
    python -m vector_db.build_embeddings --collection profiles  # Only profiles
    python -m vector_db.build_embeddings --reset            # Clear and rebuild
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from pymongo import MongoClient
from tqdm import tqdm

from vector_db.config import (
    MONGO_URI, DATABASE_NAME,
    MONGO_PROFILES_COLLECTION, MONGO_BGC_PROFILES_COLLECTION,
    MONGO_FLOATS_COLLECTION, BATCH_SIZE,
)
from vector_db.summary_generator import (
    generate_profile_summary, generate_profile_metadata,
    generate_float_summary, generate_float_metadata,
)
from vector_db.vector_store import ArgoVectorStore

logger = logging.getLogger(__name__)


def build_profile_embeddings(store, db, collection_name, mongo_coll_name,
                             batch_size=500, limit=None):
    """
    Read profiles from MongoDB, generate summaries, and embed into ChromaDB.

    Args:
        store: ArgoVectorStore instance
        db: MongoDB database
        collection_name: 'profiles' or 'bgc_profiles' (which store method to use)
        mongo_coll_name: MongoDB collection name to read from
        batch_size: Number of documents per batch
        limit: Max documents to process (None = all)
    """
    coll = db[mongo_coll_name]
    total = coll.count_documents({})
    if limit:
        total = min(total, limit)

    logger.info(f"Building embeddings for {total} documents from '{mongo_coll_name}'...")

    # MongoDB projection — only fetch fields we need for summaries
    projection = {
        '_id': 1,
        'platform_number': 1,
        'cycle_number': 1,
        'direction': 1,
        'latitude': 1,
        'longitude': 1,
        'timestamp': 1,
        'max_pres': 1,
        'n_levels': 1,
        'data_mode': 1,
        'station_parameters': 1,
        'contains_bgc': 1,
        'bgc_parameters': 1,
        'project_name': 1,
        'pi_name': 1,
        'file_type': 1,
        'profile_pres_qc': 1,
        'profile_temp_qc': 1,
        'profile_psal_qc': 1,
    }

    cursor = coll.find({}, projection)
    if limit:
        cursor = cursor.limit(limit)

    # Which method to call on the store
    if collection_name == 'bgc_profiles':
        add_fn = store.add_bgc_profiles
    else:
        add_fn = store.add_profiles

    ids_batch = []
    docs_batch = []
    meta_batch = []
    processed = 0
    errors = 0

    pbar = tqdm(total=total, desc=f"Embedding {mongo_coll_name}", unit="docs")

    for doc in cursor:
        try:
            doc_id = str(doc['_id'])
            summary = generate_profile_summary(doc)
            metadata = generate_profile_metadata(doc)

            ids_batch.append(doc_id)
            docs_batch.append(summary)
            meta_batch.append(metadata)

            if len(ids_batch) >= batch_size:
                add_fn(ids_batch, docs_batch, meta_batch)
                processed += len(ids_batch)
                pbar.update(len(ids_batch))
                ids_batch = []
                docs_batch = []
                meta_batch = []

        except Exception as e:
            errors += 1
            logger.debug(f"Error processing {doc.get('_id')}: {e}")

    # Flush remaining
    if ids_batch:
        add_fn(ids_batch, docs_batch, meta_batch)
        processed += len(ids_batch)
        pbar.update(len(ids_batch))

    pbar.close()
    logger.info(f"Completed {mongo_coll_name}: {processed} embedded, {errors} errors")
    return processed, errors


def build_float_embeddings(store, db, batch_size=500, limit=None):
    """
    Read floats from MongoDB, generate summaries, and embed into ChromaDB.
    """
    coll = db[MONGO_FLOATS_COLLECTION]
    total = coll.count_documents({})
    if limit:
        total = min(total, limit)

    logger.info(f"Building embeddings for {total} float documents...")

    cursor = coll.find({})
    if limit:
        cursor = cursor.limit(limit)

    ids_batch = []
    docs_batch = []
    meta_batch = []
    processed = 0
    errors = 0

    pbar = tqdm(total=total, desc="Embedding floats", unit="docs")

    for doc in cursor:
        try:
            doc_id = str(doc['_id'])
            summary = generate_float_summary(doc)
            metadata = generate_float_metadata(doc)

            ids_batch.append(doc_id)
            docs_batch.append(summary)
            meta_batch.append(metadata)

            if len(ids_batch) >= batch_size:
                store.add_floats(ids_batch, docs_batch, meta_batch)
                processed += len(ids_batch)
                pbar.update(len(ids_batch))
                ids_batch = []
                docs_batch = []
                meta_batch = []

        except Exception as e:
            errors += 1
            logger.debug(f"Error processing float {doc.get('_id')}: {e}")

    # Flush remaining
    if ids_batch:
        store.add_floats(ids_batch, docs_batch, meta_batch)
        processed += len(ids_batch)
        pbar.update(len(ids_batch))

    pbar.close()
    logger.info(f"Completed floats: {processed} embedded, {errors} errors")
    return processed, errors


def main():
    parser = argparse.ArgumentParser(
        description='Build ChromaDB embeddings from MongoDB ARGO data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m vector_db.build_embeddings                       # Full run (all collections)
  python -m vector_db.build_embeddings --limit 100           # Test with 100 docs per collection
  python -m vector_db.build_embeddings --collection profiles # Only core profiles
  python -m vector_db.build_embeddings --collection floats   # Only floats
  python -m vector_db.build_embeddings --reset               # Clear and rebuild everything
  python -m vector_db.build_embeddings --batch-size 250      # Smaller batches for low RAM
        """
    )

    parser.add_argument(
        '--limit', type=int, default=None,
        help='Limit documents per collection (for testing)'
    )
    parser.add_argument(
        '--collection', type=str, default='all',
        choices=['all', 'profiles', 'bgc_profiles', 'floats'],
        help='Which collection to process (default: all)'
    )
    parser.add_argument(
        '--batch-size', type=int, default=BATCH_SIZE,
        help=f'Batch size for processing (default: {BATCH_SIZE})'
    )
    parser.add_argument(
        '--reset', action='store_true',
        help='Delete all existing embeddings before rebuilding'
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
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║     FloatChat-AI: Vector Embedding Builder (ChromaDB)      ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    # Connect to MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DATABASE_NAME]
        print(f"  ✅ MongoDB connected ({MONGO_URI})")
    except Exception as e:
        print(f"  ❌ Cannot connect to MongoDB: {e}")
        sys.exit(1)

    # Print MongoDB stats
    profiles_count = db[MONGO_PROFILES_COLLECTION].count_documents({})
    bgc_count = db[MONGO_BGC_PROFILES_COLLECTION].count_documents({})
    floats_count = db[MONGO_FLOATS_COLLECTION].count_documents({})
    print(f"  📊 MongoDB: {profiles_count:,} profiles, {bgc_count:,} BGC, {floats_count:,} floats")
    print()

    # Initialize vector store
    store = ArgoVectorStore()
    print(f"  ✅ ChromaDB initialized")

    # Print existing stats
    stats = store.get_stats()
    print(f"  📊 ChromaDB: {stats['profiles']:,} profiles, "
          f"{stats['bgc_profiles']:,} BGC, {stats['floats']:,} floats")

    if args.reset:
        print(f"\n  🗑️  Resetting all collections...")
        store.delete_all()
        stats = store.get_stats()
        print(f"  📊 After reset: {stats['profiles']} profiles, "
              f"{stats['bgc_profiles']} BGC, {stats['floats']} floats")

    print(f"\n  Settings: batch_size={args.batch_size}, "
          f"limit={args.limit or 'all'}, collection={args.collection}")
    print()

    start_time = time.time()
    total_processed = 0
    total_errors = 0

    # ── Build embeddings ──────────────────────────────────────────

    if args.collection in ('all', 'profiles'):
        p, e = build_profile_embeddings(
            store, db, 'profiles', MONGO_PROFILES_COLLECTION,
            batch_size=args.batch_size, limit=args.limit,
        )
        total_processed += p
        total_errors += e

    if args.collection in ('all', 'bgc_profiles'):
        p, e = build_profile_embeddings(
            store, db, 'bgc_profiles', MONGO_BGC_PROFILES_COLLECTION,
            batch_size=args.batch_size, limit=args.limit,
        )
        total_processed += p
        total_errors += e

    if args.collection in ('all', 'floats'):
        p, e = build_float_embeddings(
            store, db, batch_size=args.batch_size, limit=args.limit,
        )
        total_processed += p
        total_errors += e

    # ── Summary ───────────────────────────────────────────────────

    elapsed = time.time() - start_time
    final_stats = store.get_stats()

    print()
    print("=" * 70)
    print("  VECTOR EMBEDDING BUILDER — SUMMARY")
    print("=" * 70)
    print(f"  Documents processed:    {total_processed:,}")
    print(f"  Errors:                 {total_errors:,}")
    print(f"  Time elapsed:           {elapsed:.1f}s ({elapsed/60:.1f} min)")
    if total_processed > 0:
        print(f"  Rate:                   {total_processed/elapsed:.1f} docs/sec")
    print()
    print(f"  ChromaDB Final Counts:")
    print(f"    argo_profiles:        {final_stats['profiles']:,}")
    print(f"    argo_bgc_profiles:    {final_stats['bgc_profiles']:,}")
    print(f"    argo_floats:          {final_stats['floats']:,}")
    print("=" * 70)
    print()


if __name__ == '__main__':
    main()
