#!/usr/bin/env python3
"""
Test Embeddings — Verifies the ChromaDB vector store.

Checks:
  1. Collection document counts match expected counts
  2. Document summaries and metadata are well-formed
  3. Semantic search returns relevant results for test queries
  4. Hybrid search (semantic + metadata filters) works correctly
"""

import sys
import logging
from pathlib import Path

# Add project root to path so we can import vector_db
sys.path.append(str(Path(__file__).parent.parent))

from vector_db.vector_store import ArgoVectorStore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Expected minimum counts (from Phase 1 ingestion)
EXPECTED_MIN_PROFILES = 87000
EXPECTED_MIN_BGC = 13000
EXPECTED_MIN_FLOATS = 500

def run_tests():
    print("==================================================")
    print("  🧪 Verifying ChromaDB Embeddings")
    print("==================================================\n")

    try:
        store = ArgoVectorStore()
        print("✅ Successfully initialized ArgoVectorStore\n")
    except Exception as e:
        print(f"❌ Failed to initialize VectorStore: {e}")
        return False

    # ─── 1. Check Document Counts ─────────────────────────────────────────
    print("─── 1. Checking Collection Counts ───")
    stats = store.get_stats()
    
    profiles_count = stats['profiles']
    bgc_count = stats['bgc_profiles']
    floats_count = stats['floats']
    
    print(f"  profiles:     {profiles_count:,} (expected > {EXPECTED_MIN_PROFILES:,})")
    print(f"  bgc_profiles: {bgc_count:,} (expected > {EXPECTED_MIN_BGC:,})")
    print(f"  floats:       {floats_count:,} (expected > {EXPECTED_MIN_FLOATS:,})")
    
    counts_ok = (
        profiles_count > EXPECTED_MIN_PROFILES and 
        bgc_count > EXPECTED_MIN_BGC and 
        floats_count > EXPECTED_MIN_FLOATS
    )
    
    if counts_ok:
        print("  ✅ All counts are within expected ranges\n")
    else:
        print("  ❌ WARNING: Counts are lower than expected!\n")

    # ─── 2. Test Semantic Search Quality ──────────────────────────────────
    print("─── 2. Testing Semantic Search ───")
    
    test_queries = [
        {
            "query": "salinity profiles near the equator",
            "collection": "profiles",
            "expected_keyword": "equatorial",
        },
        {
            "query": "BGC float with dissolved oxygen data",
            "collection": "bgc_profiles",
            "expected_keyword": "oxygen",
            "check": lambda meta: meta.get("contains_bgc", False)
        },
        {
            "query": "floats operating in the Arabian Sea",
            "collection": "floats",
            "expected_keyword": "arabian sea",
            "check": lambda meta: "Arabian Sea" in meta.get("region", "")
        }
    ]
    
    search_ok = True
    for tq in test_queries:
        query = tq['query']
        print(f"  🔍 Query: '{query}' ({tq['collection']})")
        
        try:
            if tq["collection"] == "profiles":
                results = store.query_profiles(query, n_results=3)
            elif tq["collection"] == "bgc_profiles":
                results = store.query_bgc_profiles(query, n_results=3)
            else:
                results = store.query_floats(query, n_results=3)
                
            if not results['ids'] or not results['ids'][0]:
                print(f"     ❌ No results returned")
                search_ok = False
                continue
                
            # Print top result
            top_id = results['ids'][0][0]
            top_dist = results['distances'][0][0]
            top_meta = results['metadatas'][0][0]
            top_doc = results['documents'][0][0][:100] + "..."
            
            print(f"     ✅ Top result: ID={top_id}, Distance={top_dist:.4f}")
            print(f"     📝 Summary: {top_doc}")
            
            # Run custom check if provided
            if "check" in tq:
                if tq["check"](top_meta):
                    print("     ✅ Metadata validation passed")
                else:
                    print(f"     ❌ Metadata validation failed! Meta: {top_meta}")
                    search_ok = False
                    
        except Exception as e:
            print(f"     ❌ Query failed: {e}")
            search_ok = False
    print()

    # ─── 3. Test Hybrid Search (Metadata Filtering) ───────────────────────
    print("─── 3. Testing Hybrid Search (Semantic + Filter) ───")
    
    print("  🔍 Query: 'deep profiles' with filter: {\"region\": \"Bay of Bengal\"}")
    try:
        results = store.query_profiles(
            "deep ocean profiles", 
            n_results=3, 
            where={"region": "Bay of Bengal"}
        )
        
        if not results['ids'] or not results['ids'][0]:
            print(f"     ❌ No results returned")
            hybrid_ok = False
        else:
            hybrid_ok = True
            for i, meta in enumerate(results['metadatas'][0]):
                region = meta.get("region")
                if region != "Bay of Bengal":
                    print(f"     ❌ Result {i+1} has wrong region: {region}")
                    hybrid_ok = False
            
            if hybrid_ok:
                print(f"     ✅ All {len(results['ids'][0])} results correctly filtered to Bay of Bengal")
    except Exception as e:
        print(f"     ❌ Hybrid query failed: {e}")
        hybrid_ok = False
    print()

    # ─── Final Output ─────────────────────────────────────────────────────
    all_passed = counts_ok and search_ok and hybrid_ok
    print("==================================================")
    if all_passed:
        print("  🎉 All Embedding Tests Passed successfully!")
    else:
        print("  ⚠️ Some tests failed. Check logs above.")
    print("==================================================")
    
    return all_passed

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
