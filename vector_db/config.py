"""
Configuration for the Vector DB (ChromaDB) module.
"""

import os
from pathlib import Path

# ─── ChromaDB Configuration ──────────────────────────────────────────────────

# Persistent storage directory for ChromaDB
CHROMA_PERSIST_DIR = os.environ.get(
    "CHROMA_PERSIST_DIR",
    str(Path(__file__).parent.parent / "chroma_data")
)

# Collection names in ChromaDB
PROFILES_COLLECTION = "argo_profiles"
BGC_PROFILES_COLLECTION = "argo_bgc_profiles"
FLOATS_COLLECTION = "argo_floats"

# ─── Embedding Model ─────────────────────────────────────────────────────────

# sentence-transformers model name
# all-MiniLM-L6-v2: 384 dims, ~80MB, fast on CPU
EMBEDDING_MODEL = os.environ.get(
    "EMBEDDING_MODEL",
    "sentence-transformers/all-MiniLM-L6-v2"
)
EMBEDDING_DIMENSION = 384

# ─── MongoDB (read from) ─────────────────────────────────────────────────────

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.environ.get("FLOATCHAT_DB", "floatchat_ai")

MONGO_PROFILES_COLLECTION = "profiles"
MONGO_BGC_PROFILES_COLLECTION = "bgc_profiles"
MONGO_FLOATS_COLLECTION = "floats"

# ─── Processing Settings ─────────────────────────────────────────────────────

# Batch size for reading from MongoDB and inserting to ChromaDB
BATCH_SIZE = int(os.environ.get("EMBED_BATCH_SIZE", "500"))

# ─── Ocean Region Helper ─────────────────────────────────────────────────────

OCEAN_REGIONS = {
    "Arabian Sea":       {"lat": (5, 25),   "lon": (50, 75)},
    "Bay of Bengal":     {"lat": (5, 23),   "lon": (75, 95)},
    "Equatorial Indian": {"lat": (-10, 10), "lon": (40, 100)},
    "Southern Indian":   {"lat": (-60, -10),"lon": (20, 120)},
    "Western Indian":    {"lat": (-40, 10), "lon": (30, 60)},
    "Eastern Indian":    {"lat": (-40, 10), "lon": (80, 120)},
}
