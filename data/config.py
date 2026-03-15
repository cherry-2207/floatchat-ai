"""
Configuration for FloatChat-AI ARGO Data Ingestion Pipeline.
"""

import os

# ─── MongoDB Configuration ───────────────────────────────────────────────────
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DATABASE_NAME = os.environ.get("FLOATCHAT_DB", "floatchat_ai")

# Collection names
PROFILES_COLLECTION = "profiles"
BGC_PROFILES_COLLECTION = "bgc_profiles"
FLOATS_COLLECTION = "floats"

# ─── Data Paths ──────────────────────────────────────────────────────────────
BASE_DATA_DIR = os.environ.get(
    "ARGO_DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "incois_data")
)

# ─── Ingestion Tuning ────────────────────────────────────────────────────────
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "4"))

# ─── ARGO Reference Date ─────────────────────────────────────────────────────
# JULD is days since 1950-01-01 00:00:00 UTC
ARGO_REFERENCE_DATE = "1950-01-01T00:00:00Z"

# ─── Core ARGO Parameters ────────────────────────────────────────────────────
CORE_PARAMS = ["PRES", "TEMP", "PSAL"]

# ─── Known BGC Parameters ────────────────────────────────────────────────────
BGC_PARAMS = [
    "DOXY",
    "CHLA",
    "CHLA_FLUORESCENCE",
    "BBP700",
    "BBP532",
    "PH_IN_SITU_TOTAL",
    "NITRATE",
    "CDOM",
    "DOWN_IRRADIANCE380",
    "DOWN_IRRADIANCE412",
    "DOWN_IRRADIANCE490",
    "DOWNWELLING_PAR",
]

# ─── QC Flag Mapping ─────────────────────────────────────────────────────────
# Argo QC flags: 0=no QC, 1=good, 2=probably good, 3=probably bad,
#                4=bad, 5=changed, 6=not used, 7=not used, 8=interpolated, 9=missing
QC_FLAG_MEANINGS = {
    0: "no_qc_performed",
    1: "good",
    2: "probably_good",
    3: "probably_bad",
    4: "bad",
    5: "changed",
    6: "not_used",
    7: "not_used",
    8: "interpolated",
    9: "missing",
}
