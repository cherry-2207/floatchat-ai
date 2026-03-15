# ARGO NetCDF Data Ingestion Pipeline

Ingest ~100K ARGO NetCDF profile files (567 floats, Indian Ocean) into local MongoDB with proper handling of core and BGC data, QC flags, and NaN/masked values.

## Data Analysis Findings

| Metric | Value |
|---|---|
| Total float directories | 567 |
| Total NC files | ~100,467 |
| Delayed mode (D prefix) | 75,032 |
| Real-time mode (R prefix) | 12,331 |
| Synthetic BGC (SD prefix) | 13,104 across 80 floats |
| Core parameters | PRES, TEMP, PSAL |
| BGC parameters | DOXY, CHLA, CHLA_FLUORESCENCE, BBP700 |
| Data encoding | Character arrays as `\|S1` byte arrays |

> [!IMPORTANT]
> No standalone B-prefix BGC files were found. BGC data exists only in **Synthetic (SD) files** which merge core + BGC measurements. The schema must handle both file types.

## User Review Required

> [!WARNING]
> **MongoDB must be running locally** on the default port (27017). Please confirm MongoDB is installed and running before we proceed to execution.

> [!IMPORTANT]
> **Batch size & parallelism**: With 16GB RAM and ~100K files, the pipeline will process files in batches of 500 with 4 worker processes. This should keep memory usage under 4GB. Please let me know if you'd prefer different tuning.

## MongoDB Schema Design

### Collection 1: `profiles` (one document per profile from core D/R files)

```json
{
  "_id": "2900226_001",              // {platform_number}_{cycle_number}[D]
  
  // --- Metadata ---
  "data_type": "Argo profile",
  "format_version": "3.1",
  "handbook_version": "1.19",
  "date_creation": "2003-10-01T00:00:00Z",
  "date_update": "2018-05-15T00:00:00Z",
  
  // --- Platform Info ---
  "platform_number": "2900226",
  "project_name": "Argo INDIA",
  "pi_name": "M RAVICHANDRAN",
  "platform_type": "APEX",
  "float_serial_no": "7207",
  "firmware_version": "013108",
  "wmo_inst_type": "846",
  "data_centre": "IF",
  "dc_reference": "29002260001",
  
  // --- Cycle Info ---
  "cycle_number": 1,
  "direction": "A",                  // A=ascending, D=descending
  "data_mode": "D",                  // R=realtime, A=adjusted, D=delayed
  "data_state_indicator": "2C",
  "config_mission_number": 1,
  "vertical_sampling_scheme": "Primary sampling: averaged",
  
  // --- Position & Time ---
  "timestamp": "2003-01-10T19:30:03Z",  // Converted from JULD
  "timestamp_qc": 1,
  "timestamp_location": "2003-01-10T19:50:18Z",
  "latitude": 5.912,
  "longitude": 85.028,
  "position_qc": 1,
  "positioning_system": "ARGOS",
  "geo_location": {                  // GeoJSON for spatial queries
    "type": "Point",
    "coordinates": [85.028, 5.912]   // [lon, lat]
  },
  
  // --- Profile QC Summary ---
  "profile_pres_qc": "B",
  "profile_temp_qc": "B",
  "profile_psal_qc": "B",
  
  // --- Measurements (array of level objects) ---
  "measurements": [
    {
      "pres": 4.0,         "pres_qc": 1,
      "pres_adjusted": 4.0, "pres_adjusted_qc": 2, "pres_adjusted_error": 2.4,
      "temp": 28.585,       "temp_qc": 1,
      "temp_adjusted": 28.585, "temp_adjusted_qc": 2, "temp_adjusted_error": 0.002,
      "psal": 34.406,       "psal_qc": 1,
      "psal_adjusted": 34.406, "psal_adjusted_qc": 2, "psal_adjusted_error": 0.01
    }
    // ... one entry per depth level (N_LEVELS)
  ],
  "n_levels": 51,
  "max_pres": 1023.5,
  
  // --- Station Parameters ---
  "station_parameters": ["PRES", "TEMP", "PSAL"],
  
  // --- Calibration Info ---
  "calibration": [
    {
      "parameter": "PRES",
      "equation": "PRES_ADJUSTED = PRES - surface_pressure",
      "coefficient": "surface_pressure=0.2 dbar",
      "comment": "Pressure adjusted at surface",
      "date": "2018-05-15T00:00:00Z"
    }
  ],
  
  // --- History ---
  "history": [
    {
      "institution": "IF",
      "step": "ARGQ",
      "software": "IFRP",
      "software_release": "V1.0",
      "reference": "",
      "date": "2003-10-01T00:00:00Z",
      "action": "IP",
      "parameter": "TEMP",
      "start_pres": null,
      "stop_pres": null,
      "previous_value": null,
      "qctest": ""
    }
  ],
  
  // --- Ingestion Metadata ---
  "source_file": "D2900226_001.nc",
  "ingested_at": "2026-03-13T23:00:00Z",
  "file_type": "core"               // "core" or "synthetic_bgc"
}
```

### Collection 2: `bgc_profiles` (one document per synthetic BGC profile)

Same structure as `profiles` but with additional BGC measurements:

```json
{
  "_id": "2902124_002_BGC",
  // ... same metadata fields as profiles ...
  
  "station_parameters": ["PRES", "TEMP", "PSAL", "DOXY", "CHLA", "BBP700", "CHLA_FLUORESCENCE"],
  "contains_bgc": true,
  "parameter_data_mode": ["D", "D", "D", "D", "A", "A", "A"],
  
  "measurements": [
    {
      "pres": 5.0, "pres_qc": 1,
      "temp": 28.5, "temp_qc": 1,
      "psal": 34.4, "psal_qc": 1,
      "doxy": 195.3, "doxy_qc": 1,
      "doxy_adjusted": 200.1, "doxy_adjusted_qc": 1, "doxy_adjusted_error": 5.0,
      "chla": 0.15, "chla_qc": 1,
      "chla_adjusted": 0.12, "chla_adjusted_qc": 2,
      "bbp700": 0.001, "bbp700_qc": 1,
      "chla_fluorescence": 0.08, "chla_fluorescence_qc": 1
      // dPRES fields also included where present
    }
  ],
  
  "file_type": "synthetic_bgc"
}
```

### Collection 3: `floats` (one document per physical float — aggregated)

```json
{
  "_id": "2900226",
  "platform_number": "2900226",
  "project_name": "Argo INDIA",
  "pi_name": "M RAVICHANDRAN",
  "platform_type": "APEX",
  "wmo_inst_type": "846",
  "data_centre": "IF",
  "total_cycles": 131,
  "has_bgc": false,
  "bgc_parameters": [],
  "first_date": "2003-01-10T19:30:03Z",
  "last_date": "2016-05-20T12:00:00Z",
  "geo_bounding_box": {
    "min_lat": -5.2, "max_lat": 12.1,
    "min_lon": 75.0, "max_lon": 92.3
  },
  "data_modes_used": ["D", "R"]
}
```

### MongoDB Indexes

```javascript
// profiles & bgc_profiles collections
db.profiles.createIndex({ "geo_location": "2dsphere" })
db.profiles.createIndex({ "platform_number": 1 })
db.profiles.createIndex({ "timestamp": 1 })
db.profiles.createIndex({ "platform_number": 1, "cycle_number": 1 })
db.profiles.createIndex({ "latitude": 1, "longitude": 1 })
db.profiles.createIndex({ "data_mode": 1 })

// floats collection
db.floats.createIndex({ "platform_number": 1 }, { unique: true })
db.floats.createIndex({ "has_bgc": 1 })
```

## NaN/Null/QC Handling Strategy

| Scenario | Action |
|---|---|
| Masked/fill values (`_FillValue`, `99999`) | Store as `null` in MongoDB |
| `NaN` float values | Store as `null` |
| QC flags (byte chars `b'1'`, `b'2'`, etc.) | Decode to integer (1, 2, 3...) |
| QC flag `b' '` or empty | Store as `-1` (undefined) |
| Empty/whitespace-only strings | Store as `null` |
| Character arrays (`\|S1` byte arrays) | Join and decode to UTF-8 string, strip whitespace |
| Measurements where ALL core params are masked | Skip that level entirely |
| Profile-level QC of 3 or 4 | **Still ingest** but flag with `"qc_warning": true` |

## Proposed Changes

### Project Structure

#### [NEW] [requirements.txt](file:///home/cherry/Desktop/floatchat-anti-gravity/requirements.txt)
Python dependencies: `netCDF4`, `pymongo`, `numpy`, `tqdm`

---

### Data Ingestion Module

#### [NEW] [config.py](file:///home/cherry/Desktop/floatchat-anti-gravity/config.py)  
Configuration: MongoDB URI, database name, data paths, batch sizes, worker count.

#### [NEW] [nc_parser.py](file:///home/cherry/Desktop/floatchat-anti-gravity/nc_parser.py)
Core module with two main classes:
- `ArgoNCParser`: Reads a single NetCDF file and returns a structured dict
  - Handles character array decoding (`|S1` → string)
  - JULD → ISO datetime conversion (reference date: 1950-01-01)
  - Mask/NaN → `null` conversion
  - QC flag decoding (byte → int)
  - Auto-detects core vs BGC based on station parameters
  - Builds measurement array with all available parameters per level
  - Extracts calibration and history arrays
- `BGCParameterDetector`: Identifies which BGC parameters are present in a file

#### [NEW] [ingestion.py](file:///home/cherry/Desktop/floatchat-anti-gravity/ingestion.py)
Orchestrator module:
- `ArgoIngestionPipeline`: Main entry point
  - Discovers all NC files across float directories
  - Groups files by type (core D/R vs synthetic SD)
  - Processes files in batches of 500 using `multiprocessing.Pool(4)`
  - Uses `pymongo.bulk_write` with `UpdateOne(upsert=True)` for idempotent ingestion
  - Builds `floats` collection by aggregating after all profiles are inserted
  - Progress tracking via `tqdm`
  - Error logging with per-file error capture (doesn't halt on single file failure)

#### [NEW] [run_ingestion.py](file:///home/cherry/Desktop/floatchat-anti-gravity/run_ingestion.py)
CLI entry point: `python run_ingestion.py [--batch-size 500] [--workers 4] [--limit N]`

## Verification Plan

### Automated Tests

1. **Unit test on parser** — parse a single known core file, validate all fields:
   ```bash
   source .venv/bin/activate && python -m pytest tests/test_parser.py -v
   ```

2. **Unit test on BGC parser** — parse a synthetic file, verify BGC params exist:
   ```bash
   source .venv/bin/activate && python -m pytest tests/test_bgc_parser.py -v
   ```

3. **Ingestion smoke test** — ingest 1 float directory (small), query MongoDB for results:
   ```bash
   source .venv/bin/activate && python run_ingestion.py --limit 1
   ```

4. **MongoDB validation queries** — after full ingestion, run validation script:
   ```bash
   source .venv/bin/activate && python tests/validate_ingestion.py
   ```
   This will check: document count vs file count, presence of geo_location indexes, sample QC values, no NaN values in documents, BGC profiles have extra parameters.

### Manual Verification

1. After ingestion, open `mongosh` and run:
   ```javascript
   use floatchat_ai
   db.profiles.countDocuments()         // should be ~87K
   db.bgc_profiles.countDocuments()     // should be ~13K  
   db.floats.countDocuments()           // should be 567
   db.profiles.findOne({ platform_number: "2900226" })  // inspect a sample
   db.bgc_profiles.findOne({ contains_bgc: true })      // inspect BGC
   ```

## Future Steps (Post-Ingestion)

After ingestion is complete, the next phases will be:

1. **Vector DB + RAG Pipeline** — Use ChromaDB/Pinecone to store embeddings of profile metadata and float summaries. Build MCP-powered RAG with an LLM (GPT/Qwen/LLaMA/Mistral) to translate natural language queries → MongoDB queries.

2. **Interactive Dashboards** — Plotly/Dash or Streamlit dashboards for:
   - Float trajectory maps (animated over time)
   - Depth-time cross-section plots
   - Profile comparison tools
   - T-S diagrams

3. **Chatbot Interface** — Web-based chat UI where users ask questions like "Show me salinity profiles near the equator in March 2023" and get instant visualizations.

4. **Indian Ocean PoC** — Full demonstration with the INCOIS dataset showing end-to-end flow.
