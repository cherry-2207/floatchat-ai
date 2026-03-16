# FloatChat-AI

**An AI-powered Conversational Interface for ARGO Ocean Data Discovery and Visualization.**

FloatChat-AI takes raw ARGO oceanography data (NetCDF files), ingests it into a structured MongoDB database, generates semantic vector embeddings using ChromaDB, and provides a RAG-powered chatbot interface (React + Node.js + Python) to ask natural language questions about ocean salinity, temperature, and BGC metrics.

---

## 🚀 Getting Started

Follow these steps to set up the project locally. Note that this requires at least **16GB of RAM** and approx. **10-15GB of disk space** for the data, MongoDB, and vector store combined.

### 1. Prerequisites

- **Python 3.10+**
- **Node.js 18+**
- **MongoDB**: Must be running locally on default port `27017`
- **Ollama** (optional, for local LLM inference):
  ```bash
  curl -fsSL https://ollama.ai/install.sh | sh
  ollama pull mistral:7b
  ```

### 2. Clone and Setup Environment

```bash
# Clone the repository
git clone https://github.com/cherry-2207/floatchat-ai floatchat-ai
cd floatchat-ai

# Create and activate a Python virtual environment
python -m venv .venv
source .venv/bin/activate
# On Windows: .venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 3. Provide the ARGO Data

Place the raw `.nc` (NetCDF) ARGO profile files in the `incois_data/` directory at the root of the project. This folder should contain subfolders for each float (e.g., `incois_data/<platform_number>/profiles/*.nc`).

---

## 🛠️ Pipeline Execution

The system is built in phases. Currently, the Data Ingestion (Phase 1) and Vector Embeddings (Phase 2a) are available.

### Phase 1: Data Ingestion (NetCDF ➔ MongoDB)

This step reads all `.nc` files from `incois_data/`, parses them, and upserts them into MongoDB (`floatchat_ai` database). It handles core parameters (TEMP, PSAL), BGC parameters (DOXY, CHLA, etc.), quality control flags, and masks out missing `NaN` values.

```bash
# Run the ingestion pipeline (uses all CPU cores by default)
python -m data.run_ingestion

# Run validation to check doc counts and schema integrity
python tests/validate_ingestion.py
```

_Note: For ~100,000 files, this takes ~40 minutes depending on your CPU/disk speed._

### Phase 2a: Vector Embeddings (MongoDB ➔ ChromaDB)

This step reads the MongoDB data, generates natural language summaries for every profile and float, and embeds them into a local ChromaDB vector database using the `all-MiniLM-L6-v2` model. This enables the RAG semantic search.

```bash
# Build embeddings for all collections (profiles, bgc_profiles, floats)
python -m vector_db.build_embeddings

# Verify embeddings and test semantic search
python tests/validate_embeddings.py
```

_Note: Generating embeddings for 100k documents takes ~35 minutes on a modern CPU._

---

## 📂 Project Structure

```
floatchat-ai/
├── data/                    # Phase 1: Ingestion pipeline
│   ├── config.py            # MongoDB mapping & variables
│   ├── nc_parser.py         # ARGO NetCDF parser logic
│   ├── ingestion.py         # Multiprocessing batch insertion
│   └── run_ingestion.py     # CLI entry point
├── vector_db/               # Phase 2a: Embeddings
│   ├── config.py            # ChromaDB + Model config
│   ├── summary_generator.py # Text/Metadata generation
│   ├── vector_store.py      # ChromaDB wrapper
│   └── build_embeddings.py  # Builder CLI
├── tests/                   # Validation scripts
│   ├── validate_ingestion.py
│   └── validate_embeddings.py
├── incois_data/             # Place raw ARGO NetCDF files here
├── chroma_data/             # Auto-generated vector DB storage
└── requirements.txt         # Python dependencies
```

## 🗺️ Next Steps Roadmap (In Progress)

- **Phase 2b/2c**: MCP Server & LangChain RAG pipeline
- **Phase 3**: Node.js + Express REST API Backend
- **Phase 4**: React.js + Tailwind UI Frontend with mapping (Leaflet) and charts (Plotly)
- **Phase 5**: Final demo integration
