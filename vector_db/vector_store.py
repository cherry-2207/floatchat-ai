"""
ChromaDB Vector Store — Manages embedding storage and retrieval.

Provides a clean interface to:
  - Initialize ChromaDB with persistent storage
  - Add profile/float embeddings in batches
  - Query by semantic similarity
  - Query with metadata filters (hybrid search)
  - Get collection statistics
"""

import logging
from pathlib import Path

import chromadb
from chromadb.config import Settings

from vector_db.config import (
    CHROMA_PERSIST_DIR,
    PROFILES_COLLECTION,
    BGC_PROFILES_COLLECTION,
    FLOATS_COLLECTION,
    EMBEDDING_MODEL,
)

logger = logging.getLogger(__name__)


class ArgoVectorStore:
    """
    Wrapper around ChromaDB for ARGO data embeddings.

    Manages three collections:
      - argo_profiles: Core profile summaries
      - argo_bgc_profiles: BGC profile summaries
      - argo_floats: Float summaries

    Uses sentence-transformers for embedding generation.
    """

    def __init__(self, persist_dir=None, model_name=None):
        self.persist_dir = persist_dir or CHROMA_PERSIST_DIR
        self.model_name = model_name or EMBEDDING_MODEL

        # Ensure persist directory exists
        Path(self.persist_dir).mkdir(parents=True, exist_ok=True)

        # Initialize ChromaDB client with persistent storage
        logger.info(f"Initializing ChromaDB at {self.persist_dir}")
        self.client = chromadb.PersistentClient(
            path=self.persist_dir,
            settings=Settings(
                anonymized_telemetry=False,
            )
        )

        # Load the embedding function
        self._embedding_fn = self._load_embedding_function()

        # Initialize collections
        self.profiles_collection = self._get_or_create_collection(PROFILES_COLLECTION)
        self.bgc_profiles_collection = self._get_or_create_collection(BGC_PROFILES_COLLECTION)
        self.floats_collection = self._get_or_create_collection(FLOATS_COLLECTION)

    def _load_embedding_function(self):
        """Load the sentence-transformers embedding function for ChromaDB."""
        from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
        logger.info(f"Loading embedding model: {self.model_name}")
        return SentenceTransformerEmbeddingFunction(
            model_name=self.model_name
        )

    def _get_or_create_collection(self, name):
        """Get or create a ChromaDB collection with the embedding function."""
        return self.client.get_or_create_collection(
            name=name,
            embedding_function=self._embedding_fn,
            metadata={"hnsw:space": "cosine"}
        )

    # ─── Add Documents ────────────────────────────────────────────────────

    def add_profiles(self, ids, documents, metadatas):
        """
        Add profile embeddings to the profiles collection.

        Args:
            ids: List of unique IDs (profile_id strings)
            documents: List of text summaries to embed
            metadatas: List of metadata dicts for filtering
        """
        self._add_to_collection(self.profiles_collection, ids, documents, metadatas)

    def add_bgc_profiles(self, ids, documents, metadatas):
        """Add BGC profile embeddings to the bgc_profiles collection."""
        self._add_to_collection(self.bgc_profiles_collection, ids, documents, metadatas)

    def add_floats(self, ids, documents, metadatas):
        """Add float embeddings to the floats collection."""
        self._add_to_collection(self.floats_collection, ids, documents, metadatas)

    def _add_to_collection(self, collection, ids, documents, metadatas):
        """
        Add documents to a ChromaDB collection with upsert semantics.
        Handles batching internally to avoid memory issues.
        """
        if not ids:
            return

        batch_size = 500
        total = len(ids)

        for i in range(0, total, batch_size):
            end = min(i + batch_size, total)
            batch_ids = ids[i:end]
            batch_docs = documents[i:end]
            batch_meta = metadatas[i:end]

            try:
                collection.upsert(
                    ids=batch_ids,
                    documents=batch_docs,
                    metadatas=batch_meta,
                )
                logger.debug(
                    f"Upserted batch {i//batch_size + 1} "
                    f"({len(batch_ids)} docs) to {collection.name}"
                )
            except Exception as e:
                logger.error(
                    f"Error upserting batch to {collection.name}: {e}"
                )
                raise

    # ─── Query ────────────────────────────────────────────────────────────

    def query_profiles(self, query_text, n_results=10, where=None, where_document=None):
        """
        Semantic search over profile embeddings.

        Args:
            query_text: Natural language query
            n_results: Max results to return
            where: Metadata filter dict (e.g., {"region": "Arabian Sea"})
            where_document: Document content filter

        Returns:
            ChromaDB query result with ids, documents, metadatas, distances
        """
        return self._query_collection(
            self.profiles_collection, query_text, n_results, where, where_document
        )

    def query_bgc_profiles(self, query_text, n_results=10, where=None, where_document=None):
        """Semantic search over BGC profile embeddings."""
        return self._query_collection(
            self.bgc_profiles_collection, query_text, n_results, where, where_document
        )

    def query_floats(self, query_text, n_results=10, where=None, where_document=None):
        """Semantic search over float embeddings."""
        return self._query_collection(
            self.floats_collection, query_text, n_results, where, where_document
        )

    def query_all(self, query_text, n_results=5, where=None):
        """
        Search across all three collections and return merged results.
        Returns results sorted by distance (closest first).
        """
        results = []

        for collection_name, query_fn in [
            ('profiles', self.query_profiles),
            ('bgc_profiles', self.query_bgc_profiles),
            ('floats', self.query_floats),
        ]:
            try:
                result = query_fn(query_text, n_results=n_results, where=where)
                if result and result['ids'] and result['ids'][0]:
                    for i in range(len(result['ids'][0])):
                        results.append({
                            'collection': collection_name,
                            'id': result['ids'][0][i],
                            'document': result['documents'][0][i] if result['documents'] else None,
                            'metadata': result['metadatas'][0][i] if result['metadatas'] else None,
                            'distance': result['distances'][0][i] if result['distances'] else None,
                        })
            except Exception as e:
                logger.warning(f"Error querying {collection_name}: {e}")

        # Sort by distance (lower = more similar for cosine)
        results.sort(key=lambda x: x['distance'] if x['distance'] is not None else float('inf'))
        return results[:n_results * 3]  # Return top results across all collections

    def _query_collection(self, collection, query_text, n_results, where, where_document):
        """Execute a query against a ChromaDB collection."""
        # Ensure we don't request more results than exist
        count = collection.count()
        if count == 0:
            return {'ids': [[]], 'documents': [[]], 'metadatas': [[]], 'distances': [[]]}

        actual_n = min(n_results, count)

        kwargs = {
            'query_texts': [query_text],
            'n_results': actual_n,
        }
        if where:
            kwargs['where'] = where
        if where_document:
            kwargs['where_document'] = where_document

        try:
            return collection.query(**kwargs)
        except Exception as e:
            logger.error(f"Query error on {collection.name}: {e}")
            return {'ids': [[]], 'documents': [[]], 'metadatas': [[]], 'distances': [[]]}

    # ─── Statistics ───────────────────────────────────────────────────────

    def get_stats(self):
        """Get count of documents in each collection."""
        return {
            'profiles': self.profiles_collection.count(),
            'bgc_profiles': self.bgc_profiles_collection.count(),
            'floats': self.floats_collection.count(),
        }

    def delete_all(self):
        """Delete all collections. Use with caution!"""
        for name in [PROFILES_COLLECTION, BGC_PROFILES_COLLECTION, FLOATS_COLLECTION]:
            try:
                self.client.delete_collection(name)
                logger.info(f"Deleted collection: {name}")
            except Exception:
                pass

        # Reinitialize
        self.profiles_collection = self._get_or_create_collection(PROFILES_COLLECTION)
        self.bgc_profiles_collection = self._get_or_create_collection(BGC_PROFILES_COLLECTION)
        self.floats_collection = self._get_or_create_collection(FLOATS_COLLECTION)
