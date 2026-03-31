# SEC Form 4 insider trading RAG

End-to-end pipeline and application for ingesting SEC **Form 4** insider trading filings from EDGAR, enriching them with market context, embedding transaction-level text for semantic search, and answering natural-language questions with **hybrid structured + RAG** retrieval and citations.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture, data flow, hybrid query model, and technology choices |
| [docs/PHASES.md](docs/PHASES.md) | Phased implementation plan (ingestion → enrichment → embeddings → RAG → UI) |
| [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md) | Phase 1 data contract, tables, and EDGAR + MWAA + RDS strategy |

## Disclaimer

This project is for research and engineering demonstration. SEC EDGAR data should be used in accordance with SEC policies. Nothing here is investment advice.
