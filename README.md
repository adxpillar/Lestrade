# SEC Form 4 insider trading RAG

End-to-end pipeline and application for ingesting SEC **Form 4** insider trading filings from EDGAR, enriching them with market context, embedding transaction-level text for semantic search, and answering natural-language questions with **hybrid structured + RAG** retrieval and citations.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture, data flow, hybrid query model, and technology choices |
| [docs/PHASES.md](docs/PHASES.md) | Phased implementation plan (ingestion → enrichment → embeddings → RAG → UI) |
| [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md) | Phase 1 data contract, tables, and EDGAR + MWAA + RDS strategy |

## Run Airflow locally (Docker) + Supabase Postgres

To run the ingestion DAGs without AWS MWAA/RDS:

- **Airflow on Docker**: see `docker/airflow/README.md`
- **Database**: create the Phase 1 tables in your Supabase Postgres (schema described in `docs/DATA_CONTRACT.md`)
- **Connection**: configure an Airflow Connection id **`lestrade_rds`** pointing at Supabase (details in `docker/airflow/README.md`)

## Disclaimer

This project is for research and engineering demonstration. SEC EDGAR data should be used in accordance with SEC policies. Nothing here is investment advice.
