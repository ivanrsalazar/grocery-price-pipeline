# Grocery Price Warehouse

This directory contains version-controlled DDLs for the grocery price data warehouse.

## Layers
- bronze: raw, append-only snapshots from external APIs
- silver: cleaned and normalized entities
- gold: analytics-ready aggregates and indices

DDLs here are executed by infrastructure (or manually) and referenced by Dagster assets.