# UI Guide

## Overview
The UI is a React app served via Nginx in Docker.
It provides a triage list, evidence viewer, copilot panel, and feedback capture.

## Key Screens
- **Triage list**: shows case IDs and summary fields.
- **Evidence viewer**: tables for alerts, transactions, counterparties, merchants.
- **Copilot panel**: triggers `POST /cases/{id}/copilot-summary`.
- **Feedback form**: captures helpful/not helpful, issues, missing data.

## Configuration
API base URL is provided via `VITE_API_BASE_URL` during the build.

## Build and Run
Docker compose builds and serves the UI at `http://localhost:5173`.
