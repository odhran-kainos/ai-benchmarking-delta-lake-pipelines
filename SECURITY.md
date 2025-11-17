# Security & Data Policy

## Overview
This repository fork is used for AI-assisted coding benchmarks. All data present or generated is synthetic.

## Data Handling
- No real customer, PII, or confidential data shall be added.
- Synthetic generation scripts produce controlled test datasets.

## Secrets
- No secrets committed. Use environment variables or local `.env` (ignored).
- Pre-commit hooks recommended: detect-secrets.

## Vulnerability Management
- Run `pip-audit` periodically.
- Address HIGH severity issues before merging benchmark changes.

## Reporting
Open an issue labeled `security` for any concern.