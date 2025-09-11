# 🚀 CI/CD for Prefect + Snowflake

## 📊 Project Overview
This repository provides a production-ready template to orchestrate data workflows with [Prefect](https://www.prefect.io/) and integrate them with [Snowflake](https://www.snowflake.com/), backed by a Git-based CI/CD pipeline. It streamlines how you:
- Develop and test Prefect flows locally
- Connect securely to Snowflake
- Automate quality checks and deployments via CI/CD (e.g., GitHub Actions)
- Monitor runs and receive alerts via Prefect

---

## 🎯 Objectives
- ✅ Standardize development and deployment of Prefect flows
- ✅ Enforce high code quality with automated linting and tests
- ✅ Enable secure, repeatable connections to Snowflake
- ✅ Ship changes safely using branch protections and CI gates
- ✅ Provide a clear, modular project structure

---

## ✨ Features
- Automated CI: format, lint, test, and type-check your code on every PR
- Continuous Delivery: auto-deploy Prefect deployments on merge to main
- Secrets management via GitHub Actions secrets and/or Prefect Blocks
- Snowflake integration using the official Python connector
- Example flow patterns for ELT, SQL tasks, and idempotent runs
- Ready-to-adapt project structure for teams

---

## 🧭 Architecture (High Level)
1. Developer pushes a branch/PR
2. CI runs checks (formatting, linting, tests)
3. On merge to main, CI builds and deploys Prefect deployments
4. Prefect executes flows against Snowflake using secure credentials
5. Observability and retries managed by Prefect

Add a pipeline diagram here:
<!-- Replace with your image -->
<!-- ![Architecture Diagram](docs/images/architecture.png) -->

---

## 🗂️ Project Structure
Note: Adjust paths to match your repository if they differ.

```
cicd-prefect-snowflake/
├── src/
│   ├── flows/                 # Prefect flows (Python)
│   ├── tasks/                 # Reusable Prefect tasks/utilities
│   ├── snowflake_utils.py     # Snowflake helpers (connections, SQL execution)
│   └── __init__.py
├── prefect/
│   └── deployments/           # Deployment YAMLs (built by Prefect CLI)
├── tests/                     # Unit/integration tests (pytest)
├── .github/
│   └── workflows/             # CI/CD pipelines (GitHub Actions)
├── requirements.txt           # Python dependencies
├── README.md                  # This file
└── .env.example               # Example environment variables (no secrets)
```

---

## 🧰 Tools & Technologies Used
- Python (100%)
- Prefect 2.x (or 3.x) for orchestration
- Snowflake Python Connector
- GitHub Actions (or your CI of choice)
- pytest, ruff/flake8, black, mypy (recommended)

---

## 🚀 Getting Started

### 1) Clone and create a virtual environment
```bash
git clone https://github.com/Svbongo/cicd-prefect-snowflake.git
cd cicd-prefect-snowflake

python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows
# .venv\Scripts\activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Configure environment variables
Create a `.env` (or use your shell) with the following variables. Do NOT commit secrets.

```bash
# Prefect (Cloud) – if applicable
PREFECT_API_URL="https://api.prefect.cloud/api/.../..."   # or local server URL
PREFECT_API_KEY="*****"

# Snowflake
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_USER="your_user"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_DATABASE="YOUR_DB"
SNOWFLAKE_SCHEMA="PUBLIC"
SNOWFLAKE_ROLE="SYSADMIN"
```

For CI, store these as GitHub repository secrets with the same names.

### 4) Authenticate Prefect
- Prefect Cloud:
  ```bash
  prefect cloud login --key $PREFECT_API_KEY --workspace <org>/<workspace>
  ```
- Prefect Server (self-hosted/local): ensure `PREFECT_API_URL` points to your server.

---

## ▶️ Usage

### Run a flow locally
```bash
python -m src.flows.example_flow
# or with Prefect CLI
prefect run -p src/flows/example_flow.py
```

### Build and apply a deployment
```bash
# Build a deployment from a flow
prefect deploy src/flows/example_flow.py:example_flow --name "example-deployment" --apply
# Then trigger:
prefect deployment run "example-flow/example-deployment"
```

### Connect to Snowflake in code (example)
```python
from prefect import flow, task
import snowflake.connector
import os

@task
def run_sql(sql: str):
    ctx = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    try:
        with ctx.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        ctx.close()

@flow
def example_flow():
    result = run_sql.submit("SELECT CURRENT_TIMESTAMP();")
    return result.result()

if __name__ == "__main__":
    example_flow()
```

---

## 🔁 CI/CD

### Typical GitHub Actions workflow (high level)
- On PR:
  - Setup Python, install deps
  - Format check (black), lint (ruff/flake8), type-check (mypy)
  - Run tests (pytest)
- On push to main:
  - Repeat checks
  - Build Prefect deployments
  - Apply deployments to Prefect

Required GitHub Secrets (suggested):
- `PREFECT_API_URL`
- `PREFECT_API_KEY`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_ROLE`

Add your workflow file under `.github/workflows/ci.yml`. Example skeleton:

```yaml
name: CI-CD

on:
  pull_request:
  push:
    branches: [ "main" ]

jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - name: Install
        run: pip install -r requirements.txt
      - name: Lint & Format
        run: |
          black --check .
          ruff check .
          mypy src
      - name: Tests
        run: pytest -q

  deploy:
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    needs: ci
    runs-on: ubuntu-latest
    env:
      PREFECT_API_URL: ${{ secrets.PREFECT_API_URL }}
      PREFECT_API_KEY: ${{ secrets.PREFECT_API_KEY }}
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
      SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
      SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
      SNOWFLAKE_SCHEMA: ${{ secrets.SNOWFLAKE_SCHEMA }}
      SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.10"
      - name: Install
        run: pip install -r requirements.txt
      - name: Prefect Deploy
        run: |
          prefect version
          # build & apply all deployments (adapt to your flows)
          prefect deploy src/flows/example_flow.py:example_flow --name "example-deployment" --apply
```

---

## 📈 Key Practices & Recommendations
- Keep flows idempotent and parameterized
- Use Prefect blocks for Snowflake credentials when possible
- Add retries and timeouts to critical tasks
- Log SQL statements and row counts for observability
- Add smoke tests for each flow path hitting Snowflake
- Use branch protections and required checks for main

---

## 🏆 Achievements
- Automated deployment of Prefect flows on every merge to main
- Consistent quality gates via linting, typing, and tests
- Secure integration with Snowflake using environment-based secrets

---

## 👥 Author / Team
- Soham Vasudeo

---

## 🤝 Contributing
Contributions are welcome! Please:
1. Open an issue describing your change
2. Create a feature branch
3. Submit a PR with clear description and tests
