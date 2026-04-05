# PSI-Engine: Supply Chain Control Tower
End to end analytics engineering pipeline transforming raw transactional data into PSI command center.

## Summary
PSI Engine (Production, Sales, Inventory) is an end-to-end data platform designed to mitigate supply chain risks through automated inventory forecasting and state-aware orchestration. Developed with a Security by Design philosophy, the engine transforms raw transactional data into a GDPR compliant decision support system.

---

## System Architecture
Diagram illustrates the "Lead-Level" architecture, highlighting the Engineering Gates and the Containerized Infrastructure.

<img width="1446" height="679" alt="Image" src="https://github.com/user-attachments/assets/036f9d7a-5009-4912-9d61-4afa77fc5ce5" />

---

## Key Capabilities
| **Category**       | **Tools**               | **Engineering** **Capabilities**                                       |
|----------------|---------------------|----------------------------------------------------------------|
| **Transformation** | dbt Core, DuckDB    | Medallion Architecture, Atomic Grain Definition                |
| **Orchestration**  | Apache Airflow 2.10 | Idempotent Pipelines, DAG                                      |
| **Infrastructure** | Docker              | Containerization                                               |
| **Governance**     | GDPR, SHA256        | PII Masking, Vertical Data Filtering, RLS (Row Level Security) |
| **Analytics**      | Power BI            | Dimensional Modelling, Information Design, Dashboard           |

---

## Data Lineage

> *The pipeline traces data from raw CSVs -> Staging (Cleaning) -> Intermediate (PSI Logic) -> Marts (Star Schema).*

<img width="1683" height="823" alt="Image" src="https://github.com/user-attachments/assets/5409a533-4192-4e2d-bb15-6ff11282e693" />

---

## Key Engineering Principles
### 1. Slim CI
Implemented Slim CI logic using dbt state persistence for compute resource optimization. Comparing current project manifest against stored ```manifest.json```, pipeline identifies and executes only the modified nodes and their downstream dependencies.

### 2. Privacy by Design
Engine features a built-in Privacy Wall:
- **SHA256 Hashing:** Sensitive manager identifiers are hashed at the source to maintain RLS integrity without exposing PII
- **Vertical Selection:** Export layer utilizes a "Positive Select" filter, ensuring only authorized columns reach the analytics payload.

### 3. Data Quality Circuit Breakers
Every transformation step is guarded by tests. Custom tests such as ```assert_positive_inventory``` act as business circuit breakers, identifying projected stockouts before data is committed to the Gold Layer.

---
### PowerBI Dashboard
<img width="895" height="494" alt="Image" src="https://github.com/user-attachments/assets/7d7fc53b-6674-41e6-afe1-86944e70338b" />

---
## Future Roadmap & Enterprise Scaling
### 1. Advanced Analytics
Integrate forecasting model to move from descriptive to predictive demand planning

### 2. Reverse ETL
Implement automated Slack/Email alerts for regional managers when "Emergency" inventory thresholds are crossed

### 3. Cloud Migration
Transition from Docker stack to a managed cloud enviornment for large scale resilience.

---

## Installation
```
# Clone the repository
git clone https://github.com/your-username/PSI-Supply-Chain-Tower.git

# Initialize the Docker environment
docker-compose up -d

# Access Airflow at localhost:8080 to trigger the pipeline
```