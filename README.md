# PACTRAVEL - ELT Pipeline Orhcestration For Exercise 3
## How to use this Data?
1. Requirements
2. Preparations

### 1. Requirements
- Tools :
    - Dbeaver/SQL Client
    - Docker

### 2. Preparations
- **Clone repo** :
  ```
  # Clone
  git clone [repo_link]
  ```
- **Create .env file** in project root directory :
  ```
    # Source
    SRC_POSTGRES_DB=pactravel
    SRC_POSTGRES_HOST=localhost
    SRC_POSTGRES_USER=postgres
    SRC_POSTGRES_PASSWORD=mypassword
    SRC_POSTGRES_PORT=5433

    # DWH
    DWH_POSTGRES_DB=pactravel-dwh
    DWH_POSTGRES_HOST=localhost
    DWH_POSTGRES_USER=postgres
    DWH_POSTGRES_PASSWORD=mypassword
    DWH_POSTGRES_PORT=5434
    ```


- **Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

- **Dataset**
    - Source: Pactravel
    - DWH:
        - staging schema: pactravel
        - final schema: final
