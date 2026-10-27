# End-to-End AML Medallion Detection Pipeline

This repository contains an automated, production-grade Anti-Money Laundering (AML) data engineering and machine learning pipeline operating on the **IBM AMLSim HI-Small** dataset.

The pipeline implements an **AWS S3 Medallion storage architecture**, **PySpark distributed ETL with temporal & velocity windowing**, **class-imbalanced XGBoost training with Isotonic Probability Calibration & Top-K alert capacity evaluation**, and **Apache Airflow orchestration**.

---

## 1. AWS S3 Medallion Storage Organization

All cloud operations respect these exact S3 bucket prefixes:

```
s3://<S3_BUCKET_NAME>/
├── bronze/
│   ├── HI-Small_Trans.csv        # Raw transaction log (5+ million transfers)
│   └── HI-Small_accounts.csv     # Raw accounts reference data
├── processed/
│   ├── train/ (or train.parquet) # Chronological 70% feature-engineered Parquet split
│   ├── val/   (or val.parquet)   # Chronological 15% validation Parquet split
│   └── test/  (or test.parquet)  # Chronological 15% out-of-time test Parquet split
├── evaluation/
│   ├── metrics.json              # PR-AUC, ROC-AUC, Precision, Recall, F1, Top-K Recall
│   ├── confusion_matrix.png      # High-resolution confusion matrix plot
│   └── pr_curve.png              # Precision-Recall curve under extreme class imbalance
└── model/
    ├── xgboost_aml_model.json    # Trained XGBoost tree model
    ├── xgboost_features.json     # [Artifact 1] Ordered feature sequence contract
    ├── category_maps.json        # [Artifact 2] Fixed category string-to-int dictionaries
    ├── calibrator.pkl            # [Artifact 3] Isotonic probability calibrator model
    ├── config.json               # [Artifact 4] Operational decision config (tuned tau*, top-k capacity)
    └── xgboost_model.joblib      # Serialized scikit-learn compatible joblib artifact
```

---

## 2. Where to Fill Up Your Credentials

To configure your pipeline credentials, create a `.env` file in the repository root by copying the template:

```bash
cp .env.example .env
```

Open `.env` and fill in your values in the designated sections:

### A. Kaggle API Credentials
Required for downloading the official `ealtman2019/ibm-transactions-for-anti-money-laundering-aml` dataset:
1. Log in to [Kaggle](https://www.kaggle.com).
2. Go to your **Profile** → **Settings** → **API**.
3. Click **Create New Token** (downloads a `kaggle.json` file).
4. Fill in:
   ```env
   KAGGLE_USERNAME="your-kaggle-username"
   KAGGLE_KEY="your-kaggle-api-key"
   ```
*(Note: If Kaggle credentials are missing or the API call fails, the ingestion step strictly halts and returns an error with instructions. No synthetic fallback is used).*

### B. AWS Credentials & Region
Required for uploading to S3 or executing Spark on AWS EMR:
- **Personal AWS Account**: AWS Console → IAM → Users → Security credentials → Create Access Key.
- **AWS Academy / Learner Lab**: Open Learner Lab → Click **AWS Details** → Click **Show** next to AWS CLI → Copy `aws_access_key_id`, `aws_secret_access_key`, and `aws_session_token`.
Fill in:
```env
AWS_ACCESS_KEY_ID="AKIAXXXXXXXXXXXXXXXX"
AWS_SECRET_ACCESS_KEY="your-secret-access-key"
AWS_DEFAULT_REGION="us-east-1"
# AWS_SESSION_TOKEN="FwoGZ..."   # Uncomment if using AWS Academy / SSO
```

### C. AWS S3 Bucket Name
Fill in the exact name of your S3 bucket:
```env
S3_BUCKET_NAME="my-aml-medallion-bucket"
```

### D. AWS EMR Cluster ID
Configure your active EMR cluster ID for distributed Spark ETL and XGBoost training:
```env
EMR_CLUSTER_ID="j-XXXXXXXXXXXX"
```
The pipeline automatically uses AWS S3 and EMR when `S3_BUCKET_NAME` and `EMR_CLUSTER_ID` are set.


---

## 3. The Hyperparameters in Model Training

In AML transaction monitoring, standard defaults (like predicting with 50/50 weights) completely fail because fewer than 0.5% of transactions are money laundering.

Here is what each hyperparameter in [`models/train_xgboost.py`](models/train_xgboost.py) does and why it is set that way:

| Hyperparameter | Value | AML Business & Technical Rationale |
| :--- | :--- | :--- |
| `scale_pos_weight` | $N_{\text{neg}} / N_{\text{pos}}$ (≈ 150–200) | **Addresses Extreme Class Imbalance**: Without this, the model would simply predict "0" (legitimate) 99.5% of the time, achieving 99.5% accuracy while letting 100% of money launderers slip through. This tells XGBoost: *"A missed money launderer (False Negative) is 200× more costly than a false alarm (False Positive)."* |
| `eval_metric` | `"aucpr"` | **Optimizes Precision-Recall AUC**: ROC-AUC is misleading on imbalanced data because massive legitimate transactions inflate the curve. PR-AUC focuses strictly on how well the model isolates actual laundering cases. |
| `max_depth` | `6` | **Captures Laundering Synergies**: Money laundering is rarely a single metric; it is an interaction (e.g., Cross-Border + Off-Peak Hour + Amount between $9k–$10k + Spike in Burst Ratio). Depth 6 allows up to $2^6 = 64$ decision leaves to capture these multi-hop combinations without overfitting. |
| `learning_rate` | `0.05` | **Prevents Erratic Step Sizes**: A smaller shrinkage rate ($\eta = 0.05$) combined with more trees prevents the algorithm from over-correcting on isolated anomalies. |
| `n_estimators` | `300` | Total number of boosting rounds (trees) to build. |
| `early_stopping_rounds` | `30` | **Prevents Temporal Overfitting**: Evaluates on the chronological validation split. If validation PR-AUC does not improve for 30 consecutive iterations, training halts automatically. |
| `subsample` / `colsample_bytree` | `0.8` / `0.8` | **Stochastic Regularization**: Each tree is trained on a random 80% sample of rows and 80% sample of feature columns. This prevents single dominant features (like transaction amount) from hijacking every tree. |
| `tree_method` | `"hist"` | **High-Speed Execution**: Uses histogram-based binning (similar to LightGBM), accelerating training by $5\times$ to $10\times$ on large transaction volumes. |

---

## 4. The 4 Companion Artifacts: Why They Exist & How They Work Together

When model training finishes, the pipeline exports **4 companion metadata files** to `s3://<BUCKET_NAME>/model/` alongside the model weights (`xgboost_aml_model.json`) so that streaming inference is deterministic, calibrated, and operational:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              S3 BUCKET: s3://.../model/                                │
│                                                                                        │
│  1. xgboost_features.json  ──► Guarantees exact column sequence & shape                │
│  2. category_maps.json     ──► Guarantees string-to-int mapping stays consistent       │
│  3. calibrator.pkl         ──► Converts raw boosted scores into true probabilities     │
│  4. config.json            ──► Stores tuned alert threshold (e.g., tau* = 0.324)       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### ① `xgboost_features.json` (The Column Sequence Contract)
- **What it is**: An ordered JSON array of feature names:
  ```json
  ["amount_paid_usd", "is_structuring_threshold", "tx_hour", "sender_burst_ratio_1h_24h", "payment_format_code", ...]
  ```
- **Why it is needed**: XGBoost does not read column names during inference—it reads matrix column indices ($0, 1, 2, \dots, N$). If incoming streaming data has columns in a different order, XGBoost will silently evaluate the wrong thresholds.
- **How it is used**: Streaming code executes `X_stream = df[feature_order]`, forcing the matrix into the exact training sequence.

### ② `category_maps.json` (The Category Dictionary)
- **What it is**: A lookup table mapping categorical strings to fixed integers:
  ```json
  {
    "payment_format": {"ACH": 0, "Cash": 1, "Cheque": 2, "Wire": 3},
    "payment_currency": {"EUR": 0, "GBP": 1, "USD": 2}
  }
  ```
- **Why it is needed**: If you train on historical data where `"Wire"` = 3, but tomorrow's stream receives a batch containing only `["Cash", "Wire"]`, an automatic label encoder might reassign `"Wire"` = 1. This dictionary freezes the mapping so `"Wire"` is always 3.

### ③ `calibrator.pkl` (Probability Calibrator)
- **What it is**: An Isotonic Regression model fitted on the held-out validation set.
- **The Problem**: Because we used `scale_pos_weight ≈ 150–200`, the raw output probabilities from XGBoost are mathematically distorted (boosted upward to prioritize recall). A raw prediction of 0.85 does not mean an 85% probability of laundering.
- **The Solution**: 
  $$\text{Calibrator}(P_{\text{raw}}) \longrightarrow P(\text{Is Laundering} = 1 \mid \mathbf{x})$$
  It maps raw scores back to true, mathematically accurate posterior probabilities so investigators know the realistic odds.

### ④ `config.json` (The Operational Decision Config)
- **What it is**: Stores pipeline operational parameters determined during validation:
  ```json
  {
    "model_version": "v1.0",
    "calibrated_threshold": 0.3240,
    "top_k_daily_capacity": 200,
    "validation_prauc": 0.8124,
    "test_prauc": 0.8050,
    "scale_pos_weight": 185.20
  }
  ```
- **Why it is needed**:
  Standard ML uses a default threshold of 0.50. On imbalanced fraud data, 0.50 is suboptimal. During validation, the pipeline sweeps thresholds to find $\tau^*$ (e.g. 0.3240) that maximizes the F1-score or guarantees high recall for the compliance team's daily review capacity (e.g., top 200 alerts). The inference engine reads `config.json` to flag transactions: `is_alert = (calibrated_prob >= config["calibrated_threshold"])`.

### Summary of the Streaming Prediction Flow
When a streaming transaction arrives:
1. `category_maps.json` encodes strings like `"Wire"` into `3`.
2. `xgboost_features.json` orders all 25+ features into the exact sequence `df[features]`.
3. `xgboost_aml_model.json` generates the raw tree score $P_{\text{raw}}$.
4. `calibrator.pkl` corrects $P_{\text{raw}}$ into true posterior probability $P_{\text{calibrated}}$.
5. `config.json` checks if $P_{\text{calibrated}} \ge \tau^*$ to generate an investigator alert.

---

## 5. Running the Pipeline

### Option A (Recommended): Automated Orchestration via Apache Airflow

The primary way to execute the end-to-end pipeline is using the Apache Airflow Docker container stack:

1. **Launch Airflow Stack**:
   ```bash
   docker compose up -d
   ```

2. **Access Airflow UI**:
   - URL: `http://localhost:8080`
   - Default credentials: `admin` / `admin`

3. **Trigger DAG**:
   - Locate DAG: `aml_end_to_end_pipeline`
   - Trigger execution:
     $$\text{task\_ingest\_data} \longrightarrow \text{task\_submit\_spark\_etl} \longrightarrow \text{task\_train\_xgboost}$$
   - The DAG automatically ingests Kaggle data to S3 Bronze, triggers distributed PySpark feature engineering on your active AWS EMR cluster, and runs XGBoost training, calibration, and $F_2$ evaluation on EMR.

---

### Option B: Step-by-Step Manual CLI Execution on AWS

If you prefer running individual steps from the terminal:

1. **Configure `.env`**:
   Ensure `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET_NAME`, and `EMR_CLUSTER_ID` are set.

2. **Step 1: Ingest Raw Kaggle Dataset to S3 Bronze**:
   ```bash
   python3 bq1/ingestion/ingest_aml_data.py --s3-bucket <YOUR_BUCKET>
   ```

3. **Step 2: Submit Distributed PySpark ETL to AWS EMR**:
   ```bash
   python3 bq1/spark/submit_emr.py --cluster-id <YOUR_EMR_CLUSTER_ID> --s3-bucket <YOUR_BUCKET>
   ```
   *(Monitors the Spark step until `COMPLETED` and outputs processed splits to `s3://<YOUR_BUCKET>/processed/`)*.

4. **Step 3: Train XGBoost Model & Evaluate on AWS**:
   Triggered via Airflow `task_train_xgboost` or submitted to EMR with `scripts/train_xgboost.py`. Outputs evaluation metrics, confusion matrix, PR curve, and model artifacts directly to `s3://<YOUR_BUCKET>/evaluation/` and `s3://<YOUR_BUCKET>/model/`.

---

## 6. Pipeline B: Real-Time Scoring

A Lambda replays transactions into MSK; Managed Flink rebuilds the rolling
account features, scores each record with the artifacts in `model/`, writes
alerts back to MSK and scored rows to an Iceberg table in the Glue catalog.
Requires Pipeline A to have completed.

The commands are documented once, in the root [README](../README.md#running-it),
so the two do not drift. In short: `pipeline.sh setup` once, then per session
`pipeline.sh predemo`, the `aml_bq1_demo` Airflow DAG, and
`pipeline.sh teardown`.
