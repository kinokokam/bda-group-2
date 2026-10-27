"""
XGBoost Anti-Money Laundering (AML) Training, Calibration & Evaluation Module.

Reads processed Parquet splits from:
- s3://<BUCKET_NAME>/processed/train/
- s3://<BUCKET_NAME>/processed/val/
- s3://<BUCKET_NAME>/processed/test/

Exports to S3 Model Layer (s3://<BUCKET_NAME>/model/):
- xgboost_aml_model.json : Trained XGBoost tree model
- xgboost_features.json  : Ordered feature sequence contract for deterministic streaming inference
- category_maps.json     : String-to-integer category lookup dictionaries
- calibrator.pkl         : Isotonic regression model mapping distorted boosted scores to true probabilities
- config.json            : Operational config with tuned alert threshold (tau*), top-k capacity, and PR-AUC

Exports to S3 Evaluation Layer (s3://<BUCKET_NAME>/evaluation/):
- metrics.json           : PR-AUC, ROC-AUC, Recall, Precision, F2, F1, and Top-K Recall
- confusion_matrix.png   : High-resolution confusion matrix plot
- pr_curve.png           : Precision-Recall curve under extreme class imbalance

Hyperparameters:
- scale_pos_weight       : N_neg / N_pos (counteracts extreme ~0.2%-1% class imbalance)
- eval_metric            : "aucpr" (optimizes Precision-Recall AUC)
- max_depth              : 6 (captures multi-hop laundering synergies)
- learning_rate          : 0.05 (prevents erratic step sizes)
- n_estimators           : 300 (number of boosting rounds)
- early_stopping_rounds  : 30 (prevents temporal lookahead overfitting)
- subsample / colsample  : 0.8 / 0.8 (stochastic regularization)
- tree_method            : "hist" (high-speed histogram binning)
"""

import os
import sys
import json
import argparse
import logging
import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# Configure matplotlib for headless server / CLI execution
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_recall_curve,
    confusion_matrix,
    classification_report,
    f1_score,
    fbeta_score,
    precision_score,
    recall_score
)

# Automatically load credentials from .env if present
def _load_env_file():
    for base in [os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), os.getcwd()]:
        env_file = os.path.join(base, ".env")
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip("\"' ")
                        if k not in os.environ:
                            os.environ[k] = v
            break

_load_env_file()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("train_xgboost")

# Core Feature Columns matching Spark ETL output
NUMERIC_FEATURES = [
    "amount_paid_usd",
    "amount_received_usd",
    "paid_fx_rate",
    "recv_fx_rate",
    "fx_amount_discrepancy",
    "is_structuring_threshold",
    "is_cross_currency",
    "is_cross_bank",
    "is_round_amount",
    "tx_hour",
    "tx_dayofweek",
    "tx_dayofmonth",
    "is_weekend",
    "sender_tx_count_1h",
    "sender_volume_1h",
    "sender_tx_count_24h",
    "sender_volume_24h",
    "sender_tx_count_7d",
    "sender_volume_7d",
    "sender_burst_ratio_1h_24h",
    "sender_burst_ratio_24h_7d",
    "sender_time_delta_sec",
    "receiver_tx_count_24h",
    "receiver_volume_24h",
    "receiver_time_delta_sec"
]

CATEGORICAL_FEATURES = [
    "payment_format",
    "payment_currency",
    "receiving_currency",
    "from_bank",
    "to_bank"
]


def load_parquet_dataset(source_path: str, s3_bucket: str = None, s3_prefix: str = None) -> pd.DataFrame:
    """
    Loads Parquet data from local path or downloads from S3 if source_path starts with s3://.
    Projects only the required numeric, categorical and label columns to minimize memory.
    """
    if source_path.startswith("s3://"):
        import boto3
        s3 = boto3.client("s3")
        parts = source_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        local_tmp = f"/tmp/data_s3/{prefix.strip('/')}"
        os.makedirs(local_tmp, exist_ok=True)
        logger.info(f"Downloading s3://{bucket}/{prefix} to {local_tmp}...")

        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet"):
                    dest_file = os.path.join(local_tmp, os.path.basename(key))
                    s3.download_file(bucket, key, dest_file)
        source_path = local_tmp

    logger.info(f"Reading Parquet dataset from {source_path}...")
    needed_cols = set(NUMERIC_FEATURES + CATEGORICAL_FEATURES + ["is_laundering"])

    if os.path.isdir(source_path):
        parquet_files = [os.path.join(source_path, f) for f in os.listdir(source_path) if f.endswith(".parquet")]
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {source_path}")
        schema = pq.read_schema(parquet_files[0])
        cols_to_read = [c for c in schema.names if c in needed_cols]
        table = pq.read_table(source_path, columns=cols_to_read)
    else:
        file_path = source_path if os.path.exists(source_path) else f"{source_path}.parquet"
        schema = pq.read_schema(file_path)
        cols_to_read = [c for c in schema.names if c in needed_cols]
        table = pq.read_table(file_path, columns=cols_to_read)

    return table.to_pandas()


def prepare_features(train_df: pd.DataFrame, val_df: pd.DataFrame, test_df: pd.DataFrame):
    """
    Builds fixed category mapping dictionaries and converts features to lightweight float32 matrices.
    Eliminates in-memory DataFrame copies for high memory efficiency.
    """
    y_train = train_df["is_laundering"].astype(int).values
    y_val = val_df["is_laundering"].astype(int).values
    y_test = test_df["is_laundering"].astype(int).values

    num_cols = [c for c in NUMERIC_FEATURES if c in train_df.columns]
    cat_cols = [c for c in CATEGORICAL_FEATURES if c in train_df.columns]

    category_maps = {}
    for cat in cat_cols:
        unique_vals = sorted(train_df[cat].dropna().astype(str).unique().tolist())
        mapping = {val: idx for idx, val in enumerate(unique_vals)}
        category_maps[cat] = mapping

    def _build_matrix(df: pd.DataFrame) -> np.ndarray:
        arrays = []
        for col in num_cols:
            arrays.append(df[col].fillna(0).astype(np.float32).values)
        for cat in cat_cols:
            mapping = category_maps[cat]
            mapped = df[cat].astype(str).map(mapping).fillna(0).astype(np.float32).values
            arrays.append(mapped)
        return np.column_stack(arrays).astype(np.float32)

    X_train = _build_matrix(train_df)
    X_val = _build_matrix(val_df)
    X_test = _build_matrix(test_df)

    feature_cols = num_cols + [f"{cat}_code" for cat in cat_cols]
    logger.info(f"Feature matrix prepared with {len(feature_cols)} features: {feature_cols}")
    return X_train, y_train, X_val, y_val, X_test, y_test, feature_cols, category_maps


def train_model(X_train, y_train, X_val, y_val, feature_names):
    """
    Trains XGBoost classifier configured with prescribed AML hyperparameters:
    - scale_pos_weight: N_neg / N_pos
    - eval_metric: "aucpr"
    - max_depth: 6
    - learning_rate: 0.05
    - n_estimators: 300
    - early_stopping_rounds: 30
    - subsample: 0.8
    - colsample_bytree: 0.8
    - tree_method: "hist"
    """
    import xgboost as xgb

    pos_count = int(np.sum(y_train))
    neg_count = int(len(y_train) - pos_count)
    scale_pos_weight = float(neg_count / max(pos_count, 1))

    logger.info(f"Class distribution: Positives={pos_count}, Negatives={neg_count}")
    logger.info(f"Configuring scale_pos_weight = {scale_pos_weight:.2f}")
    logger.info("Initializing native XGBoost Classifier with prescribed AML hyperparameters...")

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        early_stopping_rounds=30,
        tree_method="hist",
        random_state=42,
        n_jobs=-1
    )
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        verbose=10
    )
    logger.info("XGBoost model training completed successfully.")
    return model, "xgboost", scale_pos_weight


def fit_probability_calibrator_and_tune_threshold(model, X_val, y_val):
    """
    Artifact 3 (calibrator.pkl) & Artifact 4 (config.json derivation):
    Fits Isotonic Regression on validation predictions to map boosted scores back
    to true posterior probabilities P(Is Laundering = 1 | x).
    Sweeps thresholds on calibrated probabilities to identify the optimal alert threshold tau*
    that maximizes the F2 score (beta=2, Recall-weighted for AML compliance).
    """
    logger.info("Fitting Isotonic Probability Calibrator on held-out validation set...")
    val_raw_scores = model.predict_proba(X_val)[:, 1]

    calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
    if len(np.unique(y_val)) > 1:
        calibrator.fit(val_raw_scores, y_val)
    else:
        # Fallback calibration for single-class subsets
        calibrator.fit(np.array([0.0, 1.0]), np.array([0.0, 1.0]))

    val_calibrated_scores = calibrator.predict(val_raw_scores)
    val_prauc = float(average_precision_score(y_val, val_calibrated_scores)) if len(np.unique(y_val)) > 1 else 0.0

    # Operational Decision Threshold Sweep (tau* that maximizes F2 score: Recall-weighted)
    best_thresh = 0.0500
    best_f2 = 0.0
    val_recall_at_best = 0.0
    val_prec_at_best = 0.0

    if len(np.unique(y_val)) > 1:
        # Fine-grained sweep from 0.005 to 0.800 (160 steps)
        thresholds = np.linspace(0.005, 0.80, 160)
        for t in thresholds:
            preds = (val_calibrated_scores >= t).astype(int)
            f2 = float(fbeta_score(y_val, preds, beta=2, zero_division=0))
            if f2 > best_f2:
                best_f2 = f2
                best_thresh = float(t)

        best_preds = (val_calibrated_scores >= best_thresh).astype(int)
        val_recall_at_best = float(recall_score(y_val, best_preds, zero_division=0))
        val_prec_at_best = float(precision_score(y_val, best_preds, zero_division=0))

    logger.info(
        f"Validation Calibrated PR-AUC: {val_prauc:.4f} | "
        f"Optimal Alert Threshold (tau* based on F2): {best_thresh:.4f} "
        f"(F2: {best_f2:.4f}, Recall: {val_recall_at_best:.4f}, Precision: {val_prec_at_best:.4f})"
    )
    return calibrator, round(best_thresh, 4), round(best_f2, 4), round(val_prauc, 4)


def evaluate_model(model, calibrator, optimal_threshold: float, X_test, y_test, output_eval_dir: str):
    """
    Computes test set metrics: PR-AUC, ROC-AUC, Precision, Recall, F1-Score, and Top-K Recall.
    Generates and saves confusion matrix and PR curve visualizations.
    """
    os.makedirs(output_eval_dir, exist_ok=True)
    logger.info("Evaluating model on chronological Test set using calibrated probabilities...")

    raw_test_scores = model.predict_proba(X_test)[:, 1]
    calibrated_test_scores = calibrator.predict(raw_test_scores)
    y_pred = (calibrated_test_scores >= optimal_threshold).astype(int)

    pr_auc = float(average_precision_score(y_test, calibrated_test_scores)) if len(np.unique(y_test)) > 1 else 0.0
    roc_auc = float(roc_auc_score(y_test, calibrated_test_scores)) if len(np.unique(y_test)) > 1 else 0.0
    precision = float(precision_score(y_test, y_pred, zero_division=0))
    recall = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    f2 = float(fbeta_score(y_test, y_pred, beta=2, zero_division=0))

    # Top-K Alert Capacity Recall (Top 1%, 2%, 5%, 10% daily triage volume)
    total_tx = len(y_test)
    total_positives = max(int(np.sum(y_test)), 1)
    sorted_indices = np.argsort(calibrated_test_scores)[::-1]

    top_k_recalls = {}
    for pct in [1, 2, 5, 10]:
        k = max(1, int(total_tx * (pct / 100.0)))
        top_k_idx = sorted_indices[:k]
        positives_caught = int(np.sum(y_test[top_k_idx]))
        top_k_recall = float(positives_caught / total_positives)
        top_k_recalls[f"top_{pct}pct_alert_recall"] = round(top_k_recall, 4)

    logger.info(
        f"Test Set Evaluation: PR-AUC={pr_auc:.4f} | ROC-AUC={roc_auc:.4f} | "
        f"Recall={recall:.4f} | Precision={precision:.4f} | F2={f2:.4f} | F1={f1:.4f}"
    )
    logger.info(f"Top-K Alert Capacity Recall: {top_k_recalls}")

    cm = confusion_matrix(y_test, y_pred).tolist()
    metrics = {
        "pr_auc": round(pr_auc, 4),
        "roc_auc": round(roc_auc, 4),
        "recall": round(recall, 4),
        "precision": round(precision, 4),
        "f2_score": round(f2, 4),
        "f1_score": round(f1, 4),
        "optimization_metric": "f2_score",
        "alert_threshold_tau": optimal_threshold,
        "test_sample_count": total_tx,
        "test_positive_count": int(np.sum(y_test)),
        "confusion_matrix": cm,
        "top_k_alert_capacity": top_k_recalls
    }

    # 1. Save metrics.json
    metrics_path = os.path.join(output_eval_dir, "metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    logger.info(f"Saved evaluation metrics to {metrics_path}")

    # 2. Confusion Matrix Plot
    cm_path = os.path.join(output_eval_dir, "confusion_matrix.png")
    fig, ax = plt.subplots(figsize=(6, 5))
    cm_arr = np.array(cm)
    cax = ax.matshow(cm_arr, cmap=plt.cm.Blues, alpha=0.8)
    for i in range(cm_arr.shape[0]):
        for j in range(cm_arr.shape[1]):
            ax.text(x=j, y=i, s=str(cm_arr[i, j]), va='center', ha='center', size='xx-large')
    plt.xlabel(f'Predicted Labels (tau* = {optimal_threshold})', fontsize=12)
    plt.ylabel('True Labels', fontsize=12)
    plt.title('AML Detection Confusion Matrix', fontsize=14, pad=15)
    plt.colorbar(cax)
    plt.tight_layout()
    plt.savefig(cm_path, dpi=150)
    plt.close()
    logger.info(f"Saved confusion matrix plot to {cm_path}")

    # 3. Precision-Recall Curve Plot
    pr_path = os.path.join(output_eval_dir, "pr_curve.png")
    if len(np.unique(y_test)) > 1:
        p_vals, r_vals, _ = precision_recall_curve(y_test, calibrated_test_scores)
        plt.figure(figsize=(7, 5))
        plt.plot(r_vals, p_vals, label=f"XGBoost (PR-AUC = {pr_auc:.3f})", color="#1f77b4", lw=2)
        plt.xlabel("Recall", fontsize=12)
        plt.ylabel("Precision", fontsize=12)
        plt.title("Precision-Recall Curve (AML Class Imbalance)", fontsize=14)
        plt.legend(loc="upper right")
        plt.grid(True, linestyle="--", alpha=0.6)
        plt.tight_layout()
        plt.savefig(pr_path, dpi=150)
        plt.close()
        logger.info(f"Saved PR curve plot to {pr_path}")

    return metrics, metrics_path, cm_path, pr_path


def save_companion_artifacts(
    model,
    model_type: str,
    feature_names: list,
    category_maps: dict,
    calibrator,
    config_dict: dict,
    output_model_dir: str
):
    """
    Saves the 4 Companion Artifacts to model layer (s3://<BUCKET_NAME>/model/) for deterministic inference:
    1. xgboost_features.json (The Column Sequence Contract)
    2. category_maps.json    (The Category Dictionary)
    3. calibrator.pkl        (Probability Calibrator)
    4. config.json           (Operational Decision Config)
    Plus the model weights (xgboost_aml_model.json and joblib).
    """
    os.makedirs(output_model_dir, exist_ok=True)

    # 1. xgboost_features.json
    feat_path = os.path.join(output_model_dir, "xgboost_features.json")
    with open(feat_path, "w") as f:
        json.dump(feature_names, f, indent=2)
    logger.info(f"Saved Artifact 1 (Feature Contract): {feat_path}")

    # 2. category_maps.json
    cat_path = os.path.join(output_model_dir, "category_maps.json")
    with open(cat_path, "w") as f:
        json.dump(category_maps, f, indent=2)
    logger.info(f"Saved Artifact 2 (Category Dictionary): {cat_path}")

    # 3. calibrator.pkl
    cal_path = os.path.join(output_model_dir, "calibrator.pkl")
    joblib.dump(calibrator, cal_path)
    logger.info(f"Saved Artifact 3 (Probability Calibrator): {cal_path}")

    # 4. config.json
    cfg_path = os.path.join(output_model_dir, "config.json")
    with open(cfg_path, "w") as f:
        json.dump(config_dict, f, indent=2)
    logger.info(f"Saved Artifact 4 (Operational Config): {cfg_path}")

    # Model Weights (xgboost_aml_model.json)
    aml_model_path = os.path.join(output_model_dir, "xgboost_aml_model.json")
    joblib_model_path = os.path.join(output_model_dir, "xgboost_model.joblib")

    joblib.dump(model, joblib_model_path)
    model.save_model(aml_model_path)
    logger.info(f"Saved native XGBoost JSON model: {aml_model_path}")

    return {
        "features": feat_path,
        "category_maps": cat_path,
        "calibrator": cal_path,
        "config": cfg_path,
        "model": aml_model_path
    }


def upload_directory_to_s3(local_dir: str, s3_bucket: str, s3_prefix: str):
    """Uploads local artifacts in directory to S3 prefix."""
    import boto3
    s3 = boto3.client("s3")
    logger.info(f"Uploading artifacts from {local_dir} to s3://{s3_bucket}/{s3_prefix}/...")
    for root, _, files in os.walk(local_dir):
        for f in files:
            local_file = os.path.join(root, f)
            s3_key = f"{s3_prefix}/{f}"
            s3.upload_file(local_file, s3_bucket, s3_key)
            logger.info(f"Uploaded s3://{s3_bucket}/{s3_key}")


def run_training_pipeline(
    processed_dir: str = "",
    output_eval_dir: str = "/tmp/eval",
    output_model_dir: str = "/tmp/model",
    s3_bucket: str = None
):
    """Executes end-to-end model training, calibration, evaluation, and artifact storage."""
    logger.info("=== STEP 4, 5 & 6: XGBoost Model Training, Calibration & Evaluation ===")
    s3_bucket = s3_bucket or os.environ.get("S3_BUCKET_NAME", "")
    if not s3_bucket and not processed_dir:
        raise ValueError("S3_BUCKET_NAME is required for model training and artifact storage.")

    target_mode = f"AWS (s3://{s3_bucket})"
    logger.info(f"Target Environment: {target_mode}")

    # 1. Determine Paths
    if s3_bucket:
        train_path = f"s3://{s3_bucket}/processed/train"
        val_path = f"s3://{s3_bucket}/processed/val"
        test_path = f"s3://{s3_bucket}/processed/test"
    else:
        train_path = os.path.join(processed_dir, "train")
        val_path = os.path.join(processed_dir, "val")
        test_path = os.path.join(processed_dir, "test")

    # 2. Load Splits
    train_df = load_parquet_dataset(train_path)
    val_df = load_parquet_dataset(val_path)
    test_df = load_parquet_dataset(test_path)

    # 3. Prepare Feature Matrices & Category Maps
    X_train, y_train, X_val, y_val, X_test, y_test, feature_names, category_maps = prepare_features(
        train_df, val_df, test_df
    )

    # 4. Train Model with Prescribed Hyperparameters (Arrow ④)
    model, model_type, scale_pos_weight = train_model(X_train, y_train, X_val, y_val, feature_names)

    # 5. Probability Calibration & Optimal Alert Threshold Determination (Artifact 3 & 4)
    calibrator, optimal_threshold, best_val_f2, val_prauc = fit_probability_calibrator_and_tune_threshold(
        model, X_val, y_val
    )

    # 6. Evaluate on Test Set and Save Metrics & Visualizations (Arrow ⑤)
    eval_dir = output_eval_dir
    metrics, metrics_path, cm_path, pr_path = evaluate_model(
        model, calibrator, optimal_threshold, X_test, y_test, eval_dir
    )

    # 7. Build Operational Config Dictionary
    config_dict = {
        "model_version": "v1.0",
        "calibrated_threshold": optimal_threshold,
        "top_k_daily_capacity": 200,
        "validation_prauc": val_prauc,
        "test_prauc": metrics["pr_auc"],
        "scale_pos_weight": round(scale_pos_weight, 2),
        "optimization_metric": "f2_score",
        "optimal_val_f2_score": best_val_f2,
        "test_f2_score": metrics["f2_score"],
        "test_f1_score": metrics["f1_score"],
        "test_recall": metrics["recall"],
        "test_precision": metrics["precision"],
        "hyperparameters": {
            "scale_pos_weight": round(scale_pos_weight, 2),
            "eval_metric": "aucpr",
            "max_depth": 6,
            "learning_rate": 0.05,
            "n_estimators": 300,
            "early_stopping_rounds": 30,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "tree_method": "hist"
        }
    }

    # 8. Save the 4 Companion Model Artifacts (Arrow ⑥)
    mdl_dir = output_model_dir
    saved_artifacts = save_companion_artifacts(
        model=model,
        model_type=model_type,
        feature_names=feature_names,
        category_maps=category_maps,
        calibrator=calibrator,
        config_dict=config_dict,
        output_model_dir=mdl_dir
    )

    # 9. Upload to S3 if running in AWS mode
    if s3_bucket:
        upload_directory_to_s3(eval_dir, s3_bucket, "evaluation")
        upload_directory_to_s3(mdl_dir, s3_bucket, "model")

    logger.info("=== STEP 4, 5 & 6: Training, Calibration & Evaluation Successfully Completed ===")
    return metrics, saved_artifacts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train, Calibrate and Evaluate XGBoost AML Classifier")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET_NAME", ""), help="S3 bucket name (AWS)")
    parser.add_argument("--processed-dir", type=str, default="", help="Path to processed Parquet directory")
    parser.add_argument("--eval-dir", type=str, default="/tmp/eval", help="Staging directory for evaluation artifacts")
    parser.add_argument("--model-dir", type=str, default="/tmp/model", help="Staging directory for model artifacts")

    args = parser.parse_args()

    run_training_pipeline(
        processed_dir=args.processed_dir,
        output_eval_dir=args.eval_dir,
        output_model_dir=args.model_dir,
        s3_bucket=args.s3_bucket
    )
