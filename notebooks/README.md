# notebooks

Put `aml_xgboost.ipynb` here (owner: Qiaoye).

- Input: `curated/transactions/` synced from S3 (`aws s3 sync s3://<bucket>/curated/ ./curated/`), or the local output of `scripts/profile_aml.py` while AWS is being built.
- Split by time: train 1–7 Sep, test 8–10 Sep, drop 11–18 Sep.
- Settings: `scale_pos_weight = negatives / positives` (≈ 1,232), shallow trees, `eval_metric="aucpr"`, `tree_method="hist"`.
- Baseline to beat: F1 0.34, PR-AUC 0.31, 62.7% of laundering caught in the top 0.5% of scores.
- Outputs to `marts/model/`: feature importance, SHAP summary, detection rate per scheme, risk-weighted exposure by channel, currency and bank.
- Clear outputs before committing (`jupyter nbconvert --clear-output --inplace aml_xgboost.ipynb`) so the repo stays small.
