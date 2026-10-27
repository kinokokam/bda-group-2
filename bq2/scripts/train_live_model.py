"""
train_live_model.py  --  train the live score (model A, no look-ahead) for the live monitor.

Same split, settings and out-of-day account features as Experiment A in
notebooks/aml_xgboost.ipynb, but with "so far today" counts (monitoring/features.py), so a
replayed slice can be scored with no future data.

    fit 1-6 Sep, early stopping on 7 Sep, final check on 8-10 Sep (the 11-18 Sep tail is dropped)
    alert threshold = score at the top 0.5% of 7 Sep (a day the model did not fit on)

Writes out/live_model/: model.json, score_threshold.json, sender_features.parquet and
receiver_features.parquet (fitted on 1-7 Sep, used for every live slice) and metrics.json.

    python scripts/train_live_model.py          # about 5 min, about 3 GB RAM
"""
from __future__ import annotations

import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from monitoring.curate import curate_slice, load_currency_lookup, load_fx  # noqa: E402
from monitoring.features import (LIVE_FEATURES, account_features, fit_account_features,  # noqa: E402
                                 so_far_today, transaction_features)
from monitoring.replay import RAW_TRANS, TAIL_START, read_raw  # noqa: E402

OUT_DIR = REPO / "out" / "live_model"
SEED = 42
ALERT_SHARE = 0.005
VALID_START, TEST_START = pd.Timestamp("2022-09-07"), pd.Timestamp("2022-09-08")
XGB_PARAMS = dict(n_estimators=2000, learning_rate=0.1, max_depth=6, min_child_weight=5,
                  subsample=0.8, colsample_bytree=0.8, eval_metric="aucpr", tree_method="hist",
                  early_stopping_rounds=100, random_state=SEED, enable_categorical=True, n_jobs=-1)
EXPECTED = {"rows": 5_077_228, "laundering": 4_522}     # 1-10 Sep after de-duplication


def top_count(n: int) -> int:
    return math.ceil(ALERT_SHARE * n)


def main() -> None:
    t0 = time.time()
    raw = read_raw(RAW_TRANS)
    df = curate_slice(raw[raw["ts"] < TAIL_START], load_fx(), load_currency_lookup())
    del raw
    assert len(df) == EXPECTED["rows"], f"rows {len(df):,}"
    assert int(df["is_laundering"].sum()) == EXPECTED["laundering"], "laundering rows"
    print(f"loaded {len(df):,} rows ({time.time() - t0:.0f} s)")

    is_train = (df["ts"] < TEST_START).values
    is_fit = (df["ts"] < VALID_START).values
    is_valid, is_test = is_train & ~is_fit, ~is_train

    # Per-transfer and so-far-today features, then out-of-day account features as in the notebook
    X = pd.concat([transaction_features(df), so_far_today(df)], axis=1)
    acct = pd.DataFrame(index=df.index, columns=LIVE_FEATURES[-7:], dtype="float32")
    day = df["ts"].dt.day.values
    for d in range(1, 8):                                        # train day d <- the other six train days
        rows = is_train & (day == d)
        acct.loc[rows] = account_features(df[rows], *fit_account_features(df[is_train & (day != d)])).values
    snd, rcv = fit_account_features(df[is_train])                # test and live slices <- all of 1-7 Sep
    acct.loc[is_test] = account_features(df[is_test], snd, rcv).values
    X = pd.concat([X, acct.astype("float32")], axis=1)[LIVE_FEATURES]
    y = df["is_laundering"].values
    print(f"features built ({time.time() - t0:.0f} s)")

    spw = float((y[is_fit] == 0).sum() / (y[is_fit] == 1).sum())
    model = xgb.XGBClassifier(scale_pos_weight=spw, **XGB_PARAMS)
    model.fit(X[is_fit], y[is_fit], eval_set=[(X[is_valid], y[is_valid])], verbose=250)

    valid_scores = model.predict_proba(X[is_valid])[:, 1]
    threshold = float(np.sort(valid_scores)[::-1][top_count(len(valid_scores)) - 1])
    test_scores = model.predict_proba(X[is_test])[:, 1]
    y_test = y[is_test]
    top = np.argsort(-test_scores, kind="stable")[:top_count(len(test_scores))]
    metrics = {
        "best_iteration": int(model.best_iteration),
        "valid_pr_auc": float(model.best_score),
        "test_pr_auc": float(average_precision_score(y_test, test_scores)),
        "test_recall_top_0_5pct": float(y_test[top].sum() / y_test.sum()),
        "test_share_at_or_above_threshold": float((test_scores >= threshold).mean()),
        "test_recall_at_threshold": float(y_test[test_scores >= threshold].sum() / y_test.sum()),
        "test_precision_at_threshold": float(y_test[test_scores >= threshold].mean()),
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(OUT_DIR / "model.json")
    snd.to_parquet(OUT_DIR / "sender_features.parquet")
    rcv.to_parquet(OUT_DIR / "receiver_features.parquet")
    (OUT_DIR / "score_threshold.json").write_text(json.dumps(
        {"alert_share": ALERT_SHARE, "threshold": threshold, "computed_on": "valid 7 Sep, live model A"}, indent=2))
    (OUT_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))
    print(f"threshold {threshold:.6f}; wrote {OUT_DIR} ({(time.time() - t0) / 60:.1f} min)")


if __name__ == "__main__":
    main()
