import pandas as pd

from monitoring.curate import curate_slice
from monitoring.features import LIVE_FEATURES, fit_account_features, live_features, so_far_today


def transfers(rows):
    cols = ["ts", "from_bank", "from_account", "to_bank", "to_account", "amount_received", "receiving_currency",
            "amount_paid", "payment_currency", "payment_format", "is_laundering"]
    df = pd.DataFrame(rows, columns=cols)
    df["ts"] = pd.to_datetime(df["ts"])
    return df


ROWS = [
    ("2022-09-03 09:00", "070", "A", "001", "B", 100.0, "Euro", 100.0, "Euro", "ACH", 0),
    ("2022-09-03 10:00", "070", "A", "001", "C", 2000.0, "Euro", 2000.0, "Euro", "ACH", 1),
    ("2022-09-03 10:00", "070", "A", "001", "B", 50.0, "Euro", 50.0, "Euro", "Cash", 0),
    ("2022-09-03 23:00", "070", "A", "001", "B", 50.0, "Euro", 50.0, "Euro", "Cash", 0),
    ("2022-09-04 01:00", "070", "A", "001", "B", 50.0, "Euro", 50.0, "Euro", "Cash", 0),
]
FX = pd.DataFrame({"rate_date": ["2022/09/03", "2022/09/04"], "iso_code": ["EUR", "EUR"], "units_per_usd": [0.5, 0.5]})
LOOKUP = pd.DataFrame({"currency_name": ["Euro"], "iso_code": ["EUR"]})


def test_so_far_today_ignores_later_transfers_and_resets_each_day():
    counts = so_far_today(transfers(ROWS))["sender_txns_so_far_today"].tolist()
    # 09:00 -> 1; both 10:00 rows -> 3 (same minute counts); 23:00 -> 4; next day restarts
    assert counts == [1, 3, 3, 4, 1]


def test_scoring_a_morning_slice_gives_the_same_counts_as_the_full_day():
    full = so_far_today(transfers(ROWS))
    morning = so_far_today(transfers(ROWS[:3]))       # what the live monitor has at 11:00
    assert morning.equals(full.iloc[:3])


def test_curate_drops_rows_without_an_fx_rate():
    weekday_only = FX[FX["rate_date"] == "2022/09/03"]
    assert len(curate_slice(transfers(ROWS), weekday_only, LOOKUP)) == 4
    assert curate_slice(transfers(ROWS), FX, LOOKUP)["amount_paid_usd"].iloc[0] == 200.0


def test_live_features_columns_and_unseen_accounts():
    df = curate_slice(transfers(ROWS), FX, LOOKUP)
    snd, rcv = fit_account_features(df.iloc[:2])
    X = live_features(df, snd, rcv)
    assert X.columns.tolist() == LIVE_FEATURES
    assert X["sender_out_txn_count"].iloc[0] == 2
    assert X["receiver_fan_in_degree"].isna().sum() == 0 and pd.isna(rcv.reindex(["Z"]).iloc[0, 0])
