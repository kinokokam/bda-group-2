"""Feature derivation shared by the Flink job, mirroring the Spark ETL exactly.

Every formula here is copied from `bq1/spark/etl_pipeline.py`. A difference does
not crash anything — the model simply returns meaningless scores — so this
module is the one place the two implementations must be kept in step.
"""

import csv
import hashlib
from datetime import datetime
from typing import Optional

# Copied verbatim from the ETL. Static training-time constants, not live rates:
# using today's rates would shift every amount away from what the model learned.
FX_RATES = {
    "USD": 1.0,
    "US DOLLAR": 1.0,
    "EUR": 1.08,
    "EURO": 1.08,
    "GBP": 1.28,
    "UK POUND": 1.28,
    "CHF": 1.12,
    "SWISS FRANC": 1.12,
    "CAD": 0.74,
    "CANADIAN DOLLAR": 0.74,
    "AUD": 0.66,
    "AUSTRALIAN DOLLAR": 0.66,
    "JPY": 0.0067,
}

ONE_HOUR_SECONDS = 3600
ONE_DAY_SECONDS = 86400
SEVEN_DAYS_SECONDS = 604800

# What the ETL substitutes when an account has no prior transaction.
NO_PRIOR_TRANSACTION_SECONDS = 86400.0 * 30

RAW_COLUMNS = {
    "Timestamp": "timestamp",
    "From Bank": "from_bank",
    "Account": "from_account",
    "To Bank": "to_bank",
    "Account.1": "to_account",
    "Amount Received": "amount_received",
    "Receiving Currency": "receiving_currency",
    "Amount Paid": "amount_paid",
    "Payment Currency": "payment_currency",
    "Payment Format": "payment_format",
    "Is Laundering": "is_laundering",
}


def fx_rate(currency: str) -> float:
    """Return the training-time USD rate for a currency name.

    Parameters
    ----------
    currency : str
        Currency as written in the raw file, any case.

    Returns
    -------
    float
        Rate to multiply by, defaulting to 1.0 for anything unrecognised, which
        is what the ETL does.
    """
    return FX_RATES.get(currency.upper(), 1.0)


def parse_timestamp(raw: str) -> datetime:
    """Parse a raw timestamp the way the ETL sanitises it.

    Parameters
    ----------
    raw : str
        Timestamp such as `2022/09/10 11:00`, with or without seconds.

    Returns
    -------
    datetime
        Parsed timestamp.

    Raises
    ------
    ValueError
        If the string does not match either accepted layout.
    """
    sanitised = raw.replace("/", "-")
    if len(sanitised) == 16:
        sanitised = f"{sanitised}:00"
    return datetime.strptime(sanitised, "%Y-%m-%d %H:%M:%S")


def derive_row(raw: dict[str, str]) -> dict[str, float | int | str]:
    """Build the per-transaction features that need no account history.

    Parameters
    ----------
    raw : dict[str, str]
        One record as the replay Lambda sends it, raw column names plus `txn_id`.

    Returns
    -------
    dict[str, float | int | str]
        Identity fields, amounts in USD, temporal fields and the structural flags.

    Raises
    ------
    ValueError
        If the timestamp or either amount cannot be parsed.
    """
    moment = parse_timestamp(raw["Timestamp"])
    epoch = moment.timestamp()

    amount_paid = float(raw["Amount Paid"])
    amount_received = float(raw["Amount Received"])
    paid_rate = fx_rate(raw["Payment Currency"])
    received_rate = fx_rate(raw["Receiving Currency"])
    amount_paid_usd = round(amount_paid * paid_rate, 2)
    amount_received_usd = round(amount_received * received_rate, 2)

    # Spark's dayofweek is 1 for Sunday through 7 for Saturday.
    spark_dayofweek = (moment.isoweekday() % 7) + 1

    is_structuring = 9000.0 <= amount_paid_usd <= 9999.99
    is_round = amount_paid_usd > 100.0 and amount_paid_usd % 1000.0 == 0.0

    return {
        "txn_id": raw.get("txn_id", ""),
        "event_time": moment.strftime("%Y-%m-%d %H:%M:%S"),
        "tx_epoch": epoch,
        "from_bank": raw["From Bank"],
        "from_account": raw["Account"],
        "to_bank": raw["To Bank"],
        "to_account": raw.get("Account.1", raw.get("Account_1", "")),
        "amount_paid": amount_paid,
        "amount_received": amount_received,
        "payment_currency": raw["Payment Currency"],
        "receiving_currency": raw["Receiving Currency"],
        "payment_format": raw["Payment Format"],
        "label": int(raw.get("Is Laundering", 0)),
        "paid_fx_rate": paid_rate,
        "recv_fx_rate": received_rate,
        "amount_paid_usd": amount_paid_usd,
        "amount_received_usd": amount_received_usd,
        "fx_amount_discrepancy": round(abs(amount_paid_usd - amount_received_usd), 2),
        "is_structuring_threshold": int(is_structuring),
        "is_cross_currency": int(raw["Payment Currency"].upper() != raw["Receiving Currency"].upper()),
        "is_cross_bank": int(raw["From Bank"] != raw["To Bank"]),
        "is_round_amount": int(is_round),
        "tx_hour": moment.hour,
        "tx_dayofweek": spark_dayofweek,
        "tx_dayofmonth": moment.day,
        "is_weekend": int(spark_dayofweek in (1, 7)),
    }


def window_totals(history: list[tuple[float, float]], now: float, span: int) -> tuple[int, float]:
    """Count and sum the entries inside a trailing window, current row included.

    Parameters
    ----------
    history : list[tuple[float, float]]
        `(epoch, amount)` pairs, the current transaction already appended.
    now : float
        Epoch of the current transaction.
    span : int
        Window length in seconds, matching Spark's `rangeBetween(-span, 0)`.

    Returns
    -------
    tuple[int, float]
        Number of entries and their total.
    """
    inside = [amount for epoch, amount in history if now - span <= epoch <= now]
    return len(inside), round(sum(inside), 2)


def burst_ratio(short_volume: float, long_volume: float, long_days: float) -> float:
    """Divide a short window's volume by the long window's daily average.

    Parameters
    ----------
    short_volume : float
        Total over the shorter window.
    long_volume : float
        Total over the longer window.
    long_days : float
        Length of the longer window in days.

    Returns
    -------
    float
        Ratio rounded to four places. The `+ 1.0` matches the ETL and keeps a
        quiet account from dividing by zero.
    """
    return round(short_volume / ((long_volume / long_days) + 1.0), 4)


CSV_HEADER = tuple(RAW_COLUMNS)


def dedupe_header(header: list[str]) -> list[str]:
    """Rename repeated column names the way pandas does.

    The source file names both the sender and receiver column `Account`. A
    plain DictReader keeps only the last, which silently drops the sender.

    Parameters
    ----------
    header : list[str]
        Column names as they appear in the file.

    Returns
    -------
    list[str]
        Names with the nth repeat suffixed `.n`.
    """
    counts: dict[str, int] = {}
    names = []
    for name in header:
        if name in counts:
            counts[name] += 1
            names.append(f"{name}.{counts[name]}")
        else:
            counts[name] = 0
            names.append(name)
    return names


def row_from_csv_line(line: str) -> Optional[dict[str, str]]:
    """Parse one line of an hourly file into the raw row shape.

    Used by the file-source path, which cannot rely on reading a file's header
    before its rows. `txn_id` comes from the line's own digest, since a file
    reader has no row counter.

    Parameters
    ----------
    line : str
        One line from `bronze/hourly/*.csv`.

    Returns
    -------
    Optional[dict[str, str]]
        Row keyed as `RAW_COLUMNS`, or None for a header or malformed line.
    """
    values = next(csv.reader([line]), [])
    if not values or values[0] == CSV_HEADER[0] or len(values) != len(CSV_HEADER):
        return None
    row = dict(zip(CSV_HEADER, values))
    row["txn_id"] = hashlib.blake2s(line.encode(), digest_size=8).hexdigest()
    return row


def self_check() -> None:
    """Verify the file reader agrees with the header-driven reader.

    Returns
    -------
    None
    """
    header = "Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering"
    line = "2022/09/10 11:00,001,800C4C080,001853,802CE2B00,1476.15,US Dollar,1476.15,US Dollar,Credit Card,0"

    assert dedupe_header(header.split(",")) == list(CSV_HEADER)

    from_file = row_from_csv_line(line)
    assert from_file is not None
    from_header = dict(zip(dedupe_header(header.split(",")), next(csv.reader([line]))))
    assert {k: v for k, v in from_file.items() if k != "txn_id"} == from_header

    assert row_from_csv_line(header) is None
    assert row_from_csv_line("1,2,3") is None

    repeated = row_from_csv_line(line)
    assert repeated is not None
    assert repeated["txn_id"] == from_file["txn_id"]

    derived = derive_row(from_file)
    assert derived["from_account"] == "800C4C080"
    assert derived["to_account"] == "802CE2B00"
    assert derived["amount_paid_usd"] == 1476.15


if __name__ == "__main__":
    self_check()
    print("ok")
