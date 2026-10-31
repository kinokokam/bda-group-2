import pandas as pd
import pytest

from monitoring import replay

HEADER = ("Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,"
          "Amount Paid,Payment Currency,Payment Format,Is Laundering\n")
ROWS = [
    "2022/09/01 00:00,010,8000EBD30,010,8000EBD30,100.0,US Dollar,100.0,US Dollar,Reinvestment,0",
    "2022/09/01 00:00,010,8000EBD30,010,8000EBD30,100.0,US Dollar,100.0,US Dollar,Reinvestment,0",  # exact dup
    "2022/09/01 00:59,070,800A1B2C3,001,8001D4E5F,5.0,Euro,5.0,Euro,ACH,1",
    "2022/09/01 13:20,001,8001D4E5F,010,8000EBD30,6.0,Euro,6.0,Euro,Cheque,0",            # same day, later hour
    "2022/09/10 23:30,001,8001D4E5F,070,800A1B2C3,7.0,Yen,7.0,Yen,Cash,0",
    "2022/09/11 03:10,070,800A1B2C3,012,80023A4B5,9.0,US Dollar,9.0,US Dollar,ACH,1",
    "2022/09/11 22:45,012,80023A4B5,070,800A1B2C3,9.0,US Dollar,9.0,US Dollar,ACH,1",
    "2022/09/12 01:00,012,80023A4B5,070,800A1B2C3,9.0,US Dollar,9.0,US Dollar,ACH,1",   # beyond the replay
]


@pytest.fixture
def stream(tmp_path):
    csv = tmp_path / "trans.csv"
    csv.write_text(HEADER + "\n".join(ROWS) + "\n")
    out = tmp_path / "stream"
    return replay.write_slices(replay.read_raw(csv), out), out


def test_one_slice_per_day_including_the_tail_day(stream):
    ids, _ = stream
    assert ids == ["2022-09-01", "2022-09-10", "2022-09-11"]


def test_slices_stay_raw(stream):
    _, out = stream
    first = replay.load_slice("2022-09-01", out)
    assert len(first) == 4                                   # the whole day, duplicates kept for recon
    assert first["from_bank"].tolist()[:1] == ["010"]        # leading zeros kept
    assert len(replay.load_slice("2022-09-11", out)) == 2    # whole tail day, nothing from 12 Sep


def test_slice_bounds():
    assert replay.slice_bounds("2022-09-08") == (pd.Timestamp("2022-09-08"), pd.Timestamp("2022-09-09"))
    assert replay.slice_bounds("2022-09-11") == (pd.Timestamp("2022-09-11"), pd.Timestamp("2022-09-12"))


def test_cursor_walks_the_slices_in_order(stream):
    _, out = stream
    assert [replay.next_slice(out) for _ in range(4)] == ["2022-09-01", "2022-09-10", "2022-09-11", None]
    replay.reset_cursor(out, start_after="2022-09-01")
    assert replay.next_slice(out) == "2022-09-10"
