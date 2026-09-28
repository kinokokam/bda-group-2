"""
download_aml.py -- downloads the two HI-Small files from Kaggle into ~/aml_data.

How to run (macOS Terminal):
    1. Save this file to your Downloads folder.
    2. In Terminal type exactly:   python3 ~/Downloads/download_aml.py
    3. Press Enter and wait. It prints "done" when finished.

Needs a Kaggle account. The first time, it will ask for your Kaggle username
and API key (kaggle.com -> your avatar -> Settings -> API -> Create New Token).
"""
import os
import shutil
import subprocess
import sys

DATASET = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"
FILES = ["HI-Small_Trans.csv", "HI-Small_Patterns.txt"]
OUT = os.path.expanduser("~/aml_data")


def ensure_kagglehub():
    try:
        import kagglehub  # noqa: F401
    except ImportError:
        print("Installing kagglehub ...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "kagglehub"])


def main():
    ensure_kagglehub()
    import kagglehub

    os.makedirs(OUT, exist_ok=True)
    for name in FILES:
        print(f"Downloading {name} ...")
        try:
            path = kagglehub.dataset_download(DATASET, path=name)
        except Exception as exc:  # usually: not logged in
            print(f"Download failed ({exc}). Logging in to Kaggle, then retrying.")
            kagglehub.login()
            path = kagglehub.dataset_download(DATASET, path=name)
        shutil.copy(path, os.path.join(OUT, name))
        size_mb = os.path.getsize(os.path.join(OUT, name)) / 1e6
        print(f"  saved -> {os.path.join(OUT, name)}  ({size_mb:,.0f} MB)")
    print(f"\ndone -> {OUT}")
    print("Now add the folder above to the Claude session with 'Add folder'.")


if __name__ == "__main__":
    main()
