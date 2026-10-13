#!/usr/bin/env python3
"""
Deterministic progress check for Qiaoye's lane of the AML BQ2 project.

Reads only files, git metadata and Python ASTs in the repo. It never opens .env or any
credential file and makes no network calls unless --aws is passed.

Usage (from anywhere inside the repo):
    python .claude/skills/project-progress/scripts/check_progress.py           # table
    python .claude/skills/project-progress/scripts/check_progress.py --json    # machine-readable
    python .claude/skills/project-progress/scripts/check_progress.py --aws     # + profile/S3 checks

Statuses: DONE, PARTIAL, TODO, WARN (a rule is at risk), SKIP (not checked).
Exit code is always 0; read the statuses.
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import subprocess
import sys
from pathlib import Path

DONE, PARTIAL, TODO, WARN, SKIP = "DONE", "PARTIAL", "TODO", "WARN", "SKIP"


def repo_root() -> Path:
    here = Path(__file__).resolve()
    try:
        out = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=here.parent,
                             capture_output=True, text=True, check=True).stdout.strip()
        return Path(out)
    except Exception:
        return here.parents[4]


ROOT = repo_root()


def git(*args: str) -> str:
    r = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


def text(rel: str) -> str:
    p = ROOT / rel
    return p.read_text(errors="replace") if p.is_file() else ""


def stubbed_functions(rel: str) -> dict[str, bool]:
    """function name -> True if its body still raises NotImplementedError."""
    src = text(rel)
    if not src:
        return {}
    out = {}
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.FunctionDef):
            out[node.name] = any(
                isinstance(n, ast.Raise) and "NotImplementedError" in ast.unparse(n)
                for n in ast.walk(node))
    return out


def csv_column_values(rel: str, col: str) -> set[str]:
    p = ROOT / rel
    if not p.is_file():
        return set()
    with p.open(newline="") as fh:
        return {row.get(col, "") for row in csv.DictReader(fh)}


def result(item, status, detail):
    return {"item": item, "status": status, "detail": detail}


# --------------------------------------------------------------------------- #
# Section 0: setup fixes from the 12 Oct handover
# --------------------------------------------------------------------------- #
def section0():
    r = []
    r.append(result("0.1 venv", DONE if (ROOT / ".venv/bin/python").exists() else TODO,
                    ".venv/bin/python exists" if (ROOT / ".venv").exists() else "no .venv"))

    agent = text("agent/triage_agent.py")
    parts = {"load_dotenv": "load_dotenv()" in agent,
             "ChatBedrockConverse": "ChatBedrockConverse" in agent,
             "old ChatBedrock removed": "ChatBedrock(" not in agent,
             "docstring python-dotenv": "python-dotenv" in agent}
    n = sum(parts.values())
    r.append(result("0.2 triage_agent Bedrock+dotenv",
                    DONE if n == len(parts) else (PARTIAL if n else TODO),
                    ", ".join(f"{k}={'y' if v else 'n'}" for k, v in parts.items())))

    ex = text(".env.example")
    want = ["AWS_REGION=ap-southeast-1",
            "BEDROCK_MODEL_ID=global.anthropic.claude-haiku-4-5-20251001-v1:0"]
    have = [w for w in want if w in ex.splitlines()]
    r.append(result("0.3 .env.example", DONE if len(have) == 2 else (PARTIAL if have else TODO),
                    f"{len(have)}/2 required lines"))

    env_exists = (ROOT / ".env").exists()          # existence only; never read
    ignored = git("check-ignore", ".env") == ".env"
    tracked = bool(git("ls-files", ".env"))
    if tracked:
        r.append(result("0.4 .env", WARN, ".env is TRACKED by git: untrack and rotate keys"))
    else:
        r.append(result("0.4 .env", DONE if env_exists and ignored else (WARN if not ignored else TODO),
                        f"exists={env_exists}, git-ignored={ignored} (contents not read)"))

    reqs = {l.strip().lower() for l in text("requirements.txt").splitlines() if l.strip()}
    need = {"pandas", "numpy", "pyarrow", "scikit-learn", "xgboost", "shap", "boto3",
            "langchain-core", "langchain-aws", "pydantic", "python-dotenv"}
    missing = sorted(need - {x.split("=")[0].split(">")[0].split("<")[0] for x in reqs})
    readme_ok = "-r requirements.txt" in text("README.md")
    st = DONE if reqs and not missing and readme_ok else (PARTIAL if reqs else TODO)
    r.append(result("0.5 requirements.txt", st,
                    f"missing={missing or 'none'}, README uses -r={readme_ok}"))

    r.append(result("0.6 AWS/Claude access", SKIP, "run with --aws for the profile check; "
                    "Bedrock converse is not run by this script"))

    msg = "Agent: Bedrock Converse + dotenv"
    r.append(result("0.7 setup commit", DONE if git("log", "--oneline", f"--grep={msg}") else TODO,
                    f"commit matching '{msg}'"))
    return r


# --------------------------------------------------------------------------- #
# P1–P9
# --------------------------------------------------------------------------- #
MODEL_FILES = ["metrics.csv", "detection_by_scheme.csv", "feature_importance.csv",
               "shap_summary.csv", "exposure_by_channel.csv", "exposure_by_currency.csv",
               "exposure_by_bank.csv", "model.json", "score_threshold.json"]


def notebook_state(rel: str):
    p = ROOT / rel
    if not p.is_file():
        return None
    nb = json.loads(p.read_text())
    cells = nb.get("cells", [])
    src = "\n".join("".join(c.get("source", [])) for c in cells)
    dirty = sum(1 for c in cells if c.get("cell_type") == "code"
                and (c.get("outputs") or c.get("execution_count") is not None))
    return {"cells": len(cells), "dirty_cells": dirty,
            "has_train_eval": "def train_eval" in src,
            "has_feature_assert": "assert" in src and "is_laundering" in src,
            "has_network_feats": all(f in src for f in
                                     ("fwd_within_24h", "returns_to_sender_72h", "sender_two_hop_out")),
            "has_3cycle": "in_3cycle_72h" in src}


def tasks():
    r = []
    out = ROOT / "out/model"

    # P1 notebook + outputs
    nb = notebook_state("notebooks/aml_xgboost.ipynb")
    present = [f for f in MODEL_FILES if (out / f).is_file()]
    exps_m = csv_column_values("out/model/metrics.csv", "experiment")
    exps_d = csv_column_values("out/model/detection_by_scheme.csv", "experiment")
    if nb is None:
        r.append(result("P1 notebook", TODO, "notebooks/aml_xgboost.ipynb missing"))
    else:
        bits = [f"{nb['cells']} cells",
                f"train_eval={'y' if nb['has_train_eval'] else 'n'}",
                f"feature-assert={'y' if nb['has_feature_assert'] else 'n'}",
                f"network-feats={'y' if nb['has_network_feats'] else 'n'}",
                f"3cycle(stretch)={'y' if nb['has_3cycle'] else 'n'}",
                f"outputs-cleared={'y' if nb['dirty_cells'] == 0 else 'n'}"]
        core = (nb["has_train_eval"] and nb["has_feature_assert"] and nb["has_network_feats"]
                and len(exps_m) >= 2 and len(exps_d) >= 2)
        r.append(result("P1 notebook", DONE if core else PARTIAL, ", ".join(bits)))
    r.append(result("P1 out/model files", DONE if len(present) == len(MODEL_FILES)
                    else (PARTIAL if present else TODO),
                    f"{len(present)}/{len(MODEL_FILES)}; experiments in metrics={sorted(exps_m) or '-'}, "
                    f"in detection={sorted(exps_d) or '-'}"))

    # P2 model + threshold
    thr_ok = False
    if (out / "score_threshold.json").is_file():
        try:
            t = json.loads((out / "score_threshold.json").read_text())
            thr_ok = {"alert_share", "threshold", "computed_on"} <= set(t)
        except ValueError:
            pass
    st = DONE if (out / "model.json").is_file() and thr_ok else (
        PARTIAL if (out / "model.json").is_file() or thr_ok else TODO)
    r.append(result("P2 model.json + threshold", st,
                    f"model.json={(out / 'model.json').is_file()}, threshold keys ok={thr_ok}"))

    dag = stubbed_functions("airflow/dags/aml_bq2_dag.py")

    def impl(fn):
        return fn in dag and not dag[fn]

    # P3 replay
    slices = list((ROOT / "out/stream").glob("slice=*")) if (ROOT / "out/stream").is_dir() else []
    st = DONE if impl("replay_slice") and slices else (PARTIAL if impl("replay_slice") or slices else TODO)
    r.append(result("P3 replay_slice", st,
                    f"implemented={impl('replay_slice')}, slice dirs={len(slices)}"))

    # P4 checks + tests
    tests_src = text("tests/test_demo_cases.py")
    n_tests = tests_src.count("\ndef test_") + tests_src.startswith("def test_")
    st = DONE if impl("run_checks") and n_tests >= 3 else (
        PARTIAL if impl("run_checks") or n_tests else TODO)
    r.append(result("P4 run_checks + tests", st,
                    f"implemented={impl('run_checks')}, tests in test_demo_cases.py={n_tests} (need 3); "
                    "not executed here"))

    # P5 agent
    agent = text("agent/triage_agent.py")
    parts = {"FakeLLM": "FakeLLM" in agent, "usage logged": "usage" in agent,
             "dag agent_triage": impl("agent_triage")}
    n = sum(parts.values())
    r.append(result("P5 triage agent", DONE if n == 3 else (PARTIAL if n else TODO),
                    ", ".join(f"{k}={'y' if v else 'n'}" for k, v in parts.items())))

    # P6 airflow docker
    compose = [p for p in ("airflow/docker-compose.yaml", "airflow/docker-compose.yml",
                           "docker-compose.yaml", "docker-compose.yml") if (ROOT / p).is_file()]
    empties = [t for t in ("load_slice", "quarantine", "build_marts", "xgboost_model")
               if f'EmptyOperator(task_id="{t}"' in text("airflow/dags/aml_bq2_dag.py")]
    r.append(result("P6 Airflow local", PARTIAL if compose else TODO,
                    f"compose={compose or 'none'}; EmptyOperator stubs left={empties or 'none'}; "
                    "DAG parse not run here"))

    # P7 monitor page
    mon_json = list((ROOT / "out/site/data/monitor").glob("*.json")) \
        if (ROOT / "out/site/data/monitor").is_dir() else []
    html = (ROOT / "site/monitor.html").is_file()
    r.append(result("P7 monitor page", DONE if html and mon_json else (PARTIAL if html or mon_json else TODO),
                    f"site/monitor.html={html}, monitor json files={len(mon_json)}"))

    # P8 alert
    log = (ROOT / "out/alerts.log").is_file()
    st = DONE if impl("send_alert") and log else (PARTIAL if impl("send_alert") or log else TODO)
    r.append(result("P8 send_alert", st, f"implemented={impl('send_alert')}, out/alerts.log={log}"))

    # P9 slides outline
    outlines = sorted(str(p.relative_to(ROOT)) for d in ("docs", "notebooks", "site")
                      if (ROOT / d).is_dir() for p in (ROOT / d).glob("*slide*.md"))
    r.append(result("P9 slides outline", DONE if outlines else TODO, ", ".join(outlines) or "no *slide*.md"))
    return r


# --------------------------------------------------------------------------- #
# Rules / repo hygiene
# --------------------------------------------------------------------------- #
def hygiene():
    r = []
    tracked = git("ls-files").splitlines()
    data = [f for f in tracked if (f.endswith((".csv", ".parquet", ".csv.gz", ".txt.gz"))
                                   and not f.startswith("reference/"))]
    r.append(result("No data in git", WARN if data else DONE, ", ".join(data[:5]) or "clean"))

    secrets = [f for f in tracked if Path(f).name in (".env", "kaggle.json")
               or f.endswith((".pem", ".key")) or "credentials" in f.lower()]
    r.append(result("No secrets tracked", WARN if secrets else DONE, ", ".join(secrets) or "clean"))

    status = git("status", "--porcelain").splitlines()
    r.append(result("Working tree", DONE if not status else PARTIAL,
                    "clean" if not status else f"{len(status)} changed/untracked paths"))

    branch = git("rev-parse", "--abbrev-ref", "HEAD")
    upstream = git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    ahead = git("rev-list", "--count", f"{upstream}..HEAD") if upstream else ""
    r.append(result("Branch", WARN if branch in ("main", "master") else DONE,
                    f"{branch}; upstream={upstream or 'none'}; unpushed commits="
                    f"{ahead if upstream else 'n/a (no upstream)'}"
                    + ("; work on a feature branch before pushing" if branch in ("main", "master") else "")))

    remote = git("remote", "get-url", "origin")
    r.append(result("Push remote", DONE if remote.startswith("https://gitlab.com/shopback")
                    or remote.startswith("git@gitlab.com:shopback") else WARN,
                    f"{remote or 'none'}"
                    + ("" if "gitlab.com" in remote and "shopback" in remote
                       else " — not a ShopBack GitLab repo, pushing is not permitted by org policy")))
    return r


def aws_checks():
    r = []
    try:
        p = subprocess.run(["aws", "sts", "get-caller-identity", "--profile", "aml-bq2",
                            "--output", "json"], capture_output=True, text=True, timeout=30)
        if p.returncode == 0:
            ident = json.loads(p.stdout)
            r.append(result("AWS profile aml-bq2", DONE,
                            f"account={ident.get('Account')}, arn={ident.get('Arn')}"))
        else:
            r.append(result("AWS profile aml-bq2", TODO, p.stderr.strip().splitlines()[-1][:160]))
    except FileNotFoundError:
        r.append(result("AWS profile aml-bq2", SKIP, "aws CLI not installed"))
    except subprocess.TimeoutExpired:
        r.append(result("AWS profile aml-bq2", SKIP, "timed out"))
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--aws", action="store_true", help="also check the aml-bq2 AWS profile")
    a = ap.parse_args()

    report = {"repo": str(ROOT), "head": git("log", "-1", "--format=%h %s"),
              "sections": {"Setup (section 0)": section0(), "Tasks P1-P9": tasks(),
                           "Rules": hygiene()}}
    if a.aws:
        report["sections"]["AWS"] = aws_checks()

    if a.json:
        json.dump(report, sys.stdout, indent=2)
        print()
        return

    print(f"Repo: {report['repo']}\nHEAD: {report['head']}\n")
    for name, rows in report["sections"].items():
        print(f"## {name}")
        w = max(len(x["item"]) for x in rows)
        for x in rows:
            print(f"  [{x['status']:<7}] {x['item']:<{w}}  {x['detail']}")
        print()
    counts = {}
    for rows in report["sections"].values():
        for x in rows:
            counts[x["status"]] = counts.get(x["status"], 0) + 1
    print("Totals: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))


if __name__ == "__main__":
    main()
