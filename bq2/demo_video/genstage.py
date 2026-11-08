"""Build the film's stage pages from REAL repo files and REAL captured command output.
Code pages show only the chosen line ranges (so nothing secret-like reaches the frame)."""
import html, pathlib, re, subprocess
from pygments import highlight
from pygments.lexers import get_lexer_for_filename, TextLexer
from pygments.formatters import HtmlFormatter

REPO = pathlib.Path("/Users/qiaoye/Documents/GitHub/bda-group-2")
FILM = pathlib.Path(__file__).parent
OUT, CAP = FILM / "stage", FILM / "captures"
OUT.mkdir(exist_ok=True)

CSS = """
:root{--g:#14574a;--g2:#2f7d6d;--mint:#dcebe5;--cream:#faf6e9;--ink:#10231f}
*{box-sizing:border-box}html,body{margin:0;height:100%}
body{background:var(--cream);font-family:'DejaVu Sans',sans-serif;color:var(--ink)}
.win{position:absolute;inset:28px 28px 92px 28px;background:#fff;border-radius:18px;
 box-shadow:0 10px 40px rgba(20,87,74,.14);border:1px solid #e3ece8;overflow:hidden;display:flex;flex-direction:column}
.bar{height:46px;flex:none;display:flex;align-items:center;gap:10px;padding:0 18px;background:var(--mint);
 font:600 15px 'DejaVu Sans Mono',monospace;color:var(--g)}
.dot{width:11px;height:11px;border-radius:50%;background:#b9d4ca}
.body{flex:1;overflow:hidden;position:relative}
.scroll{position:absolute;inset:0;overflow:auto;padding:14px 0}
pre{margin:0;font:15px/1.55 'DejaVu Sans Mono',monospace}
.ln{display:inline-block;width:48px;color:#9bb3ab;text-align:right;padding-right:14px;user-select:none}
.row{padding:0 18px;white-space:pre}
.wrap .row{white-space:pre-wrap;word-break:break-word}
.row.hl{background:#e6f3ee;box-shadow:inset 4px 0 0 var(--g2)}
.gap{color:#9bb3ab;padding:4px 18px 4px 80px;font:italic 14px 'DejaVu Sans',sans-serif}
.term{background:#0f1f1b}.term .bar{background:#173029;color:#bfe3d6}.term .dot{background:#2e5248}
.term pre{color:#d9efe7}.prompt{color:#7fd1b4}.cmd{color:#fff;font-weight:600}
.card{position:absolute;inset:0;background:var(--g);color:#fff;display:flex;flex-direction:column;
 justify-content:center;padding:0 110px}
.card .kick{font:600 20px 'DejaVu Sans',sans-serif;letter-spacing:.14em;text-transform:uppercase;color:#a9d8c8}
.card h1{font:700 54px/1.15 'DejaVu Sans',sans-serif;margin:18px 0 10px}
.card p{font:24px/1.5 'DejaVu Sans',sans-serif;color:#dcefe8;max-width:1000px;margin:0}
.dots{display:flex;gap:10px;margin-top:40px}.dots i{width:12px;height:12px;border-radius:50%;background:#3d7a6c}
.dots i.on{background:#f6c64b}
""" + HtmlFormatter(style="friendly").get_style_defs(".hl-code") + ".hl-code{background:none!important}"

def page(name, body, title="BQ2"):
    (OUT / f"{name}.html").write_text(
        f"<!doctype html><html><head><meta charset=utf-8><title>{html.escape(title)}</title>"
        f"<style>{CSS}</style></head><body>{body}</body></html>")

def code(name, rel, ranges, hl=(), wrap=False):
    """ranges: list of (start,end) 1-based inclusive line ranges to show; hl: lines to highlight."""
    path = REPO / rel
    src = path.read_text().splitlines()
    try: lexer = get_lexer_for_filename(path.name)
    except Exception: lexer = TextLexer()
    rows, last = [], None
    for a, b in ranges:
        if last is not None and a > last + 1:
            rows.append(f'<div class="gap">… lines {last+1}–{a-1} not shown …</div>')
        for i in range(a, min(b, len(src)) + 1):
            h = highlight(src[i-1] or " ", lexer, HtmlFormatter(nowrap=True))
            rows.append(f'<div class="row{" hl" if i in hl else ""}" data-line="{i}"><span class="ln">{i}</span>'
                        f'<span class="hl-code">{h.rstrip()}</span></div>')
        last = b
    page(name, f'<div class="win{" wrap" if wrap else ""}"><div class="bar"><span class="dot"></span><span class="dot"></span>'
               f'<span class="dot"></span>&nbsp;{html.escape(rel)}</div><div class="body"><div class="scroll">'
               f'<pre>{"".join(rows)}</pre></div></div></div>', rel)

def term(name, blocks, title="Terminal"):
    """blocks: list of (command, output_text)."""
    parts = []
    for cmd, out in blocks:
        parts.append(f'<div class="row"><span class="prompt">bda-group-2 $ </span><span class="cmd">{html.escape(cmd)}</span></div>')
        parts += [f'<div class="row">{html.escape(l)}</div>' for l in out.rstrip("\n").splitlines()]
        parts.append('<div class="row"> </div>')
    page(name, f'<div class="win term"><div class="bar"><span class="dot"></span><span class="dot"></span>'
               f'<span class="dot"></span>&nbsp;{html.escape(title)}</div><div class="body"><div class="scroll">'
               f'<pre>{"".join(parts)}</pre></div></div></div>', title)

def card(name, kick, head, sub, step=None, total=None):
    dots = "" if step is None else '<div class="dots">' + "".join(
        f'<i class="{"on" if i == step else ""}"></i>' for i in range(1, total + 1)) + "</div>"
    page(name, f'<div class="card"><div class="kick">{html.escape(kick)}</div><h1>{html.escape(head)}</h1>'
               f'<p>{html.escape(sub)}</p>{dots}</div>', head)

def redact(text):
    """Belt and braces: captured CLI output must never carry an account id, ARN, key or token into a frame."""
    text = re.sub(r"\b\d{12}\b", "************", text)
    text = re.sub(r"arn:aws[^\s"']*", "arn:aws:…redacted…", text)
    text = re.sub(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b", "AKIA…redacted…", text)
    text = re.sub(r"(?i)(secret|token|password|key)(\s*[:=]\s*)\S+", r"\1\2…redacted…", text)
    return text

cap = lambda f: redact((CAP / f).read_text())

def tree():
    files = subprocess.run(["git", "ls-files", "bq2"], cwd=REPO, capture_output=True, text=True).stdout.split()
    keep = ["airflow/dags", "lambdas/parse_patterns", "sql", "sql/marts", "pipeline", "monitoring", "scripts", "notebooks"]
    lines = ["bq2/"]
    for d in keep:
        kids = sorted({f.split("/")[len(d.split("/")) + 1] for f in files
                       if f.startswith(f"bq2/{d}/") and f.count("/") == len(d.split("/")) + 1 and f.endswith((".py", ".sql", ".ipynb"))})
        lines.append(f"├── {d}/")
        lines += [f"│   ├── {k}" for k in kids[:7]] + ([f"│   └── … {len(kids)-7} more"] if len(kids) > 7 else [])
    return "\n".join(lines)

# ── chapter cards ──
card("card_open", "BQ2 · Laundering exposure", "A daily monitor for AML transfer data",
     "Part 1: set up the pipeline once.  Part 2: run it end to end.")
card("card_part2", "Part 2", "Run the pipeline end to end",
     "Batch DAG → model → daily monitor DAG → dashboards and chat.")
card("card_recap", "Recap", "Set up once. Run with two DAGs.",
     "Setup: bucket, Athena tables, Lambda, Airflow, Helical.   Run: aml_bq2_batch builds the tables; "
     "aml_bq2_demo checks each day, quarantines bad ones and refreshes the dashboards.")

# ── setup ──
term("s2_tree", [("tree bq2   # source files only", tree())], "Repository layout")
term("s3_uv", [("uv sync", "Resolved and installed the project's Python packages:\n" + cap("uv_sync.txt"))], "Install")
code("s4_compose_env", "docker-compose.yml", [(3, 9), (16, 19)], hl={6, 7, 19})
code("s4_envexample", ".env.example", [(1, 40)])
code("s4_dag_bucket", "bq2/airflow/dags/aml_bq2_batch.py", [(1, 24)], hl={21})
term("s4_s3", [("aws s3 ls s3://aml-bq2/ --profile aml-bq2", cap("s3_root.txt"))], "AWS · profile aml-bq2")
code("s5_download", "bq2/scripts/download_aml.py", [(1, 40)])
term("s5_raw", [("aws s3 ls s3://aml-bq2/raw/ --recursive --human-readable", cap("s3_raw.txt"))], "AWS · raw layer")
code("s6_ddl", "bq2/sql/01_tables.sql", [(1, 60)], hl=set(range(28, 47)))
code("s7_lambda", "bq2/lambdas/parse_patterns/handler.py", [(1, 75)], hl={27, 28, 29})
term("s7_deployed", [("aws lambda get-function-configuration --function-name parse_patterns", cap("lambda.txt"))], "AWS · Lambda")
term("s8_out", [("ls bq2/out/", cap("out_ls.txt")), ("ls bq2/out/stream/", cap("out_stream.txt")),
                ("python -m monitoring.replay build        # cut the 11 daily slices", "(run once during setup)"),
                ("python scripts/train_live_model.py        # live model, threshold 0.953098", "(run once during setup)"),
                ("aws s3 cp s3://aml-bq2/reference/fx/fx_rates.csv bq2/out/fx_rates.csv", "(run once during setup)")],
     "Monitor inputs")
code("s9_mounts", "docker-compose.yml", [(20, 33)], hl={23, 28, 29, 30, 31, 32})
term("s9_up", [("docker compose up -d", "# starts Postgres, the scheduler and the webserver"),
               ("docker compose ps", cap("compose_ps.txt"))], "Airflow stack")
code("s10_override", "helical/docker-compose.override.yml", [(1, 30)], hl={8})
code("s11_bedrock", "helical/instantbi-bedrock/apply.py", [(1, 22), (88, 96)], hl={15, 92, 93})

# ── run ──
code("r1_batch_code", "bq2/airflow/dags/aml_bq2_batch.py", [(44, 58)], hl={58})
term("r2_marts", [("aws s3 ls s3://aml-bq2/marts/", cap("s3_marts.txt"))], "AWS · marts")
code("r3_notebook_note", "bq2/notebooks/README.md", [(1, 40)])
term("r3_metrics", [("aws s3 cp s3://aml-bq2/marts/model/metrics.csv -", cap("metrics.txt"))], "XGBoost results")
code("r4_demo_code", "bq2/airflow/dags/aml_bq2_demo.py", [(91, 112)], hl={112})
code("r6_report", "bq2/out/monitor/reports/2022-09-01.json", [(1, 40)], hl={5, 6, 7, 8, 9})
code("r6_escalations", "bq2/out/escalations.log", [(1, 3)], wrap=True)
print("pages:", sorted(p.name for p in OUT.glob("*.html")))
