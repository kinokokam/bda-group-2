"""The BQ2 look for the BQ1 dashboard (frontend/theme.py, theme.css, .streamlit/config.toml)."""

import re
import sys
import tomllib
from pathlib import Path

import pandas as pd

FRONTEND = Path(__file__).resolve().parents[1] / "frontend"
sys.path.insert(0, str(FRONTEND))

import theme  # noqa: E402
from theme import Kpi  # noqa: E402


def test_hero_escapes_every_text():
    html = theme.hero_html("<b>Overview</b>", "a & b", [Kpi("Value <USD>", "$1", "¤", pill="1 < 2", help='say "hi"')],
                           badge="Live <feed>")
    assert "<b>Overview" not in html and "&lt;b&gt;Overview&lt;/b&gt;" in html
    assert "a &amp; b" in html and "Value &lt;USD&gt;" in html and "1 &lt; 2" in html
    assert 'title="say &quot;hi&quot;"' in html and "Live &lt;feed&gt;" in html


def test_hero_draws_one_card_per_kpi_with_its_tone():
    html = theme.hero_html("Overview", "", [Kpi("A", "1", "⚑"), Kpi("B", "2", "!", pill="x", tone="amber"),
                                            Kpi("C", "3", "!", pill="y", tone="red")])
    assert html.count('class="bq1-kpi"') == 3
    assert 'class="bq1-pill bq1-pill--amber"' in html and 'class="bq1-pill bq1-pill--red"' in html


def test_each_value_carries_its_width_so_it_can_shrink_to_fit():
    assert theme.width_units("$841,583") == 7.4         # seven wide characters and a narrow comma
    assert theme.width_units("27.8%") == 4.4
    assert 'style="--units: 7.4"' in theme.kpi_html(Kpi("Amount", "$841,583", "¤"))


def test_optional_parts_are_left_out_when_empty():
    html = theme.hero_html("Overview", "", [Kpi("A", "1", "⚑")])
    assert "bq1-pill" not in html and "bq1-kpi__help" not in html and "bq1-hero__badge" not in html


def test_priority_pills_wrap_each_tier_and_leave_the_original():
    frame = pd.DataFrame({"priority": ["High", "Low", None], "score": [0.3, 0.08, None]})
    pills = theme.with_priority_pills(frame)
    assert pills["priority"].tolist() == [["High"], ["Low"], []]
    assert frame["priority"].iloc[:2].tolist() == ["High", "Low"]     # the caller's frame is untouched


def test_period_labels_are_short_enough_to_leave_the_chart_room():
    import metrics
    assert metrics.period_label("2022-09-10 11:01", by_hour=False) == "11:01"
    assert metrics.period_label("2022-09-10 11", by_hour=True) == "09-10 11h"


def test_theme_uses_the_bq2_palette():
    config = tomllib.loads((FRONTEND / ".streamlit" / "config.toml").read_text(encoding="utf-8"))["theme"]
    assert config["primaryColor"] == "#14574A"
    assert config["chartCategoricalColors"][0] == "#14574A"
    assert config["font"].startswith("Plus Jakarta Sans:")
    css = theme.STYLESHEET.read_text(encoding="utf-8")
    for value in ("#FAF6E9", "#14574A", "#124F43", "#EFE9D2"):
        assert value in css


def test_stylesheet_survives_streamlits_html_sanitiser():
    # DOMPurify drops a whole <style> whose text has "<" before a letter or "/"
    # (as in an unencoded SVG data URI), and the page silently loses its look.
    css = theme.STYLESHEET.read_text(encoding="utf-8")
    assert re.search(r"<[/\w]", css) is None
