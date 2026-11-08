"""The BQ2 Helical dashboards' look, for the Streamlit views.

Colours, font and radii are set in `.streamlit/config.toml`; `theme.css` adds
the green header band with its KPI cards and the white card frames. Each view
opens with `hero` and puts its sections in `card`s.
"""

from dataclasses import dataclass
from html import escape
from pathlib import Path

import pandas as pd
import streamlit as st

from metrics import PRIORITIES

STYLESHEET = Path(__file__).with_name("theme.css")

EYEBROW = "AML alert triage"

PILL_TONES = {"green": "", "amber": " bq1-pill--amber", "red": " bq1-pill--red"}

# Theme colour names (config.toml): High red, Medium amber, Low grey, like
# the BQ2 fail, quarantined and neutral pills.
PRIORITY_PILL = st.column_config.MultiselectColumn(
    "Priority",
    options=PRIORITIES,
    color=["red", "orange", "gray"],
    help="High: 20% or more estimated likelihood of laundering · Medium: 10–20% · Low: below that.",
)


@dataclass(frozen=True)
class Kpi:
    """One header card: an icon, a value, an optional pill and a label."""

    label: str
    value: str
    icon: str
    pill: str = ""
    tone: str = "green"
    help: str = ""


def apply() -> None:
    """Load the stylesheet; it takes no space on the page."""
    st.html(STYLESHEET)


def width_units(text: str) -> float:
    """Estimate a value's width in digit widths: separators are narrow.

    `theme.css` divides the card's width by this to size the value, so a long
    figure such as `$841,583` shrinks to fit a narrow card instead of spilling.
    """
    return round(sum(0.4 if character in ",.:'" else 1.0 for character in text), 2)


def kpi_html(kpi: Kpi) -> str:
    """Render one header card, escaping every text."""
    help_mark = (
        f'<span class="bq1-kpi__help" title="{escape(kpi.help)}">?</span>' if kpi.help else ""
    )
    pill = (
        f'<span class="bq1-pill{PILL_TONES.get(kpi.tone, "")}">{escape(kpi.pill)}</span>' if kpi.pill else ""
    )
    return (
        '<div class="bq1-kpi">'
        f'<div class="bq1-kpi__head"><span class="bq1-kpi__icon">{escape(kpi.icon)}</span>{help_mark}</div>'
        f'<div class="bq1-kpi__row"><span class="bq1-kpi__value" style="--units: {width_units(kpi.value)}">'
        f"{escape(kpi.value)}</span>{pill}</div>"
        f'<div class="bq1-kpi__label">{escape(kpi.label)}</div>'
        "</div>"
    )


def hero_html(title: str, subtitle: str, kpis: list[Kpi], badge: str = "", eyebrow: str = EYEBROW) -> str:
    """Render the green header band: eyebrow, title, subtitle, a badge and the KPI cards.

    Parameters
    ----------
    title : str
        Page name.
    subtitle : str
        One line under the title, such as when the data was refreshed.
    kpis : list[Kpi]
        Cards along the band's lower edge, four or five fit a row.
    badge : str, optional
        White pill on the right, such as the data window.
    eyebrow : str, optional
        Small line above the title.

    Returns
    -------
    str
        The header as HTML.
    """
    badge_html = f'<span class="bq1-hero__badge">{escape(badge)}</span>' if badge else ""
    cards = "".join(kpi_html(kpi) for kpi in kpis)
    return (
        '<div class="bq1-hero">'
        '<div class="bq1-hero__top"><div>'
        f'<div class="bq1-hero__eyebrow">{escape(eyebrow)}</div>'
        f'<div class="bq1-hero__title">{escape(title)}</div>'
        f'<div class="bq1-hero__subtitle">{escape(subtitle)}</div>'
        f"</div>{badge_html}</div>"
        f'<div class="bq1-kpis">{cards}</div>'
        "</div>"
    )


def hero(title: str, subtitle: str, kpis: list[Kpi], badge: str = "") -> None:
    """Draw the header band."""
    st.html(hero_html(title, subtitle, kpis, badge))


def card(name: str):
    """Return a white card to draw a section in, as `with theme.card("name"):`."""
    return st.container(key=f"card-{name}")


def window_badge(run_id: str) -> str:
    """Name the selected data window for the header badge, as the sidebar does."""
    return st.session_state.get("window_labels", {}).get(run_id, run_id)


def with_priority_pills(frame: pd.DataFrame, column: str = "priority") -> pd.DataFrame:
    """Turn a priority column into one-item lists, which `PRIORITY_PILL` draws as a coloured pill."""
    frame = frame.copy()
    frame[column] = [[value] if isinstance(value, str) and value else [] for value in frame[column]]
    return frame
