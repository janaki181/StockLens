import pandas as pd
import plotly.express as px
from dash import dcc, html


def _layout(fig, title, height=300):
    fig.update_layout(
        template="plotly_white",
        title=title,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        height=height,
        margin={"l": 40, "r": 20, "t": 50, "b": 35},
        legend={"orientation": "h", "y": -0.2},
    )
    fig.update_xaxes(showgrid=True, gridcolor="#edf2f7")
    fig.update_yaxes(showgrid=True, gridcolor="#edf2f7")
    return fig


def render_page4(price, tickers):
    if price is None or price.empty:
        return html.Div("No price history data available.", style={"color": "#5f7387"})

    selected = tickers if isinstance(tickers, list) else ([tickers] if tickers else [])
    if not selected:
        return html.Div("Select at least one company.", style={"color": "#5f7387"})

    selected = selected[:6]
    df = price[price["ticker"].isin(selected)].copy()
    if df.empty:
        return html.Div("No history found for selected companies.", style={"color": "#5f7387"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values(["ticker", "date"])

    fig1 = px.line(df, x="date", y="vs_nifty_pct", color="ticker")
    fig1.add_hline(y=0, line_dash="dash", line_color="#d9534f")
    fig1.update_yaxes(title_text="Daily Alpha %")

    fig2 = px.line(df, x="date", y="vs_nifty_cumulative", color="ticker")
    fig2.add_hline(y=0, line_dash="dash", line_color="#d9534f")
    fig2.update_yaxes(title_text="Cumulative Alpha")

    base = df[["date", "ticker", "close", "nifty_index_close"]].dropna().copy()
    indexed_parts = []
    for ticker in selected:
        one = base[base["ticker"] == ticker].copy().sort_values("date")
        if one.empty:
            continue
        one["index_value"] = one["close"] / one["close"].iloc[0] * 100
        indexed_parts.append(one[["date", "ticker", "index_value"]])

    nifty_curve = None
    if not base.empty:
        nifty_curve = base[["date", "nifty_index_close"]].drop_duplicates("date").sort_values("date")
        nifty_curve["index_value"] = nifty_curve["nifty_index_close"] / nifty_curve["nifty_index_close"].iloc[0] * 100
        nifty_curve["ticker"] = "NIFTY"
        indexed_parts.append(nifty_curve[["date", "ticker", "index_value"]])

    indexed = pd.concat(indexed_parts, ignore_index=True) if indexed_parts else pd.DataFrame()
    fig3 = px.line(indexed, x="date", y="index_value", color="ticker")
    fig3.update_yaxes(title_text="Indexed Value (Base=100)")

    return html.Div(
        [
            dcc.Graph(figure=_layout(fig1, "Daily Relative Return vs NIFTY", 290), config={"displaylogo": False}),
            dcc.Graph(figure=_layout(fig2, "Cumulative Outperformance vs NIFTY", 290), config={"displaylogo": False}),
            dcc.Graph(figure=_layout(fig3, "Indexed Performance Comparison", 290), config={"displaylogo": False}),
        ]
    )
