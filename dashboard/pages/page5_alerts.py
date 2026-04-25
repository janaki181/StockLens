import pandas as pd
import plotly.express as px
from dash import dcc, html


def _clean_revenue_signal(value) -> str:
    if pd.isna(value):
        return "No Data"
    cleaned = str(value).strip().upper()
    if cleaned in {"", "NAN", "NONE", "UNKNOWN"}:
        return "No Data"
    if cleaned == "GROWING":
        return "Growing"
    if cleaned == "DECLINING":
        return "Declining"
    if cleaned == "FLAT":
        return "Flat"
    return cleaned.title()


def render_page5(stock, price, vol_min: float):
    if stock is None or stock.empty:
        return html.Div("No stock snapshot data available.", style={"color": "#5f7387"})

    vol_min = float(vol_min) if vol_min is not None else 1.5

    alerts = stock.copy()
    alerts["volume_ratio"] = pd.to_numeric(alerts.get("volume_ratio"), errors="coerce")
    alerts["rsi_14"] = pd.to_numeric(alerts.get("rsi_14"), errors="coerce")

    # Try to enrich RSI from latest price_history row when not in stock_data.
    if "rsi_14" not in stock.columns and price is not None and not price.empty:
        latest_rsi = (
            price.sort_values("date")
            .groupby("ticker", as_index=False)
            .tail(1)[["ticker", "rsi_14"]]
            .rename(columns={"rsi_14": "rsi_latest"})
        )
        alerts = alerts.merge(latest_rsi, on="ticker", how="left")
        alerts["rsi_14"] = alerts["rsi_14"].fillna(alerts["rsi_latest"])

    filtered = alerts[alerts["volume_ratio"] >= vol_min].copy()

    if filtered.empty:
        return html.Div(
            f"No alert candidates at or above volume ratio {vol_min:.1f}x.",
            style={"color": "#5f7387"},
        )

    filtered["alert_score"] = (
        filtered["volume_ratio"].fillna(0) * 0.7
        + (filtered["qoq_revenue_growth_pr"].fillna(0) / 10.0) * 0.2
        + ((filtered["profit_margin_pr"].fillna(0)).clip(lower=-50, upper=50) / 50.0) * 0.1
    )
    filtered["revenue_label"] = filtered["revenue_signal"].apply(_clean_revenue_signal)

    top = filtered.sort_values("alert_score", ascending=False).head(15)

    fig = px.bar(
        top.sort_values("alert_score", ascending=True),
        x="alert_score",
        y="ticker",
        orientation="h",
        color="volume_ratio",
        color_continuous_scale="YlOrRd",
        hover_data=["company_name", "revenue_label", "rsi_14"],
    )
    fig.update_layout(
        template="plotly_white",
        title="Top Alert Score Candidates",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        height=310,
        margin={"l": 40, "r": 20, "t": 50, "b": 35},
    )
    fig.update_xaxes(title_text="Alert Score", showgrid=True, gridcolor="#edf2f7")
    fig.update_yaxes(title_text="Ticker", showgrid=True, gridcolor="#edf2f7")

    table = _alerts_table(top.head(12))
    return html.Div([dcc.Graph(figure=fig, config={"displaylogo": False}), table])


def _alerts_table(df):
    header = html.Tr(
        [
            html.Th("Ticker", style=_th()),
            html.Th("Company", style=_th()),
            html.Th("Volume Ratio", style=_th()),
            html.Th("RSI", style=_th()),
            html.Th("Revenue", style=_th()),
            html.Th("Score", style=_th()),
        ]
    )

    rows = []
    for _, row in df.iterrows():
        rsi_value = row.get("rsi_14")
        if pd.notna(rsi_value):
            if rsi_value >= 70:
                rsi_color = "#e76f51"
            elif rsi_value <= 30:
                rsi_color = "#198754"
            else:
                rsi_color = "#1f2d3d"
            rsi_text = f"{rsi_value:.1f}"
        else:
            rsi_color = "#888"
            rsi_text = "NA"

        rows.append(
            html.Tr(
                [
                    html.Td(row["ticker"], style=_td("#1f2d3d")),
                    html.Td(str(row.get("company_name", ""))[:26], style=_td("#1f2d3d")),
                    html.Td(f"{row.get('volume_ratio', 0):.2f}x", style=_td("#1f2d3d")),
                    html.Td(rsi_text, style=_td(rsi_color)),
                    html.Td(str(row.get("revenue_label", "No Data")), style=_td("#5f7387")),
                    html.Td(f"{row['alert_score']:.2f}", style=_td("#ff9800")),
                ],
                style={"borderBottom": "1px solid #edf2f7"},
            )
        )

    return html.Table(
        [html.Thead(header), html.Tbody(rows)],
        style={
            "width": "100%",
            "borderCollapse": "collapse",
            "background": "#ffffff",
            "border": "1px solid #e2e8f0",
            "borderRadius": "8px",
        },
    )


def _th():
    return {
        "padding": "8px 12px",
        "color": "#4f6275",
        "fontSize": "12px",
        "borderBottom": "1px solid #dde5ef",
        "textAlign": "left",
        "background": "#f5f8fc",
    }


def _td(color: str):
    return {"padding": "6px 12px", "fontSize": "12px", "color": color}
