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


def render_page2(stock, sectors, companies=None):
    if stock is None or stock.empty:
        return html.Div("No stock snapshot data available.", style={"color": "#5f7387"})

    selected = sectors if isinstance(sectors, list) else ([sectors] if sectors else [])
    company_filter = companies if isinstance(companies, list) else ([companies] if companies else [])

    scoped = stock.copy()
    if selected:
        scoped = scoped[scoped["sector"].isin(selected)]
    if company_filter:
        scoped = scoped[scoped["ticker"].isin(company_filter)]

    if scoped.empty:
        return html.Div("No records found for selected filters.", style={"color": "#5f7387"})

    ranked = scoped.sort_values("market_cap_cr", ascending=False).head(12).copy()
    ranked["short_name"] = ranked["company_name"].astype(str).str.slice(0, 14)
    fig1 = px.bar(
        ranked,
        x="short_name",
        y="market_cap_cr",
        color="sector",
        hover_data=["company_name", "pe_ratio", "profit_margin_pr"],
    )
    fig1.update_xaxes(title_text="")
    fig1.update_yaxes(title_text="Market Cap (Cr)")

    fundamentals = (
        scoped[["company_name", "ticker", "market_cap_cr", "pe_ratio", "roe_pr", "debt_to_equity", "profit_margin_pr"]]
        .copy()
        .sort_values("market_cap_cr", ascending=False)
    )
    fundamentals["short_name"] = fundamentals["company_name"].astype(str).str.slice(0, 14)

    fundamentals_main = fundamentals[["short_name", "company_name", "pe_ratio", "roe_pr"]].melt(
        id_vars=["short_name", "company_name"], value_vars=["pe_ratio", "roe_pr"], var_name="metric", value_name="value"
    )
    fundamentals_main = fundamentals_main.dropna(subset=["value"])
    fig2 = px.bar(
        fundamentals_main,
        x="short_name",
        y="value",
        color="metric",
        barmode="group",
        hover_data=["company_name"],
        color_discrete_map={"pe_ratio": "#4c6ef5", "roe_pr": "#f76707"},
    )
    fig2.update_xaxes(title_text="Company")
    fig2.update_yaxes(title_text="Value")

    fundamentals_quality = fundamentals[["short_name", "company_name", "debt_to_equity", "profit_margin_pr"]].melt(
        id_vars=["short_name", "company_name"],
        value_vars=["debt_to_equity", "profit_margin_pr"],
        var_name="metric",
        value_name="value",
    )
    fundamentals_quality = fundamentals_quality.dropna(subset=["value"])
    fig3 = px.bar(
        fundamentals_quality,
        x="short_name",
        y="value",
        color="metric",
        barmode="group",
        hover_data=["company_name"],
        color_discrete_map={"debt_to_equity": "#0ca678", "profit_margin_pr": "#e8590c"},
    )
    fig3.update_xaxes(title_text="Company")
    fig3.update_yaxes(title_text="Value")

    dist = (
        scoped.groupby("sector")[["pe_ratio", "roe_pr", "profit_margin_pr"]]
        .median()
        .reset_index()
        .melt(id_vars="sector", var_name="metric", value_name="value")
        .dropna()
    )
    fig4 = px.bar(
        dist,
        x="sector",
        y="value",
        color="metric",
        barmode="group",
        color_discrete_map={"pe_ratio": "#4c6ef5", "roe_pr": "#f76707", "profit_margin_pr": "#0ca678"},
    )
    fig4.update_xaxes(title_text="Sector")
    fig4.update_yaxes(title_text="Value")

    return html.Div(
        [
            dcc.Graph(figure=_layout(fig1, "Top Market Caps", 300), config={"displaylogo": False}),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "14px"},
                children=[
                    dcc.Graph(figure=_layout(fig2, "P/E and ROE by Company", 300), config={"displaylogo": False}),
                    dcc.Graph(figure=_layout(fig3, "Debt/Equity and Margin by Company", 300), config={"displaylogo": False}),
                ],
            ),
            dcc.Graph(figure=_layout(fig4, "Fundamental Distribution (Clustered by Sector)", 280), config={"displaylogo": False}),
        ]
    )
