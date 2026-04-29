import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, dcc, html
from plotly.subplots import make_subplots

SECTOR_COLORS = {
    "Technology": "#6f42c1",
    "Financial Services": "#1f77b4",
    "Consumer Cyclical": "#ff9800",
    "Consumer Defensive": "#2ca58d",
    "Healthcare": "#198754",
    "Basic Materials": "#8d99ae",
    "Industrials": "#00a896",
    "Energy": "#e76f51",
    "Utilities": "#d63384",
    "Communication Services": "#7f95d1",
}

SIGNAL_COLORS = {
    "Growing": "#198754",
    "Declining": "#e76f51",
    "Flat": "#8d99ae",
    "No Data": "#6c757d",
}


def _clean_signal(value):
    if pd.isna(value):
        return "No Data"

    cleaned = str(value).strip()
    if not cleaned:
        return "No Data"

    mapping = {
        "GROWING": "Growing",
        "DECLINING": "Declining",
        "FLAT": "Flat",
        "UNKNOWN": "No Data",
        "NO DATA": "No Data",
        "NAN": "No Data",
        "NONE": "No Data",
    }
    return mapping.get(cleaned.upper(), cleaned)


def _light_layout(fig, title, height=300):
    fig.update_layout(
        template="plotly_white",
        title=title,
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        height=height,
        margin={"l": 40, "r": 20, "t": 50, "b": 40},
        legend={"orientation": "h", "y": -0.2},
    )
    fig.update_xaxes(showgrid=True, gridcolor="#edf2f7")
    fig.update_yaxes(showgrid=True, gridcolor="#edf2f7")
    return fig


def _build_bubble_chart(stock):
    agg = (
        stock.groupby("sector")
        .agg(
            median_pe=("pe_ratio", "median"),
            avg_margin=("profit_margin_pr", "mean"),
            total_cap=("market_cap_cr", "sum"),
        )
        .reset_index()
        .dropna(subset=["median_pe", "avg_margin", "total_cap"])
    )

    fig = px.scatter(
        agg,
        x="median_pe",
        y="avg_margin",
        size="total_cap",
        color="sector",
        color_discrete_map=SECTOR_COLORS,
        text="sector",
        hover_data={"median_pe": ":.2f", "avg_margin": ":.2f", "total_cap": ":,.0f"},
    )
    fig.update_traces(textposition="top center")
    fig.update_xaxes(title_text="Median P/E (median avoids outliers)")
    fig.update_yaxes(title_text="Average Profit Margin %")
    return _light_layout(fig, "Sector Bubble View: Valuation vs Profitability vs Size", height=320)


def _build_pe_margin_bars(stock):
    pe = stock.groupby("sector")["pe_ratio"].median().sort_values().dropna().reset_index()
    pe.columns = ["sector", "value"]
    margin = stock.groupby("sector")["profit_margin_pr"].mean().sort_values().dropna().reset_index()
    margin.columns = ["sector", "value"]

    fig = make_subplots(rows=1, cols=2, subplot_titles=("Median P/E by Sector", "Avg Margin by Sector"))
    fig.add_trace(
        go.Bar(
            x=pe["value"],
            y=pe["sector"],
            orientation="h",
            marker_color=[SECTOR_COLORS.get(s, "#9aa0a6") for s in pe["sector"]],
            name="Median P/E",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=margin["value"],
            y=margin["sector"],
            orientation="h",
            marker_color=[SECTOR_COLORS.get(s, "#9aa0a6") for s in margin["sector"]],
            name="Avg Margin",
        ),
        row=1,
        col=2,
    )

    pe_mid = pd.to_numeric(stock["pe_ratio"], errors="coerce").median()
    margin_avg = pd.to_numeric(stock["profit_margin_pr"], errors="coerce").mean()
    if pd.notna(pe_mid):
        fig.add_vline(x=float(pe_mid), line_dash="dash", line_color="#d9534f", row=1, col=1)
    if pd.notna(margin_avg):
        fig.add_vline(x=float(margin_avg), line_dash="dash", line_color="#d9534f", row=1, col=2)

    fig.update_annotations(font_size=12)
    fig.update_layout(showlegend=False)
    return _light_layout(fig, "Sector Bars (Median PE and Average Margin)", height=320)


def _build_signal_donut(stock):
    frame = stock.copy()
    frame["signal_label"] = frame["revenue_signal"].apply(_clean_signal)

    company_col = "company_name" if "company_name" in frame.columns else "ticker"
    sig = (
        frame.groupby("signal_label", as_index=False)
        .agg(
            count=("signal_label", "size"),
            companies=(
                company_col,
                lambda s: ", ".join(sorted(pd.Series(s).dropna().astype(str).unique())[:12]),
            ),
        )
        .rename(columns={"signal_label": "signal"})
    )

    order = ["Growing", "Declining", "Flat", "No Data"]
    sig["order"] = sig["signal"].apply(lambda value: order.index(value) if value in order else len(order))
    sig = sig.sort_values("order").drop(columns=["order"])

    growing_count = int(sig.loc[sig["signal"] == "Growing", "count"].sum())
    declining_count = int(sig.loc[sig["signal"] == "Declining", "count"].sum())

    fig = px.pie(
        sig,
        names="signal",
        values="count",
        hole=0.55,
        color="signal",
        color_discrete_map=SIGNAL_COLORS,
        custom_data=["companies", "count"],
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "Companies: %{customdata[1]}<br>"
            "%{customdata[0]}"
            "<extra></extra>"
        ),
    )
    fig.add_annotation(
        text=(
            f"{len(frame)}<br>Companies"
            f"<br><span style='font-size:11px'>G: {growing_count} | D: {declining_count}</span>"
        ),
        showarrow=False,
        font={"size": 12},
    )
    return _light_layout(fig, "Revenue Signal Distribution", height=320)


def _company_table(df, sector_filter=None, company_filter=None):
    frame = df.copy()
    frame["signal_label"] = frame["revenue_signal"].apply(_clean_signal)

    if sector_filter:
        frame = frame[frame["sector"].isin(sector_filter)]
    if company_filter:
        frame = frame[frame["ticker"].isin(company_filter)]

    frame = frame.sort_values("market_cap_cr", ascending=False)
    if frame.empty:
        return html.Div("No companies match selected filters.", style={"color": "#7a8793", "padding": "8px"})

    header_style = {
        "padding": "8px 10px",
        "color": "#415466",
        "fontSize": "12px",
        "borderBottom": "1px solid #dee5ee",
        "textAlign": "left",
        "fontWeight": "600",
        "background": "#f5f8fc",
    }

    rows = []
    for _, row in frame.iterrows():
        vol_value = pd.to_numeric(row.get("volume_ratio"), errors="coerce")
        close_value = pd.to_numeric(row.get("close"), errors="coerce")
        cap_value = pd.to_numeric(row.get("market_cap_cr"), errors="coerce")
        rows.append(
            html.Tr(
                [
                    html.Td(str(row.get("company_name", ""))[:30], style={"padding": "6px 10px", "fontSize": "12px"}),
                    html.Td(row.get("sector", ""), style={"padding": "6px 10px", "fontSize": "12px", "color": "#5f7387"}),
                    html.Td(f"Rs {close_value:,.2f}" if pd.notna(close_value) else "NA", style={"padding": "6px 10px", "fontSize": "12px"}),
                    html.Td(f"Rs {cap_value:,.0f}" if pd.notna(cap_value) else "NA", style={"padding": "6px 10px", "fontSize": "12px"}),
                    html.Td(
                        f"{vol_value:.2f}x" if pd.notna(vol_value) else "NA",
                        style={
                            "padding": "6px 10px",
                            "fontSize": "12px",
                            "color": "#d9534f" if pd.notna(vol_value) and float(vol_value) > 1.5 else "#1f2d3d",
                        },
                    ),
                    html.Td(
                        row["signal_label"],
                        style={
                            "padding": "6px 10px",
                            "fontSize": "12px",
                            "color": SIGNAL_COLORS.get(row["signal_label"], "#5f7387"),
                            "fontWeight": "600",
                        },
                    ),
                ]
            )
        )

    return html.Table(
        [
            html.Thead(
                html.Tr(
                    [
                        html.Th("Company", style=header_style),
                        html.Th("Sector", style=header_style),
                        html.Th("Close", style=header_style),
                        html.Th("Mkt Cap (Cr)", style=header_style),
                        html.Th("Vol Ratio", style=header_style),
                        html.Th("Revenue", style=header_style),
                    ]
                )
            ),
            html.Tbody(rows),
        ],
        style={"width": "100%", "borderCollapse": "collapse", "background": "#ffffff", "border": "1px solid #e2e8f0", "borderRadius": "8px"},
    )


def _dropdown(component_id, options, value, label):
    return html.Div(
        style={"display": "flex", "alignItems": "center", "gap": "8px"},
        children=[
            html.Label(label, style={"color": "#4f6275", "fontSize": "12px", "whiteSpace": "nowrap"}),
            dcc.Dropdown(
                id=component_id,
                options=options,
                value=value,
                multi=True,
                clearable=True,
                style={"width": "260px", "fontSize": "12px", "background": "#ffffff"},
            ),
        ],
    )


def render_page1(stock):
    if stock is None or stock.empty:
        return html.Div("No stock snapshot data available.", style={"color": "#5f7387"})

    sectors = sorted(stock["sector"].dropna().unique().tolist())
    companies = sorted(stock["ticker"].dropna().unique().tolist())

    return html.Div(
        [
            dcc.Graph(figure=_build_bubble_chart(stock), config={"displaylogo": False}, style={"height": "330px"}),
            dcc.Graph(figure=_build_pe_margin_bars(stock), config={"displaylogo": False}, style={"height": "330px"}),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "1fr 2fr", "gap": "16px", "alignItems": "start"},
                children=[
                    dcc.Graph(figure=_build_signal_donut(stock), config={"displaylogo": False}, style={"height": "330px"}),
                    html.Div(
                        children=[
                            html.Div(
                                style={"display": "flex", "gap": "12px", "marginBottom": "10px", "flexWrap": "wrap"},
                                children=[
                                    _dropdown("p1-sector-dd", [{"label": value, "value": value} for value in sectors], [], "Sector"),
                                    _dropdown("p1-company-dd", [{"label": value, "value": value} for value in companies], [], "Company"),
                                ],
                            ),
                            html.Div(id="p1-table-container", children=_company_table(stock)),
                        ]
                    ),
                ],
            ),
        ]
    )


def register_page1_callbacks(app, stock_df):
    @app.callback(
        Output("p1-table-container", "children"),
        Input("p1-sector-dd", "value"),
        Input("p1-company-dd", "value"),
    )
    def update_table(sector_values, company_values):
        sectors = sector_values if isinstance(sector_values, list) else ([sector_values] if sector_values else [])
        companies = company_values if isinstance(company_values, list) else ([company_values] if company_values else [])
        return _company_table(stock_df, sectors, companies)
