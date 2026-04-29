import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Input, Output, State, dcc, html
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

PAGE_SIZE = 12


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


def _company_table(df, sector_filter=None, company_filter=None, signal_filter=None, page_index=0, page_size=PAGE_SIZE):
    frame = df.copy()
    frame["signal_label"] = frame["revenue_signal"].apply(_clean_signal)

    if sector_filter:
        frame = frame[frame["sector"].isin(sector_filter)]
    if company_filter:
        frame = frame[frame["ticker"].isin(company_filter)]
    if signal_filter:
        frame = frame[frame["signal_label"].isin(signal_filter)]

    frame = frame.sort_values("market_cap_cr", ascending=False)
    if frame.empty:
        empty = html.Div("No companies match selected filters.", style={"color": "#7a8793", "padding": "8px"})
        return empty, "Showing 0 of 0 companies", True, True, 0

    total_rows = len(frame)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    page_index = max(0, min(int(page_index or 0), total_pages - 1))
    start = page_index * page_size
    end = start + page_size
    frame = frame.iloc[start:end]

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

    table = html.Table(
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
    page_label = f"Showing {start + 1}-{min(end, total_rows)} of {total_rows} companies"
    has_prev = page_index > 0
    has_next = page_index < (total_pages - 1)
    return table, page_label, not has_prev, not has_next, page_index


def _dropdown(component_id, options, value, label, multi=True):
    return html.Div(
        style={"display": "flex", "alignItems": "center", "gap": "8px", "flex": "1 1 0", "minWidth": "0"},
        children=[
            html.Label(label, style={"color": "#4f6275", "fontSize": "12px", "whiteSpace": "nowrap", "flex": "0 0 auto"}),
            dcc.Dropdown(
                id=component_id,
                options=options,
                value=value,
                multi=multi,
                clearable=True,
                style={"width": "100%", "minWidth": "0", "fontSize": "12px", "background": "#ffffff"},
            ),
        ],
    )


def render_page1(stock):
    if stock is None or stock.empty:
        return html.Div("No stock snapshot data available.", style={"color": "#5f7387"})

    sectors = sorted(stock["sector"].dropna().unique().tolist())
    companies = sorted(stock["ticker"].dropna().unique().tolist())
    signal_options = [
        {"label": "Growing", "value": "Growing"},
        {"label": "Declining", "value": "Declining"},
        {"label": "Flat", "value": "Flat"},
        {"label": "No Data", "value": "No Data"},
    ]

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
                                style={"display": "flex", "gap": "10px", "marginBottom": "10px", "flexWrap": "nowrap", "alignItems": "center"},
                                children=[
                                    _dropdown("p1-sector-dd", [{"label": value, "value": value} for value in sectors], [], "Sector"),
                                    _dropdown("p1-company-dd", [{"label": value, "value": value} for value in companies], [], "Company"),
                                    _dropdown("p1-signal-dd", signal_options, [], "Growth Signal"),
                                ],
                            ),
                            html.Div(
                                style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "8px", "gap": "12px"},
                                children=[
                                    html.Div(id="p1-page-info", style={"color": "#607080", "fontSize": "12px"}),
                                    html.Div(
                                        style={"display": "flex", "gap": "8px"},
                                        children=[
                                            html.Button("◀", id="p1-prev-page", n_clicks=0, style={"padding": "4px 10px", "border": "1px solid #d6dde6", "background": "#ffffff", "borderRadius": "6px", "cursor": "pointer"}),
                                            html.Button("▶", id="p1-next-page", n_clicks=0, style={"padding": "4px 10px", "border": "1px solid #d6dde6", "background": "#ffffff", "borderRadius": "6px", "cursor": "pointer"}),
                                        ],
                                    ),
                                ],
                            ),
                            html.Div(id="p1-table-container", children=_company_table(stock)[0]),
                            dcc.Store(id="p1-page-index", data=0),
                        ]
                    ),
                ],
            ),
        ]
    )


def register_page1_callbacks(app, stock_df):
    @app.callback(
        Output("p1-table-container", "children"),
        Output("p1-page-info", "children"),
        Output("p1-prev-page", "disabled"),
        Output("p1-next-page", "disabled"),
        Output("p1-page-index", "data"),
        Input("p1-sector-dd", "value"),
        Input("p1-company-dd", "value"),
        Input("p1-signal-dd", "value"),
        Input("p1-prev-page", "n_clicks"),
        Input("p1-next-page", "n_clicks"),
        State("p1-page-index", "data"),
    )
    def update_table(sector_values, company_values, signal_values, prev_clicks, next_clicks, page_index):
        trigger = dash.callback_context.triggered[0]["prop_id"].split(".")[0] if dash.callback_context.triggered else ""
        sectors = sector_values if isinstance(sector_values, list) else ([sector_values] if sector_values else [])
        companies = company_values if isinstance(company_values, list) else ([company_values] if company_values else [])
        signals = signal_values if isinstance(signal_values, list) else ([signal_values] if signal_values else [])

        current_page = int(page_index or 0)
        if trigger == "p1-prev-page":
            current_page = max(0, current_page - 1)
        elif trigger == "p1-next-page":
            current_page += 1
        else:
            current_page = 0

        table, info, prev_disabled, next_disabled, current_page = _company_table(
            stock_df,
            sectors,
            companies,
            signals,
            current_page,
            PAGE_SIZE,
        )
        return table, info, prev_disabled, next_disabled, current_page
