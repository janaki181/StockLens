import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, callback, dcc, html

from data_loader import get_sectors, get_tickers, load_price_history, load_stock_data
from pages.page1_sector import register_page1_callbacks, render_page1
from pages.page2_fundamentals import render_page2
from pages.page3_deep_dive import render_page3
from pages.page4_vs_nifty import render_page4
from pages.page5_alerts import render_page5

# Load data once at startup.
stock = load_stock_data()
price = load_price_history()

tickers = get_tickers(stock)
sectors = get_sectors(stock)

default_ticker = tickers[0] if tickers else None
default_sector = sectors[0] if sectors else None

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    suppress_callback_exceptions=True,
)

register_page1_callbacks(app, stock)

PAGES = ["Sector Pulse", "Fundamentals", "Company Deep Dive", "vs NIFTY", "Alerts"]

NAV_STYLE = {
    "display": "flex",
    "gap": "4px",
    "padding": "0 32px",
    "borderBottom": "1px solid #dde3ea",
    "background": "#ffffff",
}

BTN_BASE = {
    "background": "transparent",
    "border": "none",
    "borderBottom": "2px solid transparent",
    "color": "#6c7a89",
    "padding": "12px 18px",
    "cursor": "pointer",
    "fontSize": "14px",
    "fontFamily": "inherit",
}

BTN_ACTIVE = {**BTN_BASE, "color": "#1f6feb", "borderBottom": "2px solid #1f6feb"}


def _controls_row():
    return html.Div(
        style={"display": "flex", "gap": "16px", "alignItems": "center", "flexWrap": "wrap"},
        children=[
            html.Div(
                id="note-page0",
                children=html.P(
                    "Showing latest snapshot - all available companies",
                    style={"color": "#607080", "fontSize": "13px", "margin": 0},
                ),
            ),
            html.Div(
                id="sector-wrap",
                children=[
                    html.Label("Sector", style={"color": "#4f6275", "fontSize": "13px"}),
                    dcc.Dropdown(
                        id="sector-filter",
                        options=[{"label": s, "value": s} for s in sectors],
                        value=[default_sector] if default_sector else [],
                        multi=True,
                        clearable=True,
                        style={"width": "360px", "fontSize": "13px"},
                    ),
                ],
            ),
            html.Div(
                id="fund-company-wrap",
                children=[
                    html.Label("Company", style={"color": "#4f6275", "fontSize": "13px"}),
                    dcc.Dropdown(
                        id="fund-company-filter",
                        options=[
                            {
                                "label": f"{row.company_name} ({row.ticker})",
                                "value": row.ticker,
                            }
                            for row in stock[["ticker", "company_name"]].dropna().drop_duplicates().sort_values("company_name").itertuples(index=False)
                        ],
                        value=[],
                        multi=True,
                        clearable=True,
                        style={"width": "420px", "fontSize": "13px"},
                    ),
                ],
            ),
            html.Div(
                id="ticker-wrap",
                children=[
                    html.Label("Company", style={"color": "#4f6275", "fontSize": "13px"}),
                    dcc.Dropdown(
                        id="ticker-filter",
                        options=[{"label": t, "value": t} for t in tickers],
                        value=[default_ticker] if default_ticker else [],
                        multi=True,
                        clearable=True,
                        style={"width": "360px", "fontSize": "13px"},
                    ),
                ],
            ),
            html.Div(
                id="ticker-nifty-wrap",
                children=[
                    html.Label("Company", style={"color": "#4f6275", "fontSize": "13px"}),
                    dcc.Dropdown(
                        id="ticker-filter-nifty",
                        options=[{"label": t, "value": t} for t in tickers],
                        value=[default_ticker] if default_ticker else [],
                        multi=True,
                        clearable=True,
                        style={"width": "360px", "fontSize": "13px"},
                    ),
                ],
            ),
            html.Div(
                id="vol-wrap",
                style={"minWidth": "360px", "maxWidth": "460px", "width": "100%"},
                children=[
                    html.Label("Min volume ratio", style={"color": "#4f6275", "fontSize": "13px"}),
                    dcc.Slider(
                        id="vol-slider",
                        min=0.5,
                        max=4.0,
                        step=0.1,
                        value=1.5,
                        marks={1: "1x", 2: "2x", 3: "3x", 4: "4x"},
                        tooltip={"placement": "bottom"},
                    ),
                ],
            ),
        ],
    )


app.layout = html.Div(
    style={"background": "#f6f8fb", "minHeight": "100vh", "fontFamily": "Segoe UI, sans-serif"},
    children=[
        html.Div(
            style={"padding": "20px 32px 0", "background": "#ffffff"},
            children=[
                html.H1(
                    "NIFTY 50 Intelligence Dashboard",
                    style={"color": "#1f2d3d", "fontSize": "22px", "fontWeight": "600", "margin": "0 0 16px"},
                ),
            ],
        ),
        html.Div(
            id="nav-bar",
            style=NAV_STYLE,
            children=[
                html.Button(label, id=f"btn-{idx}", n_clicks=0, style=BTN_ACTIVE if idx == 0 else BTN_BASE)
                for idx, label in enumerate(PAGES)
            ],
        ),
        html.Div(id="controls-bar", style={"padding": "16px 32px 0", "background": "#ffffff"}, children=_controls_row()),
        html.Div(id="page-content", style={"padding": "24px 32px"}),
        dcc.Store(id="active-page", data=0),
    ],
)


def _normalize_multi(value, fallback):
    if isinstance(value, list):
        return value if value else ([fallback] if fallback else [])
    if value:
        return [value]
    return [fallback] if fallback else []


@callback(
    Output("active-page", "data"),
    [Input(f"btn-{idx}", "n_clicks") for idx in range(5)],
    prevent_initial_call=True,
)
def update_active_page(*_):
    ctx = dash.callback_context
    if not ctx.triggered:
        return 0
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return int(button_id.split("-")[1])


@callback(
    [Output(f"btn-{idx}", "style") for idx in range(5)],
    Input("active-page", "data"),
)
def style_buttons(active):
    return [BTN_ACTIVE if idx == active else BTN_BASE for idx in range(5)]


@callback(
    Output("note-page0", "style"),
    Output("sector-wrap", "style"),
    Output("fund-company-wrap", "style"),
    Output("ticker-wrap", "style"),
    Output("ticker-nifty-wrap", "style"),
    Output("vol-wrap", "style"),
    Input("active-page", "data"),
)
def toggle_controls(active):
    hidden = {"display": "none"}
    shown = {"display": "block"}

    return (
        shown if active == 0 else hidden,
        shown if active == 1 else hidden,
        shown if active == 1 else hidden,
        shown if active == 2 else hidden,
        shown if active == 3 else hidden,
        shown if active == 4 else hidden,
    )


@callback(
    Output("fund-company-filter", "options"),
    Output("fund-company-filter", "value"),
    Input("sector-filter", "value"),
    Input("fund-company-filter", "value"),
)
def sync_fundamentals_company_dropdown(sector_value, selected_companies):
    selected_sectors = sector_value if isinstance(sector_value, list) else ([sector_value] if sector_value else [])

    scoped = stock.copy()
    if selected_sectors:
        scoped = scoped[scoped["sector"].isin(selected_sectors)]

    options = [
        {
            "label": f"{row.company_name} ({row.ticker})",
            "value": row.ticker,
        }
        for row in scoped[["ticker", "company_name"]].dropna().drop_duplicates().sort_values("company_name").itertuples(index=False)
    ]

    current_values = selected_companies if isinstance(selected_companies, list) else ([selected_companies] if selected_companies else [])
    allowed = {item["value"] for item in options}
    sanitized = [item for item in current_values if item in allowed]
    return options, sanitized


@callback(
    Output("page-content", "children"),
    Input("active-page", "data"),
    Input("sector-filter", "value"),
    Input("fund-company-filter", "value"),
    Input("ticker-filter", "value"),
    Input("ticker-filter-nifty", "value"),
    Input("vol-slider", "value"),
)
def render_page(active, sector, fund_company, ticker, ticker_nifty, vol_min):
    selected_sectors = _normalize_multi(sector, default_sector)
    selected_fund_companies = fund_company if isinstance(fund_company, list) else ([fund_company] if fund_company else [])
    selected_tickers = _normalize_multi(ticker, default_ticker)
    selected_tickers_nifty = _normalize_multi(ticker_nifty, default_ticker)

    if active == 0:
        return render_page1(stock)
    if active == 1:
        return render_page2(stock, selected_sectors, selected_fund_companies)
    if active == 2:
        return render_page3(price, selected_tickers)
    if active == 3:
        return render_page4(price, selected_tickers_nifty)
    if active == 4:
        return render_page5(stock, price, vol_min or 1.5)
    return html.Div()


if __name__ == "__main__":
    app.run(debug=True, port=8050)
