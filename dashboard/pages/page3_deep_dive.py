import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from plotly.subplots import make_subplots


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


def render_page3(price, tickers):
    if price is None or price.empty:
        return html.Div("No price history data available.", style={"color": "#5f7387"})

    selected = tickers if isinstance(tickers, list) else ([tickers] if tickers else [])
    if not selected:
        return html.Div("Select at least one company.", style={"color": "#5f7387"})

    selected = selected[:6]
    data = price[price["ticker"].isin(selected)].copy()
    if data.empty:
        return html.Div("No history found for selected companies.", style={"color": "#5f7387"})

    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date"]).sort_values(["ticker", "date"])

    fig_price = px.line(data, x="date", y="close", color="ticker")
    fig_price.update_yaxes(title_text="Close")

    vol = data[["date", "ticker", "volume", "volume_30d_avg"]].copy()
    fig_vol = make_subplots(specs=[[{"secondary_y": True}]])
    for ticker in selected:
        one = vol[vol["ticker"] == ticker]
        fig_vol.add_trace(go.Scatter(x=one["date"], y=one["volume"], mode="lines", name=f"{ticker} Volume"), secondary_y=False)
        if "volume_30d_avg" in one.columns:
            fig_vol.add_trace(
                go.Scatter(x=one["date"], y=one["volume_30d_avg"], mode="lines", line={"dash": "dot"}, name=f"{ticker} Vol Avg"),
                secondary_y=True,
            )
    fig_vol.update_yaxes(title_text="Volume", secondary_y=False)
    fig_vol.update_yaxes(title_text="30D Avg", secondary_y=True)

    focus = data[data["ticker"] == selected[0]].copy()
    fig_ind = make_subplots(rows=1, cols=2, subplot_titles=(f"RSI - {selected[0]}", f"MACD - {selected[0]}"))
    if "rsi_14" in focus.columns:
        fig_ind.add_trace(go.Scatter(x=focus["date"], y=focus["rsi_14"], mode="lines", name="RSI"), row=1, col=1)
        fig_ind.add_hline(y=70, line_dash="dash", line_color="#d9534f", row=1, col=1)
        fig_ind.add_hline(y=30, line_dash="dash", line_color="#198754", row=1, col=1)
    if "macd" in focus.columns:
        fig_ind.add_trace(go.Scatter(x=focus["date"], y=focus["macd"], mode="lines", name="MACD"), row=1, col=2)
    if "macd_signal" in focus.columns:
        fig_ind.add_trace(go.Scatter(x=focus["date"], y=focus["macd_signal"], mode="lines", name="Signal"), row=1, col=2)
    if "macd_hist" in focus.columns:
        fig_ind.add_trace(go.Bar(x=focus["date"], y=focus["macd_hist"], name="Hist", opacity=0.45), row=1, col=2)

    return html.Div(
        [
            dcc.Graph(figure=_layout(fig_price, "Price Trend (Multi-Company)", 300), config={"displaylogo": False}),
            dcc.Graph(figure=_layout(fig_vol, "Volume vs 30-Day Average", 300), config={"displaylogo": False}),
            dcc.Graph(figure=_layout(fig_ind, "Technical Indicators", 300), config={"displaylogo": False}),
        ]
    )
