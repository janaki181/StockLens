import base64
import io

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DARK_BG = "#0f0f0f"
CARD_BG = "#1a1a1a"
TEXT_CLR = "#dddddd"
GRID_CLR = "#2a2a2a"
PURPLE = "#7F77DD"
TEAL = "#1D9E75"
CORAL = "#D85A30"
AMBER = "#EF9F27"


def apply_dark_style(ax, title: str = ""):
    ax.set_facecolor(CARD_BG)
    ax.tick_params(colors=TEXT_CLR, labelsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(GRID_CLR)
    ax.yaxis.label.set_color(TEXT_CLR)
    ax.xaxis.label.set_color(TEXT_CLR)
    ax.grid(axis="y", color=GRID_CLR, linewidth=0.5, linestyle="--")
    if title:
        ax.set_title(title, color=TEXT_CLR, fontsize=11, pad=10, fontweight="normal")


def fig_to_base64(fig) -> str:
    """Convert matplotlib figure to a base64 data URI for Dash image components."""
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=150,
        bbox_inches="tight",
        facecolor=DARK_BG,
        edgecolor="none",
    )
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return f"data:image/png;base64,{encoded}"
