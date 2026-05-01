import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

_RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
_OUT_LOGLOG  = os.path.join(_RESULTS_DIR, "complexity_loglog.png")
_OUT_LINEAR  = os.path.join(_RESULTS_DIR, "complexity_linear.png")
_OUT_OLD     = os.path.join(_RESULTS_DIR, "complexity_chart.png")

_X_MIN = 1_000
_X_MAX = 28_151_758
_PHASE1_ROWS = 420_922
_PHASE2_ROWS = 28_151_758

_CURVES = [
    ("O(1) — bulk_insert, copy_csv",  "green",      "solid"),
    ("O(log n)",                       "steelblue",  "solid"),
    ("O(n) — row_by_row, batch_insert","darkorange", "solid"),
    ("O(n²) — danger zone",            "red",        "dashed"),
]

_MAIN_TITLE = "Why algorithm choice matters more than hardware"


def _apply_style() -> None:
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("ggplot")


def _add_vlines(ax, *, data_coords: bool) -> None:
    for xval, label in [
        (_PHASE1_ROWS, "420K rows\n(Phase 1)"),
        (_PHASE2_ROWS, "28M rows\n(this benchmark)"),
    ]:
        ax.axvline(x=xval, color="#666666", linewidth=1.2, linestyle=":", alpha=0.8)
        if data_coords:
            ax.text(xval * 1.05, 1.0, label, color="#666666", fontsize=7.5, va="bottom")
        else:
            ax.text(xval + _X_MAX * 0.005, 0, label, color="#666666", fontsize=7.5, va="bottom")


def _make_loglog() -> None:
    _apply_style()
    x = np.logspace(np.log10(_X_MIN), np.log10(_X_MAX), 1000)

    def ys(x):
        return [
            np.ones_like(x),
            np.log2(np.where(x > 0, x, 1)) / np.log2(_X_MIN),
            x / _X_MIN,
            (x / _X_MIN) ** 2,
        ]

    y_vals = ys(x)

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for (lbl, col, ls), y in zip(_CURVES, y_vals):
        ax.plot(x, y, color=col, linewidth=2.5,
                linestyle="--" if ls == "dashed" else "-", label=lbl)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlim(_X_MIN, _X_MAX * 1.3)
    ax.set_ylim(0.8, y_vals[3][-1] * 2)

    _add_vlines(ax, data_coords=True)

    ax.set_xlabel("Number of rows", fontsize=11)
    ax.set_ylabel("Relative time (log scale)", fontsize=11)
    ax.tick_params(labelsize=9)
    ax.grid(True, which="both", linewidth=0.5, linestyle="--", alpha=0.5)

    ax.text(_X_MAX * 0.45, 1.8, "O(1): seconds",
            color="green", fontsize=8.5, ha="center", va="bottom")
    for y_val, lbl, col in [
        (y_vals[2][-1], "O(n): ~hours",          "darkorange"),
        (y_vals[3][-1], "O(n²): never finishes", "red"),
    ]:
        ax.annotate(lbl, xy=(_X_MAX, y_val),
                    xytext=(_X_MAX * 0.18, y_val),
                    color=col, fontsize=8,
                    arrowprops=dict(arrowstyle="->", color=col, lw=0.8),
                    va="center")

    ax.legend(fontsize=9, loc="upper left")

    fig.suptitle(_MAIN_TITLE, fontsize=14, fontweight="bold", y=1.0)
    ax.set_title("Log-Log Scale — all curves appear linear, hides true difference",
                 fontsize=10, color="#444444", pad=8)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    out = os.path.abspath(_OUT_LOGLOG)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print("Saved results/complexity_loglog.png")


def _make_linear() -> None:
    _apply_style()
    x = np.linspace(0, _X_MAX, 2000)

    y_n_max  = _X_MAX / _X_MIN
    y_lin_max = y_n_max * 3

    def ys(x):
        return [
            np.ones_like(x),
            np.log2(np.where(x > 0, x, 1)) / np.log2(_X_MIN),
            x / _X_MIN,
            (x / _X_MIN) ** 2,
        ]

    y_vals = ys(x)

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for (lbl, col, ls), y in zip(_CURVES, y_vals):
        y_clipped = np.where(y <= y_lin_max * 1.5, y, np.nan)
        ax.plot(x, y_clipped, color=col, linewidth=2.5,
                linestyle="--" if ls == "dashed" else "-", label=lbl)

    ax.set_xlim(0, _X_MAX * 1.05)
    ax.set_ylim(0, y_lin_max)

    _add_vlines(ax, data_coords=False)

    ax.annotate(
        "O(n²) is off the chart",
        xy=(_X_MAX * 0.55, y_lin_max * 0.97),
        xytext=(_X_MAX * 0.3, y_lin_max * 0.75),
        color="red", fontsize=8.5,
        arrowprops=dict(arrowstyle="->", color="red", lw=0.9),
        va="center",
    )

    def _fmt_millions(val, _pos):
        if val == 0:
            return "0"
        m = val / 1_000_000
        if m >= 1:
            return f"{m:.0f}M" if m == int(m) else f"{m:.1f}M"
        return f"{val / 1_000:.0f}K"

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    ax.xaxis.set_major_locator(mticker.MultipleLocator(5_000_000))

    ax.set_xlabel("Number of rows", fontsize=11)
    ax.set_ylabel("Relative time (linear scale)", fontsize=11)
    ax.tick_params(labelsize=9)
    ax.grid(True, linewidth=0.5, linestyle="--", alpha=0.5)
    ax.legend(fontsize=9, loc="upper left")

    fig.suptitle(_MAIN_TITLE, fontsize=14, fontweight="bold", y=1.0)
    ax.set_title("Linear Scale — true shape, O(n²) disappears off chart, O(1) is flat",
                 fontsize=10, color="#444444", pad=8)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    out = os.path.abspath(_OUT_LINEAR)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print("Saved results/complexity_linear.png")


def main() -> None:
    _make_loglog()
    _make_linear()
    old = os.path.abspath(_OUT_OLD)
    if os.path.exists(old):
        os.remove(old)


if __name__ == "__main__":
    main()


