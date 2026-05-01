import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "results", "complexity_chart.png")

_X_MIN = 1_000
_X_MAX = 28_151_758
_PHASE1_ROWS = 420_922
_PHASE2_ROWS = 28_151_758

_CURVES = [
    ("O(1) — bulk_insert, copy_csv", "green",      "solid"),
    ("O(log n)",                      "steelblue",  "solid"),
    ("O(n) — row_by_row, batch_insert","darkorange","solid"),
    ("O(n²) — danger zone",           "red",        "dashed"),
]


def _add_vlines(ax, *, data_coords: bool) -> None:
    for xval, label in [
        (_PHASE1_ROWS, "420K rows\n(Phase 1)"),
        (_PHASE2_ROWS, "28M rows\n(this benchmark)"),
    ]:
        ax.axvline(x=xval, color="#666666", linewidth=1.2, linestyle=":", alpha=0.8)
        if data_coords:
            ax.text(xval * 1.05, 1.0, label, color="#666666", fontsize=7.5, va="bottom")
        else:
            ax.text(xval + _X_MAX * 0.005, ax.get_ylim()[0] * 1.02 if ax.get_ylim()[0] > 0 else 0,
                    label, color="#666666", fontsize=7.5, va="bottom")


def main() -> None:
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("ggplot")

    # --- data ---
    x_log = np.logspace(np.log10(_X_MIN), np.log10(_X_MAX), 1000)
    x_lin = np.linspace(0, _X_MAX, 2000)

    def curves(x):
        return [
            np.ones_like(x),
            np.log2(np.where(x > 0, x, 1)) / np.log2(_X_MIN),
            x / _X_MIN,
            (x / _X_MIN) ** 2,
        ]

    ys_log = curves(x_log)
    ys_lin = curves(x_lin)

    y_n_max = _X_MAX / _X_MIN          # O(n) value at 28M rows
    y_lin_max = y_n_max * 3

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    fig.patch.set_facecolor("white")

    # ── LEFT: log-log ────────────────────────────────────────────
    ax1.set_facecolor("white")
    for (lbl, col, ls), y in zip(_CURVES, ys_log):
        ax1.plot(x_log, y, color=col, linewidth=2.5,
                 linestyle="--" if ls == "dashed" else "-", label=lbl)

    ax1.set_xscale("log")
    ax1.set_yscale("log")
    ax1.set_xlim(_X_MIN, _X_MAX * 1.3)
    ax1.set_ylim(0.8, ys_log[3][-1] * 2)

    _add_vlines(ax1, data_coords=True)

    ax1.set_xlabel("Number of rows", fontsize=11)
    ax1.set_ylabel("Relative time (log scale)", fontsize=11)
    ax1.set_title("Log-Log Scale\nAll curves appear linear — hides true difference", fontsize=11, pad=10)
    ax1.tick_params(labelsize=9)
    ax1.grid(True, which="both", linewidth=0.5, linestyle="--", alpha=0.5)

    # text labels on curves (log subplot)
    ax1.text(_X_MAX * 0.45, 1.8, "O(1): seconds",
             color="green", fontsize=8.5, ha="center", va="bottom")
    for y_val, lbl, col in [
        (ys_log[2][-1], "O(n): ~hours",          "darkorange"),
        (ys_log[3][-1], "O(n²): never finishes", "red"),
    ]:
        ax1.annotate(lbl, xy=(_X_MAX, y_val),
                     xytext=(_X_MAX * 0.18, y_val),
                     color=col, fontsize=8,
                     arrowprops=dict(arrowstyle="->", color=col, lw=0.8),
                     va="center")

    ax1.legend(fontsize=9, loc="upper left")

    # ── RIGHT: linear ────────────────────────────────────────────
    ax2.set_facecolor("white")
    for (lbl, col, ls), y in zip(_CURVES, ys_lin):
        y_clipped = np.where(y <= y_lin_max * 1.5, y, np.nan)
        ax2.plot(x_lin, y_clipped, color=col, linewidth=2.5,
                 linestyle="--" if ls == "dashed" else "-", label=lbl)

    ax2.set_xlim(0, _X_MAX * 1.05)
    ax2.set_ylim(0, y_lin_max)

    _add_vlines(ax2, data_coords=False)

    # O(n²) off-chart annotation
    ax2.annotate(
        "O(n²) is off the chart",
        xy=(_X_MAX * 0.55, y_lin_max * 0.97),
        xytext=(_X_MAX * 0.3, y_lin_max * 0.75),
        color="red", fontsize=8.5,
        arrowprops=dict(arrowstyle="->", color="red", lw=0.9),
        va="center",
    )

    # M-suffix x-axis formatter
    def _fmt_millions(val, _pos):
        if val == 0:
            return "0"
        m = val / 1_000_000
        if m >= 1:
            return f"{m:.0f}M" if m == int(m) else f"{m:.1f}M"
        k = val / 1_000
        return f"{k:.0f}K"

    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(_fmt_millions))
    ax2.xaxis.set_major_locator(mticker.MultipleLocator(5_000_000))

    ax2.set_xlabel("Number of rows", fontsize=11)
    ax2.set_ylabel("Relative time (linear scale)", fontsize=11)
    ax2.set_title("Linear Scale\nTrue shape — O(n²) disappears, O(1) is flat, O(n) is diagonal", fontsize=11, pad=10)
    ax2.tick_params(labelsize=9)
    ax2.grid(True, linewidth=0.5, linestyle="--", alpha=0.5)
    ax2.legend(fontsize=9, loc="upper left")

    # ── Suptitle ─────────────────────────────────────────────────
    fig.suptitle(
        "Why algorithm choice matters more than hardware",
        fontsize=15, fontweight="bold", y=1.01,
    )
    fig.text(0.5, 0.97, "At 28M rows, O(n) = hours. O(1) = seconds.",
             ha="center", fontsize=11, color="#444444")

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    out = os.path.abspath(_OUTPUT_PATH)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"Saved: {os.path.relpath(out)}")


if __name__ == "__main__":
    main()

