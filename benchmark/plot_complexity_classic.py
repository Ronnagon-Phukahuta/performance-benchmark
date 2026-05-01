import math
import os

import matplotlib.pyplot as plt
import numpy as np

_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "results", "complexity_classic.png")


def main() -> None:
    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:
        plt.style.use("ggplot")

    x = np.linspace(1, 20, 500)

    def _clip(arr):
        return np.clip(arr, 0, 1000)

    curves = [
        ("O(1) — constant",          "green",   "-",  _clip(np.ones_like(x))),
        ("O(log n) — logarithmic",    "blue",    "-",  _clip(np.log2(x))),
        ("O(n) — linear",             "limegreen","-", _clip(x)),
        ("O(n log n) — linearithmic", "purple",  "-",  _clip(x * np.log2(x))),
        ("O(n²) — quadratic",         "orange",  "-",  _clip(x ** 2)),
        ("O(2ⁿ) — exponential",       "red",     "-",  _clip(2.0 ** x)),
        ("O(n!) — factorial",          "darkred", "--", np.array([min(math.gamma(v + 1), 1000) for v in x])),
    ]

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    for label, color, ls, y in curves:
        ax.plot(x, y, color=color, linewidth=2.5, linestyle=ls, label=label)

    ax.set_xlim(1, 20)
    ax.set_ylim(0, 1000)
    ax.set_xlabel("Input size (n)", fontsize=12)
    ax.set_ylabel("Operations", fontsize=12)

    fig.suptitle("Big O Complexity — Growth Rates", fontsize=14, fontweight="bold", y=0.98)
    ax.set_title("How different algorithms scale as input size grows",
                 fontsize=10, color="#444444", pad=8)

    ax.tick_params(labelsize=9)
    ax.grid(True, linewidth=0.5, linestyle="--", alpha=0.5)

    # Zone labels on the right side
    zone_labels = [
        (curves[0][3][-1], curves[1][3][-1], "EXCELLENT", "green"),
        (curves[2][3][-1], curves[2][3][-1], "GOOD",      "limegreen"),
        (curves[3][3][-1], curves[4][3][-1], "BAD",       "orange"),
        (curves[5][3][-1], curves[6][3][-1], "HORRIBLE",  "red"),
    ]
    for y_lo, y_hi, zone_text, color in zone_labels:
        y_mid = (y_lo + y_hi) / 2
        ax.text(20.3, min(y_mid, 980), zone_text,
                color=color, fontsize=9, fontweight="bold",
                va="center", ha="left", clip_on=False)

    # Annotation box
    ax.text(
        0.42, 0.72,
        "In this benchmark:\n  bulk_insert = O(1)  [fast]\n  row_by_row  = O(n)  [slow]",
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8),
    )

    ax.legend(fontsize=9, loc="upper left")

    plt.tight_layout(rect=[0, 0, 0.93, 0.95])
    out = os.path.abspath(_OUTPUT_PATH)
    plt.savefig(out, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close()
    print(f"Saved to results/complexity_classic.png")


if __name__ == "__main__":
    main()
