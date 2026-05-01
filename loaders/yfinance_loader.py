
import logging
import os

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
KAGGLE_DIR = os.path.join(os.path.dirname(__file__), "..", "kaggle-dataset", "stocks")

# S&P 500 sample list
SAMPLE_TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "TSLA", "JPM", "V", "XOM",
    "MA", "JNJ", "PG", "HD", "MRK", "AVGO", "CVX", "ABBV", "COST", "PEP",
    "ADBE", "KO", "WMT", "MCD", "CSCO", "CRM", "ACN", "BAC", "TMO", "ABT",
    "NFLX", "LIN", "DHR", "ORCL", "AMD", "TXN", "QCOM", "PM", "NEE", "RTX",
    "HON", "UPS", "AMGN", "INTU", "IBM", "SPGI", "CAT", "GS", "BLK", "ISRG"
]


def download(
    tickers: list[str] | None = None,
    output_dir: str = RAW_DIR,
    kaggle_dir: str = KAGGLE_DIR,
) -> pd.DataFrame:
    """
    Load daily OHLCV data from local Kaggle dataset CSV files and save
    individual + combined CSVs in long format.

    Parameters
    ----------
    tickers:
        List of ticker symbols. If None, uses the hardcoded sample list.
    output_dir:
        Directory where output CSV files are written.
    kaggle_dir:
        Directory containing the Kaggle dataset CSV files ({TICKER}.csv).

    Returns
    -------
    Combined DataFrame of all successfully loaded tickers (long format).
    """
    if tickers is None:
        tickers = SAMPLE_TICKERS

    os.makedirs(output_dir, exist_ok=True)

    all_frames: list[pd.DataFrame] = []
    skipped: list[str] = []

    for ticker in tqdm(tickers, desc="Loading tickers", unit="ticker"):
        csv_path = os.path.join(kaggle_dir, f"{ticker}.csv")
        if not os.path.exists(csv_path):
            logger.warning("CSV not found for %s (%s) — skipping.", ticker, csv_path)
            skipped.append(ticker)
            continue

        ticker_df = pd.read_csv(csv_path, parse_dates=["Date"])
        ticker_df = ticker_df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        ticker_df = ticker_df.drop(columns=["Adj Close"], errors="ignore")
        ticker_df = ticker_df.dropna(subset=["close"])
        ticker_df["date"] = ticker_df["date"].dt.date
        ticker_df.insert(1, "ticker", ticker)
        ticker_df = ticker_df[["date", "ticker", "open", "high", "low", "close", "volume"]]

        out_path = os.path.join(output_dir, f"{ticker}.csv")
        ticker_df.to_csv(out_path, index=False)
        all_frames.append(ticker_df)

    if skipped:
        logger.warning("Skipped %d ticker(s): %s", len(skipped), ", ".join(skipped))

    if not all_frames:
        logger.error("No data was loaded.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    combined_path = os.path.join(output_dir, "all_stocks.csv")
    combined.to_csv(combined_path, index=False)
    logger.info(
        "Saved %d tickers to %s (%d rows total).",
        len(all_frames),
        combined_path,
        len(combined),
    )
    return combined


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    download()

