import json
import logging
from datetime import datetime
from typing import Any, Dict, Generator

import polars as pl
from scraper_config import S3_BUCKET_NAME, TEST_DATA_PATH, VFV_HOLDING_EXCEL_PATH
from util import upload_json_to_s3
from yfinance import Ticker

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class stock_etl_flow:
    def __init__(self):
        self.vfv_pdf: pl.DataFrame = None

    def get_vfv_info(self) -> None:
        logger.info("Parsing VFV holdings from Excel file")
        df = pl.read_excel(
            VFV_HOLDING_EXCEL_PATH, engine="calamine", read_options={"header_row": 6}
        )
        self.vfv_pdf = df

    def _get_ticker_summaries(self) -> Generator[Dict[str, Any], None, None]:
        """Generator that yields stock data one at a time"""
        logger.info("Pulling individual stock data from Yahoo Finance")
        tickers = self.vfv_pdf.select("Ticker").to_series().to_list()

        for ticker_symbol in tickers:
            try:
                ticker = Ticker(ticker_symbol)
                info = ticker.info
                news = ticker.news[:3] if ticker.news else []  # Get top 3 news

                yield {
                    "ticker": ticker_symbol,
                    "longName": info.get("longName"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "marketCap": info.get("marketCap"),
                    "previousClose": info.get("previousClose"),
                    "currentPrice": info.get("currentPrice"),
                    "52WeekHigh": info.get("fiftyTwoWeekHigh"),
                    "52WeekLow": info.get("fiftyTwoWeekLow"),
                    "news": news,
                }
                logger.info(f"Fetched data for {ticker_symbol}")
            except Exception as e:
                logger.error(f"Error fetching data for {ticker_symbol}: {e}")

    def store_data_locally(self) -> None:
        logger.info("Storing stock data to JSON files")
        for stock_data in self._get_ticker_summaries():
            filename = TEST_DATA_PATH + f"/{stock_data['ticker']}.json"
            logger.info(f"Storing data for {stock_data['ticker']} to {filename}")
            with open(filename, "w") as f:
                json.dump(stock_data, f, indent=2)
        logger.info("Storing VFV dataframe to CSV")
        self.vfv_pdf.write_csv(TEST_DATA_PATH + "/vfv_holdings.csv")
        logger.info("Data storage completed")

    def store_data_to_s3(self) -> None:
        """Upload stock data to S3 with metadata"""
        logger.info(f"Uploading stock data to S3 bucket: {S3_BUCKET_NAME}")

        for stock_data in self._get_ticker_summaries():
            ticker = stock_data["ticker"]
            s3_key = f"stocks/{ticker}.json"

            # Create metadata with ticker and timestamp
            metadata = {
                "ticker": ticker,
                "timestamp": datetime.now().isoformat(),
                "sector": stock_data.get("sector", "unknown"),
            }

            success = upload_json_to_s3(
                data=stock_data, bucket=S3_BUCKET_NAME, key=s3_key, metadata=metadata
            )

            if success:
                logger.info(f"Successfully uploaded {ticker} to S3")
            else:
                logger.error(f"Failed to upload {ticker} to S3")

        logger.info("S3 upload completed")

    def run(self) -> None:
        self.get_vfv_info()
        self.store_data_to_s3()  # Upload to S3
        # self.store_data_locally()  # Optional: also save locally


if __name__ == "__main__":
    flow = stock_etl_flow()
    flow.run()
