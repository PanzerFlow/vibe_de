from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import boto3
import frontmatter
from markdownify import markdownify as html_to_md

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def dict_to_rag_markdown(data: Dict[str, Any]) -> str:
    ticker = (data.get("ticker") or "unknown").strip()
    long_name = (data.get("longName") or ticker).strip()
    updated = datetime.now(timezone.utc).date().isoformat()

    meta = {
        "doc_id": f"equity-{ticker}",
        "title": f"{long_name} ({ticker})" if ticker != "unknown" else long_name,
        "type": "equity_profile",
        "sector": data.get("sector"),
        "industry": data.get("industry"),
        "source": "Yahoo Finance",
        "updated": updated,
        "tags": (
            ["equity", "stocks", ticker]
            if ticker != "unknown"
            else ["equity", "stocks"]
        ),
    }

    lines = [
        f"# {long_name} ({ticker})" if ticker != "unknown" else f"# {long_name}",
        "",
        "## Company Overview",
        f"{long_name} operates in the {data.get('sector')} sector, "
        f"within the {data.get('industry')} industry.",
        "",
        "## Market Data",
        f"- Market Cap: {data.get('marketCap')}",
        f"- Previous Close: {data.get('previousClose')}",
        f"- Current Price: {data.get('currentPrice')}",
        f"- 52 Week High: {data.get('52WeekHigh')}",
        f"- 52 Week Low: {data.get('52WeekLow')}",
        "",
        "## Recent News",
    ]

    for item in data.get("news", []) or []:
        c = (item or {}).get("content") or {}
        title = c.get("title")
        if not title:
            continue

        provider = (c.get("provider") or {}).get("displayName", "Unknown")
        pub_date = (c.get("pubDate") or "")[:10]

        raw = c.get("summary") or c.get("description") or ""
        # Convert HTML to markdown (much cleaner than regex stripping)
        summary_md = html_to_md(raw).strip() if raw else ""
        if not summary_md:
            continue

        lines += [
            "",
            f"### {title}",
            f"**Published:** {pub_date}  ",
            f"**Source:** {provider}",
            "",
            summary_md,
        ]

    post = frontmatter.Post("\n".join(lines).strip() + "\n", **meta)
    return frontmatter.dumps(post)  # includes YAML front matter automatically


def sanitize_s3_metadata(md: Dict[str, str]) -> Dict[str, str]:
    # keep it minimal: stringify values; avoid None
    return {str(k).lower(): str(v) for k, v in (md or {}).items() if v is not None}


def upload_data_to_s3(
    data: Dict[str, Any],
    bucket: str,
    key: str,
    metadata: Optional[Dict[str, str]] = None,
) -> bool:
    """Upload RAG Markdown (YAML front matter + MD body) to S3."""
    try:
        body = dict_to_rag_markdown(data)

        extra_args: Dict[str, Any] = {"ContentType": "text/markdown; charset=utf-8"}
        if metadata:
            extra_args["Metadata"] = sanitize_s3_metadata(metadata)

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            **extra_args,
        )

        logger.info(f"Uploaded Markdown to s3://{bucket}/{key}")
        return True

    except Exception as e:
        logger.error(f"Error uploading {key} to {bucket}: {e}", exc_info=True)
        return False
