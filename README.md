# vibe_de

A stock data RAG (Retrieval Augmented Generation) system to test out AWS Bedrock capabilities.

## Purpose

This project demonstrates building an end-to-end RAG pipeline using AWS Bedrock and LangChain:

1. **Data Collection**: Scrapes stock data (VFV holdings) from Yahoo Finance
2. **Data Transformation**: Converts stock data into RAG-formatted Markdown documents with YAML front matter
3. **Storage**: Uploads processed documents to S3
4. **Knowledge Base Sync**: Syncs data to AWS Bedrock Knowledge Base for retrieval-based question answering
5. **Query Interface**: Uses LangChain with Google Generative AI (Gemini 3.0) to answer stock-related questions with retrieval-augmented responses

## How It Works

- **Scraper** (`stock_vfv_flow.py`): Fetches stock info and recent news from Yahoo Finance
- **Utilities** (`util.py`): Formats data as Markdown documents suitable for RAG/Knowledge Base ingestion
- **Sync** (`kb_sync.py`): Manages ingestion jobs to sync S3 documents into Bedrock Knowledge Base
- **Main** (`main.py`): Query interface that retrieves relevant stock documents and generates answers using Gemini

## Example Query

```
"Recommend me a good tech stock based on recent news."
```

The system retrieves relevant stock documents from the Knowledge Base and uses them to inform the LLM's response.