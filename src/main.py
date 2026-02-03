import os

from dotenv import load_dotenv
from langchain_aws.retrievers import AmazonKnowledgeBasesRetriever
from langchain_classic.chains import RetrievalQA
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

model = ChatGoogleGenerativeAI(
    model="gemini-3-flash-preview",
    temperature=1.0,  # Gemini 3.0+ defaults to 1.0
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

retriever = AmazonKnowledgeBasesRetriever(
    knowledge_base_id=os.getenv("AWS_KNOWLEDGE_BASE_ID"),
    retrieval_config={"vectorSearchConfiguration": {"numberOfResults": 4}},
)


if __name__ == "__main__":
    # ai_msg = model.invoke(messages)

    qa = RetrievalQA.from_chain_type(
        llm=model, retriever=retriever, return_source_documents=True
    )

    res = qa("Recommend me a good tech stock based on recent news.")
    print(res)
