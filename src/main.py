from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()

model = ChatGoogleGenerativeAI(
    model="gemini-3-flash-preview",
    temperature=1.0,  # Gemini 3.0+ defaults to 1.0
    max_tokens=None,
    timeout=None,
    max_retries=2,
    # other params...
)

messages = [
    (
        "system",
        "You are an senior data engineer, help the user and answer questions about our metrics.",
    ),
    ("human", "What is ride completion rate?"),
]

if __name__ == "__main__":
    ai_msg = model.invoke(messages)
    print(ai_msg)
