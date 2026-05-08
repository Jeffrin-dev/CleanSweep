import openai

openai.api_key = "sk-proj-abc123fakekey9999"

def ask(prompt):
    return openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
