import boto3
import openai

# AWS credentials
AWS_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"

# OpenAI
openai.api_key = "sk-proj-abc123fakekey9999"

# S3 - public bucket
s3 = boto3.client('s3')
s3.put_bucket_acl(Bucket='prod-data', ACL='public-read')

# No rate limiting
def call_ai(prompt):
    return openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
