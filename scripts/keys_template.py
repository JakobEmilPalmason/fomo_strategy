import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Polygon API keys - Use environment variables or .env file
# DO NOT hardcode actual keys in this file
polygon_key = os.getenv('POLYGON_KEY')
polygon_api_key = os.getenv('POLYGON_API_KEY')
polygon_access_key = os.getenv('POLYGON_ACCESS_KEY')
polygon_secret_key = os.getenv('POLYGON_SECRET_KEY')

# S3 configuration
s3_endpoint = os.getenv('S3_ENDPOINT', 'https://files.polygon.io')
bucket_name = os.getenv('BUCKET_NAME', 'flatfiles')

# Instructions:
# 1. Copy this file to keys.py: cp scripts/keys_template.py scripts/keys.py
# 2. Add your actual API keys to your .env file
# 3. The keys.py file is gitignored to prevent accidental commits 