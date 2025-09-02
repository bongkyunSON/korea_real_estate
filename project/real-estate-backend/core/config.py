# project/real-estate-backend/core/config.py
import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL 환경 변수를 찾을 수 없습니다.")