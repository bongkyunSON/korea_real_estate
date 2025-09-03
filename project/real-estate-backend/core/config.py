import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY")
# .env 파일에 ACCESS_TOKEN_EXPIRE_MINUTES 값이 없으면 기본 30을 사용
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL 환경 변수를 찾을 수 없습니다.")

if SECRET_KEY is None:
    raise ValueError("SECRET_KEY 환경 변수를 찾을 수 없습니다.")