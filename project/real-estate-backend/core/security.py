from passlib.context import CryptContext
from datetime import datetime, timedelta
from jose import JWTError, jwt
from core.config import SECRET_KEY, ACCESS_TOKEN_EXPIRE_MINUTES
from schemas.user import TokenData 
from fastapi.security import OAuth2PasswordBearer 
from fastapi import Depends, HTTPException, status 
from fastapi.security import OAuth2PasswordBearer 

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

# 비밀번호 해싱을 위한 컨텍스트 설정
# bcrypt 알고리즘을 사용하며, deprecated="auto"는 호환성을 유지해줍니다.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """일반 비밀번호와 해시된 비밀번호가 일치하는지 확인합니다."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """일반 비밀번호를 해시하여 반환합니다."""
    return pwd_context.hash(password)

def create_access_token(data: dict):
    """Access Token을 생성합니다."""
    to_encode = data.copy()
    # 토큰 만료 시간 설정
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    # JWT 토큰 인코딩
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def verify_token(token: str, credentials_exception):
    """토큰을 검증하고, 유효하면 payload를 반환합니다."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception
    return token_data