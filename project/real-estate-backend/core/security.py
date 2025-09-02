from passlib.context import CryptContext

# 비밀번호 해싱을 위한 컨텍스트 설정
# bcrypt 알고리즘을 사용하며, deprecated="auto"는 호환성을 유지해줍니다.
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """일반 비밀번호와 해시된 비밀번호가 일치하는지 확인합니다."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """일반 비밀번호를 해시하여 반환합니다."""
    return pwd_context.hash(password)