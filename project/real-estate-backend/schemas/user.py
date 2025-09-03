from pydantic import BaseModel, EmailStr

# --- User Schemas ---
# 비유: 레스토랑 '회원가입 신청서' 와 '회원 카드'

class UserCreate(BaseModel):
    """회원가입 시 사용자가 제출하는 '신청서' 양식입니다. (입력용)"""
    email: EmailStr
    password: str

class UserInDB(BaseModel):
    """데이터베이스에 저장된 사용자 정보의 내부 처리용 모델입니다."""
    id: int
    email: EmailStr
    is_active: bool

    class Config:
        from_attributes = True

class UserResponse(BaseModel):
    """서버가 사용자 정보를 외부에 보여줄 때 사용하는 '회원 카드'입니다. (출력용)
    비밀번호처럼 민감한 정보는 제외됩니다.
    """
    id: int
    email: EmailStr
    is_active: bool

# --- Token Schemas ---
# 비유: 로그인 성공 후 받는 '자유이용권'

class Token(BaseModel):
    """로그인 성공 시 사용자에게 발급되는 '자유이용권'의 형식입니다. (출력용)"""
    access_token: str
    token_type: str

class TokenData(BaseModel):
    """'자유이용권' 안에 위변조 방지용으로 숨겨둔 사용자의 '이메일 정보'입니다."""
    email: EmailStr | None = None

class UserLogin(BaseModel):
    """로그인 시 사용자가 제출하는 '로그인 폼' 양식입니다.
    (참고: 현재는 Swagger UI 호환을 위해 Form 데이터로 직접 받으므로, 이 스키마는 직접 사용되지 않습니다.)
    """
    email: EmailStr
    password: str