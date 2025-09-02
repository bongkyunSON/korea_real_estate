from pydantic import BaseModel, EmailStr
# 사용자 생성을 위한 입력 스키마 (API Body)
class UserCreate(BaseModel):
    email: EmailStr
    password: str

# 데이터베이스에서 읽어온 사용자 정보를 위한 스키마
class UserInDB(BaseModel):
    id: int
    email: EmailStr
    is_active: bool

    class Config:
        from_attributes = True # SQLAlchemy 모델 객체를 Pydantic 모델로 자동 변환

# API 응답으로 사용될 사용자 정보 스키마 (비밀번호 제외)
class UserResponse(BaseModel):
    id: int
    email: EmailStr
    is_active: bool