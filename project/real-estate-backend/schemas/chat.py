from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

# --- Message Schemas ---
# 비유: 채팅방의 '쪽지'

class MessageBase(BaseModel):
    """모든 '쪽지'에 공통적으로 들어가는 '내용'입니다."""
    content: str

class MessageCreate(MessageBase):
    """사용자가 '쪽지'를 새로 보낼 때 사용하는 '쪽지 작성 양식'입니다. (입력용)"""
    sender: str  # "user" or "ai"

class Message(BaseModel):
    """데이터베이스에 저장된 '쪽지'의 전체 정보가 담긴 '보관용 쪽지'입니다. (출력용)"""
    id: int
    chat_room_id: int
    sender: str
    content: str # MessageBase로부터 상속받음
    created_at: datetime

    class Config:
        from_attributes = True

# --- ChatRoom Schemas ---
# 비유: '대화가 이루어지는 방'

class ChatRoomBase(BaseModel):
    """모든 '대화방'에 공통적으로 들어가는 '이름표'입니다."""
    name: Optional[str] = None

class ChatRoomCreate(ChatRoomBase):
    """사용자가 '새로운 대화방 만들기'를 요청할 때 사용하는 '신청서'입니다. (입력용)"""
    pass

class ChatRoom(BaseModel):
    """데이터베이스에 저장된 '대화방'의 전체 정보가 담긴 '대화방 안내문'입니다. (출력용)
    이 안에는 대화방에 속한 모든 '쪽지' 목록도 포함될 수 있습니다.
    """
    id: int
    user_id: int
    name: Optional[str] = None # ChatRoomBase로부터 상속받음
    created_at: datetime
    messages: List[Message] = []

    class Config:
        from_attributes = True