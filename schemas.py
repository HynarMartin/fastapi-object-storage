from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional, Any

class BucketBase(BaseModel):
    name: str

class BucketCreate(BucketBase):
    pass

class BucketResponse(BucketBase):
    id: str
    created_at: datetime
    model_config = ConfigDict(from_attributes=True)

class BillingResponse(BaseModel):
    bucket_name: str
    current_storage_bytes: int
    ingress_bytes: int
    egress_bytes: int
    internal_transfer_bytes: int
    total_api_calls: int
    
    model_config = ConfigDict(from_attributes=True)

class FileMetadataResponse(BaseModel):
    id: str
    filename: str
    size: int
    user_id: str
    bucket_id: str
    created_at: datetime
    is_deleted: bool
    
    # NOVÉ POLOŽKY PRO HAYSTACK
    status: str
    volume_id: Optional[int] = None
    offset: Optional[int] = None
    
    model_config = ConfigDict(from_attributes=True)

class BrokerMessage(BaseModel):
    action: str  
    topic: Optional[str] = None
    message_id: Optional[int] = None
    payload: Optional[Any] = None

class MessageResponse(BaseModel):
    message: str

class ImageProcessRequest(BaseModel):
    operation: str
    params: dict = {}