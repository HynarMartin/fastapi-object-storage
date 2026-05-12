from sqlalchemy import String, Integer, DateTime, ForeignKey, Boolean, LargeBinary
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime, timezone
from database import Base
import uuid

class Bucket(Base):
    __tablename__ = "buckets"
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    current_storage_bytes: Mapped[int] = mapped_column(Integer, default=0)
    ingress_bytes: Mapped[int] = mapped_column(Integer, default=0)
    egress_bytes: Mapped[int] = mapped_column(Integer, default=0)
    internal_transfer_bytes: Mapped[int] = mapped_column(Integer, default=0)
    count_write_requests: Mapped[int] = mapped_column(Integer, default=0)
    count_read_requests: Mapped[int] = mapped_column(Integer, default=0)
    
    files: Mapped[list["FileMetadata"]] = relationship("FileMetadata", back_populates="bucket")

class FileMetadata(Base):
    __tablename__ = "files"
    id: Mapped[str] = mapped_column(String, primary_key=True, index=True)
    user_id: Mapped[str] = mapped_column(String, index=True)
    filename: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="uploading") 
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    
    bucket_id: Mapped[str] = mapped_column(String, ForeignKey("buckets.id"))
    bucket: Mapped["Bucket"] = relationship("Bucket", back_populates="files")
    
    # 1 soubor se nyní může rozkládat na X svazcích
    segments: Mapped[list["FileSegment"]] = relationship("FileSegment", back_populates="file", cascade="all, delete-orphan")

class FileSegment(Base):
    """Reprezentuje jeden fyzický kus souboru na disku (Chunking)"""
    __tablename__ = "file_segments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[str] = mapped_column(String, ForeignKey("files.id"), index=True)
    segment_index: Mapped[int] = mapped_column(Integer)
    
    volume_id: Mapped[int] = mapped_column(Integer)
    offset: Mapped[int] = mapped_column(Integer)
    size: Mapped[int] = mapped_column(Integer)
    
    file: Mapped["FileMetadata"] = relationship("FileMetadata", back_populates="segments")

class QueuedMessage(Base):
    __tablename__ = "queued_messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String, index=True)
    payload: Mapped[bytes] = mapped_column(LargeBinary) 
    is_binary: Mapped[bool] = mapped_column(Boolean, default=False)
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc))