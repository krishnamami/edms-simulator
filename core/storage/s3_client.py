"""S3 document store with local-filesystem fallback for dev/test.

S3 path layout: loans/{application_id}/{category}/{document_id}.pdf
"""
import logging
import os
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.use_local = os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true"
        self.bucket = os.getenv("AWS_S3_BUCKET", "edms-simulator-dev")
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.local_path = Path(os.getenv("LOCAL_STORAGE_PATH", "./local_storage"))
        self._client = None
        if self.use_local:
            self.local_path.mkdir(parents=True, exist_ok=True)

    @property
    def client(self):
        if not self._client and not self.use_local:
            import boto3
            self._client = boto3.client("s3", region_name=self.region)
        return self._client

    def _key(self, application_id: str, category: str, document_id: str, extension: str = "pdf") -> str:
        return f"loans/{application_id}/{category}/{document_id}.{extension.lstrip('.')}"

    def upload_document(
        self,
        application_id: str,
        category: str,
        document_id: str,
        content: Union[bytes, str],
        extension: str = "pdf",
        content_type: str = "application/pdf",
    ) -> str:
        key = self._key(application_id, category, document_id, extension=extension)
        body = content.encode() if isinstance(content, str) else content

        if self.use_local:
            full_path = self.local_path / key
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_bytes(body)
            logger.info("doc_local_saved", extra={"key": key})
            return key

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ServerSideEncryption="aws:kms",
                ContentType=content_type,
            )
            logger.info("doc_s3_uploaded", extra={"bucket": self.bucket, "key": key})
            return key
        except Exception as e:
            logger.error("doc_s3_upload_failed", extra={"key": key, "error": str(e)})
            raise

    def get_presigned_url(self, s3_key: str, expires_in: int = 3600) -> str:
        if self.use_local:
            return f"file://{(self.local_path / s3_key).resolve()}"
        try:
            return self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": s3_key},
                ExpiresIn=expires_in,
            )
        except Exception as e:
            logger.error("presign_failed", extra={"key": s3_key, "error": str(e)})
            raise

    def document_exists(self, s3_key: str) -> bool:
        if self.use_local:
            return (self.local_path / s3_key).exists()
        try:
            self.client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False

    # ---------------- Phase A: raw inbound storage -----------------

    def store_raw(
        self,
        source_channel: str,
        content: bytes,
        filename: str | None = None,
        applicant_id: str | None = None,
    ) -> tuple[str, int]:
        """Store an inbound raw payload BEFORE any extraction runs.

        Key layout: ``raw/{channel}/{applicant_id?}/{YYYY/MM/DD}/{uuid}.{ext}``.
        Returns ``(s3_key, size_bytes)``.
        """
        import uuid as _uuid
        from datetime import datetime as _dt

        ext = self._infer_extension(content, filename)
        date_prefix = _dt.utcnow().strftime("%Y/%m/%d")
        file_id = str(_uuid.uuid4())
        applicant_prefix = f"{applicant_id}/" if applicant_id else ""
        key = (
            f"raw/{source_channel}/{applicant_prefix}{date_prefix}/{file_id}.{ext}"
        )
        size = len(content)

        if self.use_local:
            full_path = self.local_path / key
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_bytes(content)
            logger.info("raw_local_saved", extra={"key": key, "size": size})
            return key, size

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                ContentType=self._infer_mime(content, filename),
                ServerSideEncryption="aws:kms",
                Metadata={
                    "source_channel": source_channel,
                    "applicant_id": applicant_id or "",
                    "original_filename": filename or "",
                },
            )
            logger.info("raw_s3_uploaded", extra={"key": key, "size": size})
            return key, size
        except Exception as e:
            logger.error("raw_s3_upload_failed", extra={"key": key, "error": str(e)})
            raise

    def get_raw(self, s3_key: str) -> bytes:
        """Read the raw bytes back. Used by the reprocess path."""
        if self.use_local:
            return (self.local_path / s3_key).read_bytes()
        response = self.client.get_object(Bucket=self.bucket, Key=s3_key)
        return response["Body"].read()

    @staticmethod
    def _infer_extension(content: bytes, filename: str | None = None) -> str:
        if filename and "." in filename:
            return filename.rsplit(".", 1)[-1].lower()
        head = content[:8] if content else b""
        if head[:4] == b"%PDF":
            return "pdf"
        if head[:2] == b"\xff\xd8":
            return "jpg"
        if head[:8] == b"\x89PNG\r\n\x1a\n":
            return "png"
        if head[:4] == b"II*\x00" or head[:4] == b"MM\x00*":
            return "tiff"
        if head[:2] == b"PK":
            return "docx"
        if b"<?xml" in content[:100]:
            return "xml"
        if content[:1] in (b"{", b"["):
            return "json"
        return "bin"

    def _infer_mime(self, content: bytes, filename: str | None = None) -> str:
        ext = self._infer_extension(content, filename)
        return {
            "pdf":  "application/pdf",
            "jpg":  "image/jpeg",
            "png":  "image/png",
            "tiff": "image/tiff",
            "xml":  "application/xml",
            "json": "application/json",
            "csv":  "text/csv",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }.get(ext, "application/octet-stream")
