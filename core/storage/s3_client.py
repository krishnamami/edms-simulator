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
