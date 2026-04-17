from __future__ import annotations

from pathlib import Path
import re
from urllib.parse import urlparse

UPLOAD_URL_PREFIX = "/operation-log/static/uploads/"
LOCAL_UPLOAD_URL_PATTERN = re.compile(r"/operation-log/static/uploads/[A-Za-z0-9_./-]+")


def is_local_upload_url(url: str) -> bool:
    parsed = urlparse(url)
    candidate = parsed.path if parsed.path else url
    return candidate.startswith(UPLOAD_URL_PREFIX)


def extract_local_upload_urls(content: str, cover_image_url: str = "") -> set[str]:
    urls = set(LOCAL_UPLOAD_URL_PATTERN.findall(content or ""))
    if cover_image_url and is_local_upload_url(cover_image_url):
        urls.add(urlparse(cover_image_url).path or cover_image_url)
    return urls


def collect_referenced_upload_urls(logs: list[dict[str, object]]) -> set[str]:
    referenced: set[str] = set()
    for log in logs:
        referenced.update(
            extract_local_upload_urls(
                str(log.get("content") or ""),
                str(log.get("cover_image_url") or ""),
            )
        )
    return referenced


def resolve_upload_path(upload_root: Path, image_url: str) -> Path | None:
    parsed = urlparse(image_url)
    candidate = parsed.path if parsed.path else image_url
    if not is_local_upload_url(candidate):
        return None
    relative = candidate[len(UPLOAD_URL_PREFIX) :]
    if not relative:
        return None
    return upload_root / relative


def cleanup_orphaned_uploads(
    upload_root: Path,
    candidate_urls: set[str],
    referenced_urls: set[str],
) -> list[Path]:
    removed: list[Path] = []
    orphaned = {url for url in candidate_urls if url not in referenced_urls}
    for image_url in orphaned:
        file_path = resolve_upload_path(upload_root, image_url)
        if file_path is None or not file_path.exists():
            continue
        file_path.unlink()
        removed.append(file_path)

        parent = file_path.parent
        while parent != upload_root and parent.exists():
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent

    return removed