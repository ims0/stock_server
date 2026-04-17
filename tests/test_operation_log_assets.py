from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from operation_log.assets import cleanup_orphaned_uploads, collect_referenced_upload_urls, extract_local_upload_urls, resolve_upload_path


class OperationLogAssetsTestCase(unittest.TestCase):
    def test_extract_local_upload_urls_finds_markdown_and_cover(self) -> None:
        urls = extract_local_upload_urls(
            "![a](/operation-log/static/uploads/202604/a.png)\ntext\n![b](/operation-log/static/uploads/202604/b.webp)",
            "/operation-log/static/uploads/202604/cover.jpg",
        )

        self.assertEqual(
            urls,
            {
                "/operation-log/static/uploads/202604/a.png",
                "/operation-log/static/uploads/202604/b.webp",
                "/operation-log/static/uploads/202604/cover.jpg",
            },
        )

    def test_cleanup_orphaned_uploads_only_removes_unreferenced_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            upload_root = Path(temp_dir)
            month_dir = upload_root / "202604"
            month_dir.mkdir(parents=True, exist_ok=True)
            kept = month_dir / "keep.png"
            removed = month_dir / "remove.png"
            kept.write_bytes(b"keep")
            removed.write_bytes(b"remove")

            deleted = cleanup_orphaned_uploads(
                upload_root,
                {
                    "/operation-log/static/uploads/202604/keep.png",
                    "/operation-log/static/uploads/202604/remove.png",
                },
                {"/operation-log/static/uploads/202604/keep.png"},
            )

            self.assertTrue(kept.exists())
            self.assertFalse(removed.exists())
            self.assertEqual(deleted, [removed])

    def test_collect_referenced_upload_urls_aggregates_logs(self) -> None:
        referenced = collect_referenced_upload_urls(
            [
                {
                    "content": "![img](/operation-log/static/uploads/202604/a.png)",
                    "cover_image_url": "",
                },
                {
                    "content": "no image",
                    "cover_image_url": "/operation-log/static/uploads/202604/cover.jpg",
                },
            ]
        )

        self.assertEqual(
            referenced,
            {
                "/operation-log/static/uploads/202604/a.png",
                "/operation-log/static/uploads/202604/cover.jpg",
            },
        )

    def test_resolve_upload_path_maps_local_url_to_file(self) -> None:
        upload_root = Path("/tmp/uploads")
        resolved = resolve_upload_path(upload_root, "/operation-log/static/uploads/202604/demo.png")
        self.assertEqual(resolved, Path("/tmp/uploads/202604/demo.png"))


if __name__ == "__main__":
    unittest.main()