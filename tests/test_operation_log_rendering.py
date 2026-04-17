import unittest

from operation_log.rendering import render_markdown_document


class OperationLogRenderingTestCase(unittest.TestCase):
    def test_render_markdown_document_supports_rich_blocks(self) -> None:
        rendered = render_markdown_document(
            """
# Architecture Notes

Paragraph text.

| Name | Value |
| ---- | ----- |
| MA5  | On    |

![chart](https://example.com/chart.png)
            """.strip()
        )

        self.assertIn('<h1 id="architecture-notes">Architecture Notes</h1>', rendered.html)
        self.assertIn("<table>", rendered.html)
        self.assertIn('src="https://example.com/chart.png"', rendered.html)
        self.assertIn('href="#architecture-notes"', rendered.toc_html)

    def test_render_markdown_document_strips_script_tags(self) -> None:
        rendered = render_markdown_document("Hello<script>alert('x')</script>World")

        self.assertNotIn("<script>", rendered.html)
        self.assertIn("Hello", rendered.html)
        self.assertIn("World", rendered.html)
        self.assertIn("alert", rendered.html)


if __name__ == "__main__":
    unittest.main()