from io import BytesIO
from pathlib import Path
import unittest

from app import app


PNG_BYTES = (
    b'\x89PNG\r\n\x1a\n'
    b'\x00\x00\x00\rIHDR'
    b'\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00'
    b'\x90wS\xde'
    b'\x00\x00\x00\x0cIDATx\x9cc``\x00\x00\x00\x02\x00\x01'
    b'\x0b\xe7\x02\x9d'
    b'\x00\x00\x00\x00IEND\xaeB`\x82'
)


class OperationLogUploadTestCase(unittest.TestCase):
    def _upload_demo_image(self, client) -> dict[str, str]:
        response = client.post(
            '/operation-log/technical-summaries/upload-image',
            data={
                'alt_text': 'chart',
                'image': (BytesIO(PNG_BYTES), 'chart.png'),
            },
            content_type='multipart/form-data',
        )
        self.assertEqual(response.status_code, 200)
        return response.get_json()

    def test_upload_image_endpoint_returns_markdown_and_url(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        payload = self._upload_demo_image(client)
        self.assertIn('/operation-log/static/uploads/', payload['url'])
        self.assertIn('![chart](', payload['markdown'])

        relative_path = payload['url'].split('/operation-log/static/', 1)[1]
        uploaded_file = Path(app.root_path) / 'operation_log' / 'static' / relative_path
        if uploaded_file.exists():
            uploaded_file.unlink()

    def test_upload_image_endpoint_rejects_non_image_extension(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        response = client.post(
            '/operation-log/technical-summaries/upload-image',
            data={
                'image': (BytesIO(b'plain text'), 'note.txt'),
            },
            content_type='multipart/form-data',
        )

        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertIn('仅支持', payload['error'])

    def test_create_technical_summary_accepts_local_uploaded_cover(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        payload = self._upload_demo_image(client)
        response = client.post(
            '/operation-log/technical-summaries/new',
            data={
                'category': 'technical_summary',
                'title': 'With Local Cover',
                'cover_image_url': payload['url'],
                'action_summary': 'summary',
                'content': payload['markdown'],
                'status': 'published',
                'published_at': '2026-04-17T12:00',
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        detail = client.get(response.headers['Location'])
        html = detail.get_data(as_text=True)
        self.assertIn(payload['url'], html)

        relative_path = payload['url'].split('/operation-log/static/', 1)[1]
        uploaded_file = Path(app.root_path) / 'operation_log' / 'static' / relative_path
        if uploaded_file.exists():
            uploaded_file.unlink()


if __name__ == '__main__':
    unittest.main()