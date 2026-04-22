import unittest
import sqlite3
from pathlib import Path

from app import app


class OperationLogPreviewTestCase(unittest.TestCase):
    def test_new_technical_summary_hides_manual_published_at_input(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        response = client.get('/operation-log/technical-summaries/new')

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertNotIn('type="datetime-local" name="published_at"', html)
        self.assertIn('value="保存后自动填充"', html)

    def test_operation_log_root_redirects_to_technical_summaries(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        response = client.get('/operation-log/', follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers['Location'].endswith('/operation-log/technical-summaries'))

    def test_preview_endpoint_returns_rendered_markdown_and_toc(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        response = client.post(
            '/operation-log/technical-summaries/preview',
            data={
                'content': '# Overview\n\n## Details\n\n| Name | Value |\n| ---- | ----- |\n| MA5 | On |',
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertIn('<h1 id="overview">Overview</h1>', payload['html'])
        self.assertIn('<table>', payload['html'])
        self.assertIn('href="#overview"', payload['toc_html'])

    def test_published_technical_summary_auto_fills_published_at(self) -> None:
        client = app.test_client()
        with client.session_transaction() as sess:
            sess['username'] = 'root'

        response = client.post(
            '/operation-log/technical-summaries/new',
            data={
                'category': 'technical_summary',
                'title': 'Auto publish time',
                'action_summary': 'summary',
                'content': '# Body',
                'status': 'published',
            },
            follow_redirects=False,
        )

        self.assertEqual(response.status_code, 302)
        log_id = int(response.headers['Location'].rstrip('/').split('/')[-1])
        db_path = Path(app.root_path) / 'data' / 'operation_logs.db'
        with sqlite3.connect(db_path) as conn:
            row = conn.execute('SELECT published_at FROM operation_logs WHERE id = ?', (log_id,)).fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(row[0])


if __name__ == '__main__':
    unittest.main()