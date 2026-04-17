import unittest

from app import app


class OperationLogPreviewTestCase(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()