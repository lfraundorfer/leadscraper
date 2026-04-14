from __future__ import annotations

import unittest

from crm_mailer import _parse_draft, _render_html_body
from crm_templates import compose_email_draft, parse_email_draft


class EmailDraftFormattingTests(unittest.TestCase):
    def test_compose_and_parse_preserve_leading_whitespace(self) -> None:
        body = "                    Schau.\n\t\t\tHier.\n\t\tHin."

        draft = compose_email_draft("Spacing", body)
        subject, parsed_body = parse_email_draft(draft)

        self.assertEqual(subject, "Spacing")
        self.assertEqual(parsed_body, body)

    def test_mailer_parse_preserves_indented_first_line(self) -> None:
        draft = "Betreff: Spacing\n\n        Erste Zeile\n  Zweite Zeile"

        subject, body = _parse_draft(draft)

        self.assertEqual(subject, "Spacing")
        self.assertEqual(body, "        Erste Zeile\n  Zweite Zeile")

    def test_html_render_preserves_spacing_and_links(self) -> None:
        html = _render_html_body("        Schau.\nHier: https://example.com")

        self.assertIn("white-space: pre-wrap", html)
        self.assertIn("        Schau.", html)
        self.assertIn('<a href="https://example.com">https://example.com</a>', html)


if __name__ == "__main__":
    unittest.main()
