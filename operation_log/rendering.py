from __future__ import annotations

from dataclasses import dataclass
from html import escape
import bleach
import markdown

ALLOWED_TAGS = list(bleach.sanitizer.ALLOWED_TAGS) + [
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "p",
    "pre",
    "code",
    "blockquote",
    "hr",
    "table",
    "thead",
    "tbody",
    "tr",
    "th",
    "td",
    "img",
]

ALLOWED_ATTRIBUTES = {
    "a": ["href", "title", "target", "rel"],
    "div": ["class"],
    "img": ["src", "alt", "title", "loading"],
    "th": ["align"],
    "td": ["align"],
    "h1": ["id"],
    "h2": ["id"],
    "h3": ["id"],
    "h4": ["id"],
    "h5": ["id"],
    "h6": ["id"],
}

ALLOWED_PROTOCOLS = list(bleach.sanitizer.ALLOWED_PROTOCOLS) + ["data"]


@dataclass(frozen=True)
class MarkdownRenderResult:
    html: str
    toc_html: str


def _sanitize_html(raw_html: str) -> str:
    return bleach.clean(
        raw_html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        protocols=ALLOWED_PROTOCOLS,
        strip=True,
    )


def _render_toc_items(items: list[dict[str, object]]) -> str:
    if not items:
        return ""

    parts: list[str] = ["<ul>"]
    for item in items:
        name = escape(str(item.get("name", "")))
        anchor = escape(str(item.get("id", "")), quote=True)
        parts.append(f'<li><a href="#{anchor}">{name}</a>')
        children = item.get("children")
        if isinstance(children, list) and children:
            parts.append(_render_toc_items(children))
        parts.append("</li>")
    parts.append("</ul>")
    return "".join(parts)


def render_markdown_document(source: str) -> MarkdownRenderResult:
    renderer = markdown.Markdown(
        extensions=["extra", "sane_lists", "smarty", "toc"],
        output_format="html5",
    )
    html = renderer.convert(source)
    toc_html = _render_toc_items(getattr(renderer, "toc_tokens", []))
    return MarkdownRenderResult(
        html=_sanitize_html(html),
        toc_html=_sanitize_html(toc_html),
    )