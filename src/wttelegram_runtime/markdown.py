from html import escape
import re

import bleach
import markdown

ALLOWED_TAGS = [
    "a",
    "b",
    "blockquote",
    "code",
    "em",
    "i",
    "pre",
    "s",
    "strong",
    "u"
]

ALLOWED_ATTRIBUTES = {
    "a": ["href"]
}


def _normalize_html_blocks(value: str) -> str:
    normalized = value
    replacements = {
        "<p>": "",
        "</p>": "\n\n",
        "<br>": "\n",
        "<br/>": "\n",
        "<br />": "\n",
        "<ul>": "\n",
        "</ul>": "\n",
        "<ol>": "\n",
        "</ol>": "\n",
        "<li>": "• ",
        "</li>": "\n",
        "<h1>": "<strong>",
        "</h1>": "</strong>\n\n",
        "<h2>": "<strong>",
        "</h2>": "</strong>\n\n",
        "<h3>": "<strong>",
        "</h3>": "</strong>\n\n"
    }

    for source, target in replacements.items():
        normalized = normalized.replace(source, target)

    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def render_text(value: str, parse_mode: str) -> tuple[str, str | None]:
    text = str(value or "").strip()
    mode = str(parse_mode or "plain").strip().lower()

    if not text:
        return "", None

    if mode == "plain":
        return escape(text), "HTML"

    if mode == "html":
        cleaned = bleach.clean(
            _normalize_html_blocks(text),
            tags=ALLOWED_TAGS,
            attributes=ALLOWED_ATTRIBUTES,
            strip=True
        )
        return cleaned, "HTML"

    markdown_html = markdown.markdown(
        text,
        extensions=["extra", "sane_lists"]
    )
    cleaned = bleach.clean(
        _normalize_html_blocks(markdown_html),
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        strip=True
    )
    return cleaned, "HTML"

