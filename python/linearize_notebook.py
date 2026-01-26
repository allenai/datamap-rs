#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
Linearize Jupyter notebook JSON content to markdown format.

Usage:
    zstdcat file.jsonl.zst | head -1 | jq '.text' -rc | uv run python/linearize_notebook.py
    zstdcat file.jsonl.zst | head -1 | jq '.text' -rc | uv run python/linearize_notebook.py --max-lines 30 --max-chars 80
"""

import argparse
import json
import sys
from typing import Any


def truncate_line(line: str, max_chars: int) -> str:
    """Truncate a line to max_chars, adding '...' if truncated."""
    if max_chars <= 0 or len(line) <= max_chars:
        return line
    return line[:max_chars - 3] + "..."


def truncate_lines(lines: list[str], max_lines: int) -> list[str]:
    """Truncate to max_lines, keeping first K/2 and last K/2."""
    if max_lines <= 0 or len(lines) <= max_lines:
        return lines

    keep_top = max_lines // 2
    keep_bottom = max_lines - keep_top
    truncated_count = len(lines) - max_lines

    result = lines[:keep_top]
    result.append(f"[additional {truncated_count} lines truncated]")
    result.extend(lines[-keep_bottom:])
    return result


def join_source(source: list[str] | str) -> str:
    """Join source lines, handling both list and string formats."""
    if isinstance(source, list):
        return "".join(source)
    return source


def format_outputs(
    outputs: list[dict[str, Any]], max_lines: int = 20, max_chars: int = 120
) -> str:
    """Format cell outputs as markdown with truncation."""
    if not outputs:
        return ""

    parts = []
    for output in outputs:
        output_type = output.get("output_type", "")

        if output_type == "stream":
            # stdout/stderr text output
            text = output.get("text", [])
            text_content = join_source(text)
            if text_content.strip():
                parts.append(text_content.rstrip("\n"))

        elif output_type == "execute_result":
            # Result from executing a cell
            data = output.get("data", {})
            if "text/plain" in data:
                text_content = join_source(data["text/plain"])
                if text_content.strip():
                    parts.append(text_content.rstrip("\n"))

        elif output_type == "display_data":
            # Display output (images, HTML, etc.)
            data = output.get("data", {})
            if "text/plain" in data:
                text_content = join_source(data["text/plain"])
                if text_content.strip():
                    parts.append(text_content.rstrip("\n"))

        elif output_type == "error":
            # Error traceback
            traceback = output.get("traceback", [])
            if traceback:
                # Remove ANSI escape codes for cleaner output
                import re

                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                text_content = "\n".join(
                    ansi_escape.sub("", line) for line in traceback
                )
                if text_content.strip():
                    parts.append(text_content.rstrip("\n"))

    if not parts:
        return ""

    output_text = "\n\n".join(parts)

    # Apply truncation
    lines = output_text.split("\n")
    lines = [truncate_line(line, max_chars) for line in lines]
    lines = truncate_lines(lines, max_lines)
    output_text = "\n".join(lines)

    return f"\n\n```output\n{output_text}\n```"


def linearize_notebook(
    notebook: dict[str, Any], max_lines: int = 20, max_chars: int = 120
) -> str:
    """
    Convert a Jupyter notebook to markdown format.

    - Markdown cells are output as-is
    - Code cells are wrapped in ```python fencing
    - Outputs are wrapped in ```output fencing (with truncation)
    """
    cells = notebook.get("cells", [])
    parts = []

    for cell in cells:
        cell_type = cell.get("cell_type", "")
        source = join_source(cell.get("source", []))

        if cell_type == "markdown":
            if source.strip():
                parts.append(source.rstrip("\n"))

        elif cell_type == "code":
            if source.strip():
                code_block = f"```python\n{source.rstrip()}\n```"
                outputs = format_outputs(cell.get("outputs", []), max_lines, max_chars)
                parts.append(code_block + outputs)

        elif cell_type == "raw":
            if source.strip():
                parts.append(f"```\n{source.rstrip()}\n```")

    return "\n\n".join(parts)


def main():
    """Read notebook JSON from stdin and output linearized markdown."""
    parser = argparse.ArgumentParser(
        description="Linearize Jupyter notebook JSON to markdown format."
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=20,
        help="Maximum lines per output cell (default: 20, 0 to disable)",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=120,
        help="Maximum characters per line in output (default: 120, 0 to disable)",
    )
    args = parser.parse_args()

    content = sys.stdin.read().strip()

    try:
        notebook = json.loads(content)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)

    markdown = linearize_notebook(notebook, args.max_lines, args.max_chars)
    print(markdown)


if __name__ == "__main__":
    main()
