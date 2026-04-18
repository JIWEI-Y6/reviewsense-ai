"""Export a Claude Code session to a readable markdown file.

Usage:
    python export_chat.py                          # latest session
    python export_chat.py --session-id <uuid>      # specific session
    python export_chat.py --list                   # list available sessions
    python export_chat.py --output my_chat.md      # custom output path
"""

import argparse
import json
from datetime import datetime
from pathlib import Path


def find_project_folder(cwd: Path) -> Path:
    """Convert cwd path to the Claude Code project folder name."""
    cwd_str = str(cwd).replace("\\", "-").replace("/", "-").replace(":", "-")
    candidates = [
        cwd_str,
        cwd_str.lower(),
        cwd_str[0].lower() + cwd_str[1:],
        cwd_str[0].upper() + cwd_str[1:],
    ]
    projects_root = Path.home() / ".claude" / "projects"
    for cand in candidates:
        folder = projects_root / cand
        if folder.exists():
            return folder
    # Fallback: fuzzy match on last component
    last = cwd.name
    for folder in projects_root.iterdir():
        if folder.is_dir() and last in folder.name:
            return folder
    raise FileNotFoundError(f"No project folder found for {cwd}")


def find_latest_session(project_folder: Path) -> Path:
    """Return the most recently modified .jsonl file."""
    jsonls = list(project_folder.glob("*.jsonl"))
    if not jsonls:
        raise FileNotFoundError(f"No .jsonl files in {project_folder}")
    return max(jsonls, key=lambda p: p.stat().st_mtime)


def find_session_by_id(project_folder: Path, session_id: str) -> Path:
    """Return a specific .jsonl by session ID (full UUID or prefix)."""
    path = project_folder / f"{session_id}.jsonl"
    if path.exists():
        return path
    # Try prefix match
    matches = [p for p in project_folder.glob("*.jsonl") if p.stem.startswith(session_id)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        names = ", ".join(p.stem for p in matches)
        raise ValueError(f"Session ID '{session_id}' is ambiguous. Matches: {names}")
    raise FileNotFoundError(f"No session found with ID '{session_id}' in {project_folder}")


def list_sessions(project_folder: Path) -> None:
    """Print available sessions sorted by mtime (newest first)."""
    jsonls = sorted(
        project_folder.glob("*.jsonl"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not jsonls:
        print(f"No sessions found in {project_folder}")
        return

    print(f"Sessions in {project_folder}:\n")
    print(f"  {'MODIFIED':<20} {'SIZE':>10}  SESSION ID")
    print(f"  {'-'*20} {'-'*10}  {'-'*36}")
    for p in jsonls:
        mtime = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        size = f"{p.stat().st_size:,}"
        print(f"  {mtime:<20} {size:>10}  {p.stem}")


def truncate(text, max_len: int = 2000) -> str:
    if text is None:
        return ""
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"\n\n... [truncated {len(text) - max_len} chars]"


def format_tool_input(tool_input) -> str:
    try:
        return "```json\n" + json.dumps(tool_input, indent=2, default=str) + "\n```"
    except Exception:
        return f"```\n{tool_input}\n```"


def format_tool_result(content) -> str:
    if isinstance(content, str):
        return truncate(content)
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(truncate(block.get("text", "")))
                elif block.get("type") == "image":
                    parts.append("[image]")
                else:
                    parts.append(truncate(str(block)))
            else:
                parts.append(truncate(str(block)))
        return "\n".join(parts)
    return truncate(str(content))


def extract_message(entry: dict):
    """Return (role, markdown_content) or None to skip."""
    msg = entry.get("message")
    if not msg:
        return None

    role = msg.get("role")
    content = msg.get("content")

    if role not in ("user", "assistant"):
        return None

    if isinstance(content, str):
        text = content.strip()
        if not text:
            return None
        return role, text

    if not isinstance(content, list):
        return None

    parts = []
    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type")

        if btype == "text":
            text = block.get("text", "").strip()
            if text:
                parts.append(text)
        elif btype == "thinking":
            continue
        elif btype == "tool_use":
            tool_name = block.get("name", "unknown")
            tool_input = block.get("input", {})
            parts.append(f"**Tool call:** `{tool_name}`\n\n{format_tool_input(tool_input)}")
        elif btype == "tool_result":
            result_content = block.get("content", "")
            is_error = block.get("is_error", False)
            label = "Tool result (error)" if is_error else "Tool result"
            formatted = format_tool_result(result_content)
            parts.append(f"**{label}:**\n\n```\n{formatted}\n```")

    if not parts:
        return None

    return role, "\n\n".join(parts)


def export_session(jsonl_path: Path, output_path: Path) -> int:
    turns_written = 0
    with open(output_path, "w", encoding="utf-8") as out:
        out.write("# Chat Export\n\n")
        out.write(f"**Source:** `{jsonl_path.name}`\n\n")
        out.write("---\n\n")

        last_role = None
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                result = extract_message(entry)
                if result is None:
                    continue

                role, content = result

                if role != last_role:
                    out.write(f"## {'User' if role == 'user' else 'Assistant'}\n\n")
                    last_role = role

                out.write(content)
                out.write("\n\n")
                turns_written += 1

    return turns_written


def main():
    parser = argparse.ArgumentParser(description="Export a Claude Code session to Markdown.")
    parser.add_argument(
        "--session-id",
        help="Session ID (UUID or prefix). Defaults to the most recently modified session.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available sessions and exit.",
    )
    parser.add_argument(
        "--output",
        default="chat_export.md",
        help="Output markdown path (default: chat_export.md in cwd).",
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    project_folder = find_project_folder(cwd)

    if args.list:
        list_sessions(project_folder)
        return

    if args.session_id:
        session = find_session_by_id(project_folder, args.session_id)
        print(f"Session: {session.name} ({session.stat().st_size:,} bytes)")
    else:
        session = find_latest_session(project_folder)
        print(f"Latest session: {session.name} ({session.stat().st_size:,} bytes)")

    output = Path(args.output)
    if not output.is_absolute():
        output = cwd / output

    turns = export_session(session, output)
    size = output.stat().st_size
    print(f"\nExported {turns} turns to {output}")
    print(f"File size: {size:,} bytes ({size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
