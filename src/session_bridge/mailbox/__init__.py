"""fs mailbox + managed-mode watcher duty (item_13c)."""

from .core import (
    POLL_SECONDS,
    WATCHER_LOOP,
    consume,
    mailbox_dir,
    mailbox_duty_text,
    next_message,
    queue_root,
    watcher_loop_text,
    write_message,
)

__all__ = [
    "POLL_SECONDS",
    "WATCHER_LOOP",
    "consume",
    "mailbox_dir",
    "mailbox_duty_text",
    "next_message",
    "queue_root",
    "watcher_loop_text",
    "write_message",
]
