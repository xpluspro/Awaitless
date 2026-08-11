TERMINAL_STATES = {"succeeded", "failed", "cancelled", "timed_out", "lost"}
# ``pending`` remains active for rows written by Awaitless <= 0.3. New work uses
# ``queued`` for both named concurrency queues and Slurm scheduler admission.
ACTIVE_STATES = {"pending", "queued", "starting", "running", "stalled"}

EXIT_CODES = {
    "succeeded": 0,
    "failed": 3,
    "timed_out": 4,
    "cancelled": 5,
    "lost": 6,
}

DEFAULT_TAIL_LINES = 200
DEFAULT_MAX_RETURN_BYTES = 65_536
DEFAULT_POLL_INTERVAL = 2.0
