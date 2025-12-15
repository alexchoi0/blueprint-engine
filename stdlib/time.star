"""Time utilities for Blueprint."""

def sleep(seconds):
    """Sleep for a specified duration.

    Args:
        seconds: Number of seconds to sleep (can be fractional, e.g., 0.5 for 500ms)

    Returns:
        None after the sleep completes
    """
    return __bp_sleep(seconds)

def now():
    """Get the current Unix timestamp.

    Returns:
        Current time as a float (seconds since epoch)
    """
    return __bp_now()
