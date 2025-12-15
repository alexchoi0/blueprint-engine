"""Utility functions for Blueprint."""

def reduce(fn, items, initial=None):
    """Reduce items to a single value using a function.

    Args:
        fn: Function taking (accumulator, item) and returning new accumulator
        items: Iterable of items
        initial: Initial accumulator value (uses first item if not provided)

    Returns:
        Final accumulated value
    """
    items_list = list(items)
    if initial == None:
        if len(items_list) == 0:
            fail("reduce() of empty sequence with no initial value")
        acc = items_list[0]
        items_list = items_list[1:]
    else:
        acc = initial
    for item in items_list:
        acc = fn(acc, item)
    return acc

def _log_info(message):
    """Log an info message to stdout."""
    __bp_stdout("[INFO]", message)

def _log_warn(message):
    """Log a warning message to stdout."""
    __bp_stdout("[WARN]", message)

def _log_error(message):
    """Log an error message to stderr."""
    __bp_stderr("[ERROR] " + str(message))

def _log_debug(message):
    """Log a debug message to stdout."""
    __bp_stdout("[DEBUG]", message)

log = struct(
    info = _log_info,
    warn = _log_warn,
    error = _log_error,
    debug = _log_debug,
)
