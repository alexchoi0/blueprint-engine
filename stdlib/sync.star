"""Synchronization primitives for Blueprint."""

def race(ops):
    """Return the first operation to complete.

    Args:
        ops: List of operations to race

    Returns:
        Result of the first operation to complete
    """
    return take(1, ops)[0] if len(ops) > 0 else None

def take(n, ops):
    """Get the first N operations to complete.

    Args:
        n: Number of operations to wait for
        ops: List of operations

    Returns:
        List of the first N results (by completion order)
    """
    if n <= 0:
        return []
    if n >= len(ops):
        return __bp_gather(ops)
    return __bp_take(n, ops)

def after(dependency, op):
    """Execute an operation only after a dependency completes.

    Creates an explicit dependency to force sequential execution
    of operations that would otherwise run in parallel.

    Args:
        dependency: Operation that must complete first
        op: Operation to execute after dependency

    Returns:
        Result of op (after dependency completes)

    Example:
        a = write_file("/tmp/data.txt", content)
        b = after(a, read_file("/tmp/data.txt"))
    """
    return __bp_after(dependency, op)

def pipeline(fns, items):
    """Process items through a pipeline of functions.

    Each function receives items and transforms them for the next stage.

    Args:
        fns: List of functions, each taking an item and returning a result
        items: Initial list of items to process

    Returns:
        Final results after all pipeline stages

    Example:
        results = pipeline([fetch, parse, validate], urls)
    """
    current = items
    for fn in fns:
        current = [fn(item) for item in current]
    return __bp_gather(current)
