import time


DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF = 1


def retry_call(
    func,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff: float = DEFAULT_BACKOFF,
):
    """
    Execute a function with bounded exponential retries.

    Delays:
        attempt 1 -> no delay
        attempt 2 -> 1x backoff
        attempt 3 -> 2x backoff

    Raises the final exception if all attempts fail.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    last_exception = None

    for attempt in range(max_attempts):
        try:
            return func()

        except Exception as exc:
            last_exception = exc

            if attempt == max_attempts - 1:
                raise

            delay = backoff * (2 ** attempt)
            time.sleep(delay)

    raise last_exception