import os


def is_automated_test() -> bool:
    return os.environ.get('IS_AUTOMATED_UNITTEST')
