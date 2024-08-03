from typing import Any, Callable


def arg_callback(
    arg: str, func: Callable[[Any], Any], func_kw_args: dict | None = None
) -> Callable:
    """Preprocesses function argument `arg` using the specified callback function.

    Parameters
    ----------
    args : str
        Arg name.

    func : Callable
        Callback function.

    Example
    -------
    @arg_callback(arg="a", func=lambda x: x + 1)
    def foo(a: int) -> int:
        return a

    >>> foo(a=2)  # Returns 3
    """
    if func_kw_args is None:
        func_kw_args = {}

    def decorator(fn: Callable):

        def inner(*args, **kwargs):

            if arg not in kwargs:
                raise ValueError(
                    f"Cannot apply preprocessor since argument `{arg}` "
                    "was not given."
                )

            # Apply preprocessor prior to calling ``fn``.
            kwargs[arg] = func(kwargs[arg], **func_kw_args)

            return fn(*args, **kwargs)

        return inner

    return decorator
