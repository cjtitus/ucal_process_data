import collections
import inspect
from functools import update_wrapper


def iterfy(x):
    """
    This function guarantees that a parameter passed will act like a list (or tuple) for the purposes of iteration,
    while treating a string as a single item in a list.

    Parameters
    ----------
    x : Any
        The input parameter to be iterfied.

    Returns
    -------
    Iterable
        The input parameter as an iterable.
    """
    if isinstance(x, collections.abc.Iterable) and not isinstance(x, (str, bytes)):
        return x
    else:
        return [x]


def adjust_signature(*omit_args):
    """
    A decorator factory that adjusts the signature of the decorated function.
    It omits specified arguments from the function's signature.

    Parameters
    ----------
    *omit_args : str
        Names of the arguments to be omitted from the function's signature.

    Returns
    -------
    function
        The decorated function with an adjusted signature.

    Example
    -------
    @adjust_signature('arg_to_omit')
    def func(arg_to_keep, arg_to_omit):
        pass
    """

    def decorator(func):
        sig = inspect.signature(func)
        new_params = [p for name, p in sig.parameters.items() if name not in omit_args]
        new_sig = sig.replace(parameters=new_params)
        func.__signature__ = new_sig
        return func

    return decorator


def merge_signatures(func):
    def decorator(wrapper):
        # Get the signatures of the two functions
        sig_func = inspect.signature(func)
        sig_wrapper = inspect.signature(wrapper)

        # Separate positional and keyword parameters from variadic keyword parameters
        func_params = list(sig_func.parameters.values())[1:]
        wrapper_params = [param for param in sig_wrapper.parameters.values() if param.kind != param.VAR_KEYWORD]
        wrapper_var_keyword_params = [param for param in sig_wrapper.parameters.values() if param.kind == param.VAR_KEYWORD]

        # Create a new parameters list that includes parameters from both functions
        
        new_params = wrapper_params + [param for param in func_params if param.name not in sig_wrapper.parameters] + wrapper_var_keyword_params

        # Create a new signature with the combined parameters
        new_sig = sig_wrapper.replace(parameters=new_params)

        # Update the signature of the wrapper function
        wrapper.__signature__ = new_sig

        # Update the docstring and other attributes of the wrapper function
        update_wrapper(wrapper, func)

        return wrapper
    return decorator
