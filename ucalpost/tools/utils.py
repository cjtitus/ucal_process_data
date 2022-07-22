import collections

def iterfy(x):
    if not isinstance(x, str) and isinstance(x, collections.abc.Iterable):
        return x
    else:
        return [x]
