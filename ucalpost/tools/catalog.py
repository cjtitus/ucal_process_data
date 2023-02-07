from databroker.queries import TimeRange, In, Key
from abc import ABC, abstractmethod
from .utils import iterfy


class WrappedCatalogBase(ABC):

    @classmethod
    @property
    @abstractmethod
    def KEY_MAP(cls):
        raise NotImplementedError

    @classmethod
    def _filter_function_name(cls, search_key):
        fname = f"filter_by_{search_key}"
        return fname

    @classmethod
    def _list_function_name(cls, search_key):
        fname = f"list_{search_key}"
        return fname

    @classmethod
    def _make_filter_function(cls, search_key, catalog_key):

        fname = cls._filter_function_name(search_key)
        if not hasattr(cls, fname):
            def _inner(self, values):
                return self.filter_by_key(catalog_key, values)
            _inner.__name__ = fname
            setattr(cls, fname, _inner)

    @classmethod
    def _make_list_function(cls, list_key, catalog_key):
        fname = cls._list_function_name(list_key)
        if not hasattr(cls, fname):
            def _inner(self):
                return self.list_meta_key_vals(catalog_key)

            _inner.__name__ = fname
            setattr(cls, fname, _inner)

    def __init__(self, catalog):
        self._catalog = catalog

        for function_key, catalog_key in self.KEY_MAP.items():
            self.__class__._make_filter_function(function_key, catalog_key)
            self.__class__._make_list_function(function_key, catalog_key)

    def search(self, expr):
        return self.__class__(self._catalog.search(expr))

    def filter_by_key(self, key, values):
        return self.search(In(key, list(iterfy(values))))

    def _get_subcatalogs(self, **kwargs):
        subcatalogs = []
        for k in self.KEY_MAP:
            if kwargs.pop(k, False):
                list_function = getattr(self, self.__class__._list_function_name(k))
                filter_function = getattr(self, self.__class__._filter_function_name(k))
                for val in list_function():
                    catalog = filter_function(val)
                    subcatalogs += catalog._get_subcatalogs(**kwargs)
                return subcatalogs
        return [self]

    def get_subcatalogs(self, **kwargs):
        defaults = {k: True for k in self.KEY_MAP}
        defaults.update(kwargs)
        return self._get_subcatalogs(**defaults)

    def filter(self, **kwargs):
        for k in self.KEY_MAP:
            val = kwargs.pop(k, None)
            if val is not None:
                filter_function = getattr(self, self.__class__._filter_function_name(k))
                catalog = filter_function(val)
                return catalog.filter(**kwargs)
        return self
