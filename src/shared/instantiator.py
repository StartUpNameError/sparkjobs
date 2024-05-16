from __future__ import annotations

import inspect
import pydoc
from types import SimpleNamespace
from typing import Any, Type

__all__ = ("Instatiator",)


class InitArgsInference:
    """Infers __init__ parameters.

    Parameters
    ----------
    context : Any
        Context object from which __init__ parameters values will be inferred.

    kwargs : key-word arguments
        Additional arguments from which to also infer __init__ parameters.
    """

    def __init__(self, context: Any, **kwargs):
        self.context = context
        self.kwargs = kwargs

    def infer(self, cls: Type[Any]) -> dict[str, Any]:
        """Infers __init__ parameters for ``cls``.

        Parameters
        ----------
        cls : Type[Any]
            Class whose __init__ parameters will be obtained base on ``obj``.

        Returns
        -------
        parameters : dict
        """
        init_signature = self.get_init_signature(cls)
        parameters = self._get_init_kwargs(init_signature)
        return parameters

    def get_init_signature(self, cls: Type[Any]) -> inspect.Signature:
        """Retrieves __init__ signature from ``cls``.

        Parameters
        ----------
        cls : class

        Returns
        -------
        Signature
        """
        init = getattr(cls.__init__, "deprecated_original", cls.__init__)
        return inspect.signature(init)

    def _get_init_kwargs(self, init_signature):
        """Returns init kwargs inferred from :attr:`context`."""
        context_dict = {**self.context.__dict__, **self.kwargs}
        return {
            k: v
            for k, v in context_dict.items()
            if k in init_signature.parameters
        }


class Instatiator:

    def __init__(self, context: Any) -> None:
        self.context = context

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Instatiator:
        return cls(context=SimpleNamespace(**d))

    def instantiate(self, clspath: str, **kwargs) -> Any:
        cls = pydoc.locate(clspath)
        initargs = self.get_initargs(cls, **kwargs)
        return cls(**initargs)

    def get_initargs(self, cls: Type, **kwargs) -> dict[str, Any]:
        return InitArgsInference(self.context, **kwargs).infer(cls)
