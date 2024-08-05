from __future__ import annotations

import inspect
import pydoc
from typing import Any, Type

__all__ = ("Instatiator",)


class InitArgsGetter:
    """Infers __init__ parameters.

    Parameters
    ----------
    context : dict, str -> Any
        Context dictionary from which __init__ args will be inferred.
    """

    def __init__(self, context: dict[str, Any]):
        self.context = context

    def get(
        self, cls: Type[Any], context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Infers __init__ parameters for type `cls`.

        Parameters
        ----------
        cls : Type[Any]
            Class whose __init__ parameters will be obtained base on the context.

        context : dict, str -> Any
            Additional context args from which also infer __init__ args.

        Returns
        -------
        initargs : dict
        """
        init_signature = self._get_init_signature(cls)
        initargs = self._get_initargs(init_signature, context=context)
        return initargs

    def _get_init_signature(self, cls: Type[Any]) -> inspect.Signature:
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

    def _get_initargs(
        self, init_signature, context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Returns init kwargs inferred from context."""
        if context is None:
            context = {}

        context = {**self.context, **context}
        return {k: v for k, v in context.items() if k in init_signature.parameters}


class Instatiator:
    """Generic object instantiator.

    Parameters
    ----------
    context : dict, str -> Any
        Context dictionary from which __init__ args will be inferred.
    """

    def __init__(self, context: dict[str, Any]) -> None:
        self.context = context

        self._initargs_getter = InitArgsGetter(self.context)

    def instantiate(self, clspath: str, context: dict[str, Any] | None = None) -> Any:
        """Instantiates type located at `clspath`.

        Parameters
        ----------
        clspath : str
            Path, in dot notation format, of the class to instantiate.

        context : dict, str -> Any
            Additional context args from which also infer __init__ args.


        Returns
        -------
        object: Any
        """
        cls = pydoc.locate(clspath)
        initargs = self.get_initargs(cls, context)
        return cls(**initargs)

    def get_initargs(
        self, cls: Type[Any], context: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Infers __init__ parameters for type `cls`.

        Parameters
        ----------
        cls : Type[Any]
            Class whose __init__ parameters will be obtained based on the context.

        context : dict, str -> Any
            Additional context args from which also infer __init__ args.

        Returns
        -------
        initargs : dict, str -> Any
        """
        return self._initargs_getter.get(cls, context=context)
