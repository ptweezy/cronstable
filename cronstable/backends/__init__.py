"""Optional leadership backends that use Kubernetes and etcd lease stores.

Each backend implements :class:`cronstable.leadership.LeaseBackend`. Both
can use HTTP through the core ``aiohttp`` dependency, so neither requires
additional packages or gRPC and protobuf wheels.

* The Kubernetes backend manages a ``coordination.k8s.io/v1`` ``Lease``.
  ``cluster.kubernetes.clientLibrary`` selects the transport: ``auto``
  prefers the official client when it is installed and importable, then
  falls back to the built-in HTTP transport; ``library`` requires the
  official client; ``http`` selects the built-in transport.
* The etcd backend uses the v3 gRPC-gateway JSON/HTTP API directly. It has
  one transport and requires no optional client library.

:func:`cronstable.leadership.make_backend` imports a backend only when
``cluster.backend`` selects it.
"""

from cronstable.config import ConfigError

# transport kinds returned by select_transport.
TRANSPORT_HTTP = "http"
TRANSPORT_LIBRARY = "library"


def select_transport(
    client_library: str, native_available: bool, backend: str
) -> str:
    """Choose the transport for a lease backend (pure; unit-tested).

    ``client_library`` is the resolved ``cluster.<backend>.clientLibrary``
    setting, ``native_available`` whether the native client imported on this
    architecture.  ``auto`` prefers the native client when present; ``library``
    requires it (raising :class:`~cronstable.config.ConfigError` if absent);
    ``http`` always uses the hand-rolled transport.
    """
    if client_library == "http":
        return TRANSPORT_HTTP
    if client_library == "library":
        if not native_available:
            raise ConfigError(
                "cluster.{0}.clientLibrary is 'library' but the native client "
                "is not importable on this architecture; install the optional "
                "cronstable[{0}] extra, or use clientLibrary auto/http".format(
                    backend
                )
            )
        return TRANSPORT_LIBRARY
    # "auto": prefer the native client when present, else the HTTP fallback.
    return TRANSPORT_LIBRARY if native_available else TRANSPORT_HTTP
