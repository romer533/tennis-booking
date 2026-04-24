class AltegioError(Exception):
    """Базовый класс всех ошибок altegio-модуля."""


class AltegioConfigError(AltegioError):
    """Ошибка конфигурации (env var, валидация). До любого сетевого вызова."""


class AltegioBusinessError(AltegioError):
    """4xx с понятной бизнес-классификацией от сервера.

    Engine реагирует по `code` ("slot_busy", "not_open", "unauthorized", ...).
    """

    def __init__(self, code: str, message: str, http_status: int) -> None:
        super().__init__(f"[{http_status}] {code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status


class AltegioTransportError(AltegioError):
    """5xx, network error, timeout, malformed transport. Engine может ретраить."""

    def __init__(self, cause: str) -> None:
        super().__init__(f"transport error: {cause}")
        self.cause = cause
