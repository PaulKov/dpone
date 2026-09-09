from typing import Any


class DictConfigHandler:
    def __init__(self, params_kv: dict[str, Any], defaults_param_kv: dict[str, Any], p0_params: dict[str, Any] = None):
        self.p0_params = p0_params or {}
        self.params_kv = params_kv
        self.defaults_param_kv = defaults_param_kv

    def get_config_param(
        self,
        param_name: str,
    ) -> Any | None:
        """
        Извлекает параметр конфигурации по иерархии приоритетов:
        1. Значение из self.p0_params (Dict[str, Any], переданные при инициализации, high priority (0) params).
        2. Значение из self.params_kv.
        3. Значение из self.defaults_param_kv (глобальные настройки по умолчанию).

        Args:
            param_name: Имя параметра.

        Returns:
            Найденное значение параметра или None, если его нет ни на одном уровне.
        """
        if param_name in self.p0_params:
            return self.p0_params[param_name]

        if param_name in self.params_kv:
            return self.params_kv[param_name]

        return self.defaults_param_kv.get(param_name)
