from dynastore.modules.db_config.platform_config_service import PluginConfig
from dynastore.extensions.tools.exposure_mixin import ExposableConfigMixin


class JoinsPluginConfig(ExposableConfigMixin, PluginConfig):
    """Service-exposure config for the OGC API - Joins extension."""
    # `enabled` inherited from ExposableConfigMixin — no further fields.
