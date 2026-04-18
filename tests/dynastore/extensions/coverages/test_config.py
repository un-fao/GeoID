from dynastore.extensions.coverages.config import CoveragesConfig


def test_defaults():
    cfg = CoveragesConfig()
    assert cfg.max_bands_per_request == 16
    assert cfg.request_deadline_soft_s == 60
    assert cfg.request_deadline_hard_s == 120
    assert cfg.default_block_size == 512
    assert cfg.default_style_id is None


def test_override_default_style_id():
    cfg = CoveragesConfig(default_style_id="ndvi")
    assert cfg.default_style_id == "ndvi"
