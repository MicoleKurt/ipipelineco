import pytest

from pipelineco.config import load_config
from pipelineco.sources.generate import generate
from pipelineco.warehouse import open_warehouse


@pytest.fixture
def cfg(tmp_path):
    generate(tmp_path, tick=0)
    c = load_config(None, tmp_path, api_url="http://127.0.0.1:0")
    c.retry.delay_s = 0.0
    return c


@pytest.fixture
def wh(cfg):
    w = open_warehouse(cfg)
    yield w
    try:
        w.close()
    except Exception:
        pass
