"""测试配置：每个测试前重置 store 保证隔离。"""
import pytest
from app.store import store as _store


@pytest.fixture(autouse=True)
def fresh_store():
    _store.reset()
    yield

