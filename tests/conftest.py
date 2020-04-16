
import json
import logging

import pytest

from marasa import StateKeeper, MonoLog, MultiLog, SerializingMultiLog

# show debug-level logging
# TODO: figure out how to toggle this with a simple decorator
logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture
def statekeeper(tmpdir):
    yield StateKeeper(str(tmpdir), segment_size=5)

@pytest.fixture
def multilog(tmpdir):
    yield MultiLog(str(tmpdir), segment_size=5)

@pytest.fixture
def ser_multilog(tmpdir):
    yield SerializingMultiLog(str(tmpdir), json.dumps, json.loads, segment_size=5)

@pytest.fixture
def monolog(tmpdir):
    yield MonoLog(str(tmpdir), segment_size=5)
