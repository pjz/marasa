
import json
import logging

import pytest

from marasa import StateKeeper, EventLogSingle, EventLogMulti

# show debug-level logging
# TODO: figure out how to toggle this with a simple decorator
logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture
def statekeeper(tmpdir):
    yield StateKeeper(str(tmpdir), segment_size=5)

@pytest.fixture
def elmulti(tmpdir):
    yield EventLogMulti(str(tmpdir), json.dumps, json.loads, segment_size=5)

@pytest.fixture
def elsingle(tmpdir):
    yield EventLogSingle(str(tmpdir), json.dumps, json.loads, segment_size=5)



