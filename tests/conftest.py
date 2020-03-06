
import logging

import pytest

from marasa import Marasa

# show debug-level logging
# TODO: figure out how to toggle this with a simple decorator
logging.getLogger().setLevel(logging.DEBUG)


@pytest.fixture
def db(tmpdir):
    yield Marasa(str(tmpdir), epoch_size=5)


