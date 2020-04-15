
from collections import namedtuple

class MyTestClassA(dict): pass
class MyTestClassB(dict): pass


def test_single_put_get(elmulti):

    for n in range(1, 12):
        datum = f"a{n}" if n % 3 == 0 else f"b{n}"
        elmulti.put('data', datum)

    for n in range(1, 12):
        datum = elmulti.get(seqno=n)
        assert int(datum[1:]) == n

    for n, datum in elmulti.read(1):
        assert int(datum[1:]) == n


def test_multi_put_get(elmulti):

    for n in range(1, 12):
        datum = f"a{n}" if n % 3 == 0 else f"b{n}"
        elmulti.put(datum[0], datum)

    for n in range(1, 12):
        datum = elmulti.get(seqno=n, tags=['a'])
        if n % 3 == 0:
            assert int(datum[1:]) == n
        else:
            assert datum == elmulti.NOTFOUND, f"seqno {n} was incorrectly found!"

    for n, datum in elmulti.read(1, ['a']):
        assert n % 3 == 0


