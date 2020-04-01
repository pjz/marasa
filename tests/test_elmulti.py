
from collections import namedtuple

class MyTestClassA(dict): pass
class MyTestClassB(dict): pass


def test_single_put_get(elmulti):

    for n in range(1, 12):
        datum = MyTestClassA(data=n) if n % 3 == 0 else MyTestClassB(data=n)
        elmulti.put(datum, typestr='MyTestClassA')

    for n in range(1, 12):
        datum = elmulti.get(seqno=n)
        assert datum['data'] == n

    for n, datum in elmulti.read(1):
        assert datum['data'] == n


def test_multi_put_get(elmulti):

    for n in range(1, 12):
        datum = MyTestClassA(data=n) if n % 3 == 0 else MyTestClassB(data=n)
        elmulti.put(datum)

    for n in range(1, 12):
        datum = elmulti.get(seqno=n, msgtypes=[MyTestClassA])
        if n % 3 == 0:
            assert datum['data'] == n
        else:
            assert datum == elmulti.NOTFOUND, f"seqno {n} was incorrectly found!"

    for n, datum in elmulti.read(1, [MyTestClassA]):
        assert n % 3 == 0


