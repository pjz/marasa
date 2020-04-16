
from collections import namedtuple


def test_single_put_get(multilog):

    for n in range(1, 12):
        datum = f"a{n}" if n % 3 == 0 else f"b{n}"
        multilog.put(datum, 'data')

    for n in range(1, 12):
        datum = multilog.get(seqno=n)
        assert int(datum[1:]) == n

    for n, _, datum in multilog.read(1):
        assert int(datum[1:]) == n


def test_multi_put_get(multilog):

    for n in range(1, 12):
        datum = f"a{n}" if n % 3 == 0 else f"b{n}"
        multilog.put(datum, datum[0])

    for n in range(1, 12):
        datum = multilog.get(seqno=n, tags=['a'])
        if n % 3 == 0:
            assert int(datum[1:]) == n
        else:
            assert datum == multilog.NOTFOUND, f"seqno {n} was incorrectly found!"

    for n, tag, datum in multilog.read(1, ['a']):
        assert n % 3 == 0
        assert tag == 'a'



class MyTestClassA(dict): pass
class MyTestClassB(dict): pass


def test_single_put_get_ser(ser_multilog):

    for n in range(1, 12):
        datum = MyTestClassA(data=n) if n % 3 == 0 else MyTestClassB(data=n)
        ser_multilog.put(datum, tag='MyTestClassA')

    for n in range(1, 12):
        datum = ser_multilog.get(seqno=n)
        assert datum['data'] == n

    for n, datum in ser_multilog.read(1):
        assert datum['data'] == n


def test_multi_put_get_ser(ser_multilog):

    for n in range(1, 12):
        datum = MyTestClassA(data=n) if n % 3 == 0 else MyTestClassB(data=n)
        ser_multilog.put(datum)

    for n in range(1, 12):
        datum = ser_multilog.get(seqno=n, tags=[MyTestClassA])
        if n % 3 == 0:
            assert datum['data'] == n
        else:
            assert datum == ser_multilog.NOTFOUND, f"seqno {n} was incorrectly found!"

    for n, datum in ser_multilog.read(1, [MyTestClassA]):
         assert n % 3 == 0


