

def test_single_ns(statekeeper):
    db = statekeeper

    NS = 'ns'

    # make 21 entries, 0..20
    for i in range(21):
        s = db.write(NS, {'k': i})
        # ensure that just after write, .get() works
        assert db.get(NS, key='k') == i
        # ensure that just after write, .get() can see history
        assert db.get(NS, key='k', seqno=s) == i

    # ensure that long after write, .get() can see history
    for i in range(1, 21):
        v = db.get(NS, key='k', seqno=i)
        assert v == i-1

    # ensure that read_ns() works
    for s, d in db.read_ns(NS, 1, 'k'):
        assert d == s-1

    for s, d in db.read(1, key='k'):
        assert NS in d
        assert d[NS]['k'] == s-1


def test_multi_ns(statekeeper):
    db = statekeeper

    namespaces = ('ns1', 'ns2', 'ns3', 'ns2', 'ns1')

    for i, ns in enumerate(namespaces):
        for j in range(6):
            db.write(ns, {'k': i*10 + j})

    # make sure all the namespaces exist
    assert set(db.namespaces()) == set(namespaces)

    # make sure that changes in ns2/3 don't affect ns1
    # and that asking ns1 for seqnos that dont have changes in ns1 works
    ns1_middle = db.get('ns1', 'k', 7)
    for s in range(8, 20):
        assert db.get('ns1', 'k', s) == ns1_middle


