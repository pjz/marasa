

def test_simple(db):

    NS = 'name'

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


