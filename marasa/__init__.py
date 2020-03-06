
from pathlib import Path

import orjson as json

NOTFOUND = object()


class Marasa:

    def __init__(self, storage_dir, epoch_size=10000):
        """
        :storage_dir: is the directory to store log files in
        :epoch_size: is how many records to store per file; the default is 10000, so if average change size is 1KB, that's
        a 10MB file
        """
        self.dir = Path(storage_dir)
        if not self.dir.exists():
            self.dir.mkdir()
        self.epoch_size = epoch_size
        self._seq = 0
        self._state = {}

    @property
    def seq(self):
        """The sequence number.  Non-writable."""
        return self._seq

    def write(self, namespace, kvdict):
        """
        update the set of key/value pairs in kvdict in the namespace
        """
        self._seq += 1
        self._write(namespace, self._seq, kvdict)

    def multiwrite(self, ns_kvdict):
        """
        write to multiple namespaces
        :ns_kvdict: a dictionary of namespace to kvdicts to update
        """
        self._seq += 1
        for ns in ns_kvdict:
            self._write(ns, self._seq, ns_kvdict[ns])

    def read_val(self, namespace, key=None, seqno=None):
        """
        return the values from the specified namespace
        :key: only get the value of the specified key
        :seqno: get value at or before the specified sequence number.  If not specified, get the current value
        Empty namespaces are empty, missing keys are NOTFOUND
        """
        if seqno is None:
            return self._read_cur(namespace, key)
        return self._read_history(namespace, seqno, key)

    def namespaces(self):
        """
        return the set of existing namespaces
        """
        return self._state.keys()

    def _namespaces(self):
        """raw list of namespaces, from the filesystem"""
        return set( f.name.split('.', 1)[0] for f in self.dir.glob('*.*') if f.is_file() )

    def _segfiles(self, ns):
        return self.dir.glob(ns + '.*')

    def _segfile_for_seq(self, namespace, seq=None):
        """find the namespace file to open to get the state as of seqno=seq.  If seq is None, get the latest one."""
        seg = seq // self.epoch_size if seq is not None else None
        biggest = (-1, None)
        for f in self._segfiles(namespace):
            fileseg = int(f.name.split('.')[-1])
            if seg is not None:
                if seg == fileseg: return f.name
                if fileseg > seg: continue
            if fileseg > biggest[0]:
                biggest = (fileseg, f)
        return biggest[1]

    def _latest_segfile(self, ns):
        """latest file for the specified namespace"""
        return self._segfile_for_seq(ns, None)

    def _write(self, namespace, seqno, kvdict):
        """write to a single file
        """
        # figure out the file to write to
        seg = seqno // self.epoch_size
        segfile = Path(f'{namespace}.{seg:09}')
        if not segfile.exists():
            #  create it, and store a full snapshot in it
            mode = 'w'
            data = self._state.get(namespace, {}).copy()
        else:
            # append to it, only the changes
            mode = 'a'
            data = {}
        # apply the changes to what's to be stored
        data.update(kvdict)
        # write it out
        with segfile.open(mode) as f:
            dataline = str(seqno) + " " + json.dumps(data) + '\n'
            f.write(dataline)
        # update cache
        if namespace not in self._state:
            self._state[namespace] = {}
        self._state[namespace].update(kvdict)

    def _read_ns(self, namespace):
        """read a single namespace in full.  Return a tuple of the last seqno and the latest state"""
        state = {}
        seqno = 0
        with self._latest_segfile(namespace).open() as f:
            for seq, data in self._segfile_reader(f):
                seqno = seq
                state.update(data)
        return seqno, state

    def reload(self):
        latest = 0
        for ns in self.namespaces():
            seqno, state = self._read_ns(ns)
            latest = max(latest, seqno)
        return latest

    def _read_cur(self, namespace, key=None):
        if not self._state:
            # repop the cache by reading all namespaces
            last = self.reload()
            if self.seq and last != self.seq:
                raise IOError  # database inconsistent
        # get the value(s) from the cache
        value = self._state.get(namespace, {})
        if key is None:
            return value
        return value.get(key, NOTFOUND)

    @staticmethod
    def _segfile_reader(fh):
        for line in fh:
            seqno, jdata = line.split(' ', 1)
            yield seqno, json.loads(jdata)

    def _read_history(self, namespace, seqno, key):
        # see if we can cheat
        # TODO: track last-update of each subitem in ._state
        if seqno == self.seq:
            return self._read_cur(namespace, key)
        state = {}
        # read from a point in history
        with self._segfile_for_seq(namespace, seqno).open() as f:
            for seq, data in self._segfile_reader(f):
                if seq <= seqno:
                    state.update(data)
                else:
                    break
        if key is None:
            return state
        return state.get(key, NOTFOUND)

    def read_range(self, namespace, start_seqno):
        """
        return a generator that will return the initial state and then the changes
        """
        pass






class MultiWrite:

    def __init__(self, db):
        self.db = db
        self.ops = dict()

    def write(self, namespace, kvdict):
        changes = self.ops.get(namespace, {})
        changes.update(kvdict)
        self.ops[namespace] = changes
        return self

    def execute(self):
        self.db.multiwrite(self.ops)
