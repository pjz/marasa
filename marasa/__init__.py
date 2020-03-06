
import logging
from pathlib import Path
from typing import Union, Optional, Dict, Any

import orjson as json

NOTFOUND = object()


class Marasa:

    def __init__(self, storage_dir: Union[Path, str], epoch_size=10000):
        """
        :storage_dir: is the directory to store log files in
        :epoch_size: is how many records to store per file; the default is 10000, so if average change size is 1KB, that's
        a 10MB file
        """
        self.dir = storage_dir if isinstance(storage_dir, Path) else Path(storage_dir)
        logging.debug("Making a MarasaDB in %s", str(self.dir))
        if not self.dir.exists():
            self.dir.mkdir()
        self.epoch_size = epoch_size
        self._state: Dict[str, Dict[str, Any]] = {}
        self._seq = self.reload()

    @property
    def seq(self):
        """The sequence number.  Non-writable."""
        return self._seq

    def write(self, namespace: str, kvdict):
        """
        update the set of key/value pairs in kvdict in the namespace
        return the seqno the update was applied in
        """
        self._seq += 1
        self._write(namespace, self._seq, kvdict)
        return self._seq

    def multiwrite(self, ns_kvdict):
        """
        write to multiple namespaces
        :ns_kvdict: a dictionary of namespace to kvdicts to update
        return the seqno the update was applied in
        """
        self._seq += 1
        for ns in ns_kvdict:
            self._write(ns, self._seq, ns_kvdict[ns])
        return self._seq

    def get(self, namespace: str, key: Optional[str]=None, seqno: Optional[int]=None):
        """
        return the values from the specified namespace
        :key: only get the value of the specified key
        :seqno: get value at or before the specified sequence number.  If not specified, get the current value
        Empty namespaces are empty, missing keys are NOTFOUND
        """
        if seqno is None:
            return self._read_cur(namespace, key)
        if seqno < 1:
            raise ValueError("Sequence numbers are never lower than 1")
        return self._read_history(namespace, key, seqno)

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

    def _segfile_for_seq(self, namespace: str, seq: Optional[int]=None) -> Optional[Path]:
        """
        find the namespace file to open to get the state as of seqno=seq.
        If seq is None, get the latest one.
        """
        seg = seq // self.epoch_size if seq is not None else None
        biggest = (-1, None)
        for f in self._segfiles(namespace):
            fileseg = int(f.name.split('.')[-1])
            if seg is not None:
                if seg == fileseg:
                    logging.debug("Segfile for ns %r seq %r (seg %r) is clearly %r", namespace, seq, seg, f)
                    return f
                if fileseg > seg: continue
            if fileseg > biggest[0]:
                biggest = (fileseg, f)
        return biggest[1]

    def _write(self, namespace: str, seqno: int, kvdict):
        """write to a single file
        """
        # figure out the file to write to
        seg = seqno // self.epoch_size
        segfile = self.dir / f'{namespace}.{seg:09}'
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
            dataline = str(seqno) + " " + json.dumps(data).decode('utf8') + '\n'
            f.write(dataline)
        # update cache
        if namespace not in self._state:
            self._state[namespace] = {}
        self._state[namespace].update(kvdict)

    def _read_ns(self, namespace: str):
        """Return a tuple of the last seqno and the latest state for the specified namespace"""
        state = {}
        seqno = 0
        segfile = self._segfile_for_seq(namespace, None)
        if segfile is not None:
            with segfile.open() as f:
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
            logging.debug("segfile returning %r %r", seqno, jdata)
            yield int(seqno), json.loads(jdata)

    def _read_history(self, namespace, key, seqno):
        # see if we can cheat
        # TODO: track last-update of each subitem in ._state
        if seqno == self.seq:
            return self._read_cur(namespace, key)
        logging.debug("looking in history")
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
        logging.debug("read historical state %r", state)
        return state.get(key, NOTFOUND)

    def read_range(self, namespace, start_seqno, key=None):
        """
        return a generator that will return the initial state and then the changes.
        of either the specified key or the whole namespace if key is None
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
