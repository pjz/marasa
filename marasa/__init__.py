
import logging
from pathlib import Path
from typing import Union, Optional, Dict, Any

import orjson as json

NOTFOUND = object()


class Marasa:
    """
    Marasa stores data as a series of changes, written to what are essentially logfiles.
    Each logfile is segmented into at most :epoch_size: lines.
    Each line consists of a sequence number followed by a space followed by the json representation of the changes
    made.
    """

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

    def _segfile_for_seg(self, ns, seg) -> Path:
        """Segfile for the specified segment.  Note: may not exist"""
        return self.dir / f"{ns}.{seg:09}"

    def _segfile_for_seq(self, namespace: str, seq: Optional[int]=None) -> Optional[Path]:
        """
        find the namespace file to open to get the state as of seqno=seq.
        eg. the largest segfile under the segment specified by seq.
        If seq is None, get the latest one.
        If there is no such namespace, or it's empty at or before that seq, return None
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
        segfile = self._segfile_for_seg(namespace, seqno // self.epoch_size)
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
        state: Dict[str, Any] = dict()
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

    def read(self, namespace: str, start_seqno: int, key=None):
        """
        return a generator that will return the initial state and then the changes.
        of either the specified key or the whole namespace if key is None
        If the specified key doesn't exist at start_seqno, NOTFOUND will be returned
        """
        # get full initial state to send
        segfile = self._segfile_for_seq(namespace, start_seqno)
        if segfile is None:
            yield start_seqno, NOTFOUND
        else:
            state = {} if key is None else { key: NOTFOUND }
            sentfirst = False
            with segfile.open() as f:
                for seq, data in self._segfile_reader(f):

                    if key is None:
                        state.update(data)
                        if seq >= start_seqno:
                            yield seq, state
                    else:
                        if not sentfirst:
                            if seq > start_seqno:
                                yield start_seqno, state[key]
                                sentfirst = True
                            state[key] = data.get(key, NOTFOUND)
                        if key in data:
                            yield seq, data[key]
            if not sentfirst:
                yield start_seqno, state if key is None else state[key]

        curseg = ( start_seqno // self.epoch_size )
        # use a lambda for lastseg b/c self.seq could change while looping
        lastseg = lambda : self.seq // self.epoch_size
        while curseg < lastseg():
            curseg += 1
            segfile = self._segfile_for_seg(namespace, curseg)
            if not segfile.exists(): continue
            with segfile.open() as f:
                for seq, data in self._segfile_reader(f):
                    if key is None:
                        yield seq, data
                    elif key in data:
                        yield seq, data[key]









class MultiWrite:
    """
    A helper class for writing into multple namespaces simultaneously.
    Usage:

        w = MultiWrite(db)
        w.write('foo', {1:2})
        w.write('bar', {1:2})
        w.execute()

    though it also implements the Builder pattern so it ca be written as:

        MultiWrite(db).write('foo', {1:2}).write('bar', {1:2}).execute()

    """

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

