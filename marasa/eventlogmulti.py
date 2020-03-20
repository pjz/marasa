
import logging
from pathlib import Path
from typing import Union, Optional, Dict, Any, List, Callable, TypeVar, Tuple

from .constants import NOTFOUND, NotFound

# Placeholder for the user's event type(s)
YourEventType = TypeVar('YourEventType')

# serialized data
Datum = Union[NotFound, str]

class EventLogMulti:
    """
    EventLogMulti stores a series of events, in a set of logfiles that are partitioned by type
    The logfiles are segmented so each has at most :segment_size: lines.
    Each line consists of a sequence number, a space, the type identifier, another space, and the serialized event
    """

    NOTFOUND = NOTFOUND

    def __init__(self, storage_dir: Union[Path, str],
                 serializer: Callable[[YourEventType], str],
                 deserializer: Callable[[str], YourEventType],
                 segment_size: int =10000):
        """
        :storage_dir: is the directory to store log files in
        :serializer: a single-argument function that can serialize any events handed to write().  IMPORTANT: the output
        must NOT contain newlines!
        :deserializer: a single-argument function that can deserialize the output of :serializer:
        :segment_size: is how many records to store per file; the default is 10000, so if average change size is 1KB, that's
        a 10MB file
        """
        self.dir = storage_dir if isinstance(storage_dir, Path) else Path(storage_dir)
        logging.debug(f"Making a {self.__class__.__name__}DB in %s", str(self.dir))
        if not self.dir.exists():
            self.dir.mkdir()
        self.segment_size = segment_size
        self.serialize = serializer
        self.deserialize = deserializer
        # self._cur is a dict of msgtype: (seqno, msg), so we can find most recent of any type easily
        self._cur: Dict[str, Tuple[int, Datum]] = dict()
        self._seq: int = 0
        self.reload()

    @property
    def seq(self) -> int:
        """The last sequence number used for a record.  Non-writable. Zero (0) means no records yet written."""
        return self._seq

    def put(self, event: YourEventType) -> int:
        """
        save the specified :event
        return the seqno it was saved at
        """
        self._seq += 1
        typestr = type(event).__name__
        self._write(self._seq, typestr, self.serialize(event))
        return self._seq

    def get(self, msgtypes: Optional[List[str]]=None, seqno: Optional[int]=None) -> Union[YourEventType, NotFound]:
        """
        fetch an event
        :msgtypes: limit the return types to one of these.  If unspecified, any will do
        :seqno: get value at or before the specified sequence number.  If unspecified, get the current value
        if no event matches, return NOTFOUND
        """
        if seqno is None:
            result = self._get_cur(msgtypes)
        elif seqno < 1:
            raise ValueError("Sequence numbers are never lower than 1")
        else:
            result = self._get_history(msgtypes, seqno)
        if isinstance(result, NotFound):
            return NOTFOUND
        return self.deserialize(result)

    @staticmethod
    def _segfile_reader(fh):
        for line in fh:
            seqno, typestr, jdata = line.split(' ', 2)
            #logging.debug("segfile returning %r %r %r", seqno, typestr, jdata)
            yield int(seqno), typestr, jdata

    def _segfiles(self, typestr=None):
        prefix = '*' if typestr is None else typestr
        return self.dir.glob('{prefix}.*')

    def _types(self):
        return set(f.name.rsplit('.', 1)[0] for f in self._segfiles())

    def _segfile_for_seqno(self, typestr: str, seq: Optional[int]=None) -> Optional[Path]:
        """
        find the segment file to open to get the state as of seqno=seq.
        eg. the largest segfile under the segment specified by seq.
        If seq is None, get the latest one.
        If there is no such typestr, or it's empty at or before that seq, return None
        """
        seg = seq // self.segment_size if seq is not None else None
        biggest = (-1, None)
        for f in self._segfiles(typestr):
            fileseg = int(f.name.split('.')[-1])
            if seg is not None:
                if seg == fileseg:
                    logging.debug("Segfile for seq %r (seg %r) is clearly %r", seq, seg, f)
                    return f
                if fileseg > seg: continue
            if fileseg > biggest[0]:
                biggest = (fileseg, f)
        return biggest[1]

    def _tail_typeseg(self, typestr: str):
        """Return a tuple of the last seqno and the latest data for the specified type"""
        seqno, last = 0, NOTFOUND
        segfile = self._segfile_for_seqno(typestr, None)
        if segfile is not None:
            with segfile.open() as f:
                *_, (seqno, _, last) = self._segfile_reader(f)
        return seqno, last

    def reload(self):
        latest = {}
        for t in self._types():
            seqno, data = self._tail_typeseg(t)
            if seqno > latest[t][0]:
                latest[t] = (seqno, data)
        self._cur = latest
        self._seq = max(latest[t][0] for t in latest)

    def _segfile_for_seg(self, typestr, seg) -> Path:
        """Segfile for the specified segment.  None if it doesn't exist."""
        return self.dir / f"{typestr}.{seg:09}"

    def _write(self, seqno: int, typestr, data):
        """write to a single file
        """
        # figure out the file to write to
        segfile = self._segfile_for_seg(typestr, seqno // self.segment_size)
        mode = 'a' if segfile.exists() else 'w'
        # write it out
        with segfile.open(mode) as f:
            dataline = f"{seqno!s} {typestr} {data}\n"
            f.write(dataline)
        self._cur[typestr] = (seqno, data)

    def _get_cur(self, msgtypes: Optional[List[str]]) -> Datum:
        if not self._cur:
            self.reload()
        if not self._cur:
            return NOTFOUND
        if msgtypes is None:
            which = max(self._cur.values(), key=lambda e: e[0])
        else:
            which = max((self._cur[t] for t in msgtypes), key=lambda e:e[0])
        return which[1]

    def _get_history(self, msgtypes: Optional[List[str]], seqno: int) -> Datum:
        if seqno == self.seq:
            return self._get_cur(msgtypes)
        logging.debug("looking in history of {msgtype}")
        result = NOTFOUND
        # read from a point in history
        for t in self._types():
            segfile = self._segfile_for_seqno(t, seqno)
            if segfile is None: continue
            with segfile.open() as f:
                for seq, _, data in self._segfile_reader(f):
                    if seq < seqno:
                        continue
                    elif seq == seqno:
                        return data
                    else:
                        break
        return NOTFOUND # if that seqno is missing

    def _read_type(self, start_seqno: int, typename: str):
        segfile = self._segfile_for_seqno(typename, start_seqno)
        # if nonexistant, send NOTFOUND
        if segfile is None:
            yield start_seqno, NOTFOUND
            return
        # find the spot in the first segfile and read the rest of it
        with segfile.open() as f:
            for seq, _, data in self._segfile_reader(f):
                if seq < start_seqno:
                    continue
                yield seq, self.deserialize(data)

        # now send subsequent segments
        curseg = ( start_seqno // self.segment_size )
        while curseg < self.seq // self.segment_size:
            curseg += 1
            segfile = self._segfile_for_seg(typename, curseg)
            if not segfile.exists(): continue
            with segfile.open() as f:
                for seq, _, data in self._segfile_reader(f):
                    yield seq, self.deserialize(data)


    def read(self, start_seqno: int, typenames=None):
        """
        return a generator that will return the initial event and all subsequent events
        of the specified type names (or all types if typenames is unspecified)
        If the specifie sequence number doesn't exist, NOTFOUND will be returned
        """

        def _existing_segfiles(typelist, segno):
            for t in typelist:
                segfile = self._segfile_for_seg(t, segno)
                if segfile.exists():
                    yield t, segfile

        types = self._types() if typenames is None else typenames
        curseg = ( start_seqno // self.segment_size )
        while curseg < self.seq // self.segment_size:
            cursors = { t: self._segfile_reader(f.open()) for t, f in _existing_segfiles(types, curseg) }
            latest = { t: next(cursors[t]) for t in cursors }
            while cursors:
                seq, t, data = min(latest.values(), key=lambda i: i[0])
                latest[t] = next(cursors[t], None)
                if latest[t] is None:
                    del latest[t]
                    del cursors[t]
                yield seq, self.deserialize(data)
            curseg += 1


Taimo = EventLogMulti




