import re
import logging
import contextlib
from pathlib import Path
from typing import Union, Optional, TypeVar, Iterable, Tuple

from .constants import NotFound, NOTFOUND

YourEventType = TypeVar('YourEventType')

Datum = Union[NotFound, str]

class MonoLog:
    """
    MonoLog stores a series of events, written to a single logfile.
    The logfile is segmented into pieces with at most :segment_size: lines.
    Each line consists of a sequence number followed by a space followed by the json representation of the changes
    made.
    """

    def __init__(self, storage_dir: Union[Path, str], basename: str = 'log', segment_size: int = 10000, lock = None):
        """
        :storage_dir: is the directory to store log files in
        :basename: the name prefix events are stored in under storage_dir ('log' by default)
        :segment_size: is how many records to store per file; the default is 10000,
        so if average change size is 1KB, that's a 10MB file
        """
        self.dir = storage_dir if isinstance(storage_dir, Path) else Path(storage_dir)
        self.name = basename
        logging.debug(f"Making a {self.__class__.__name__}DB in %s", str(self.dir))
        if not self.dir.exists():
            self.dir.mkdir()
        self.segment_size = segment_size
        self._cur: Datum = NOTFOUND
        self._seq: int = 0
        self.writelock = contextlib.nullcontext() if lock is None else lock
        self.reload()

    @property
    def seq(self) -> int:
        """The sequence number.  Non-writable."""
        return self._seq

    def put(self, event: YourEventType) -> int:
        """
        save the specified event
        return the seqno it was saved at
        """
        with self.writelock:
            self._seq += 1
            self._write(self._seq, event)
            return self._seq

    def get(self, seqno: Optional[int]=None) -> YourEventType:
        """
        return the event at the specified
        :seqno: get value at or before the specified sequence number.  If not specified, get the current value.
        """
        if seqno is None:
            result = self._read_cur()
        elif seqno < 1:
            raise ValueError("Sequence numbers are never lower than 1")
        else:
            result = self._read_history(seqno)
        return result

    def _segfiles(self):
        return self.dir.glob('{self.name}.*')

    def _segfile_for_seg(self, seg) -> Path:
        """Segfile for the specified segment.  Note: may not exist"""
        return self.dir / f"{self.name}.{seg:09}"

    def _segfile_for_seq(self, seq: Optional[int]=None) -> Optional[Path]:
        """
        find the namespace file to open to get the state as of seqno=seq.
        eg. the largest segfile under the segment specified by seq.
        If seq is None, get the latest one.
        Returns None if there is no such namespace, or it's empty at and before that seq
        """
        seg = seq // self.segment_size if seq is not None else None
        biggest = (-1, None)
        for f in self._segfiles():
            fileseg = int(f.name.split('.')[-1])
            if seg is not None:
                if seg == fileseg:
                    logging.debug("Segfile for seq %r (seg %r) is clearly %r", seq, seg, f)
                    return f
                if fileseg > seg: continue
            if fileseg > biggest[0]:
                biggest = (fileseg, f)
        return biggest[1]

    def _write(self, seqno: int, data: str):
        """write to a single file
        """
        # figure out the file to write to
        segfile = self._segfile_for_seg(seqno // self.segment_size)
        mode = 'a' if segfile.exists() else 'w'
        # write it out
        with segfile.open(mode) as f:
            dataline = f"{seqno!s} {data}\n"
            f.write(dataline)
        self._cur = data

    def reload(self):
        latest = 0
        cur: Datum = NOTFOUND
        segfile = self._segfile_for_seq()
        if segfile is None:
            return 0
        for seq, _, data in self._segfile_reader(segfile):
            latest, cur = seq, data
        self._cur = cur
        self._seq = latest

    def _read_cur(self):
        if self._cur == NOTFOUND:
            # repop the cache by reading all namespaces
            self.reload()
        return self._cur

    @staticmethod
    def _segfile_reader(fh):
        for line in fh:
            seqno, data = line.split(' ', 1)
            logging.debug("segfile returning %r %r", seqno, data)
            yield int(seqno), data

    def _read_history(self, seqno):
        if seqno == self.seq:
            return self._read_cur()
        logging.debug("looking in history")
        result = NOTFOUND
        # read from a point in history
        segfile = self._segfile_for_seq(seqno)
        if segfile is None:
            return NOTFOUND
        with segfile.open() as f:
            for seq, _, data in self._segfile_reader(f):
                if seq == seqno:
                    return data
                elif seq > seqno:
                    break

    def read(self, start_seqno: int) -> Iterable[Tuple[int, Union[YourEventType, NotFound]]]:
        """
        Return a generator that will return (seqence number, event) tuples for the
        initial event and all subsequent events
        If the specified sequence number doesn't exist, NOTFOUND will be returned
        """

        segfile = self._segfile_for_seq(start_seqno)
        # if nonexistant, send NOTFOUND
        if segfile is None:
            yield start_seqno, NOTFOUND
            return

        # send the partial segment the staring seqno is in
        with segfile.open() as f:
            for seq, data in self._segfile_reader(f):
                if seq < start_seqno:
                    continue
                yield seq, data

        # now send subsequent segments
        curseg = ( start_seqno // self.segment_size )
        ## use a lambda for lastseg b/c self.seq could change while looping
        lastseg = lambda : self.seq // self.segment_size
        while curseg < lastseg():
            curseg += 1
            segfile = self._segfile_for_seg(curseg)
            if not segfile.exists(): continue
            with segfile.open() as f:
                for seq, data in self._segfile_reader(f):
                    yield seq, data





