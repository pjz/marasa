import re
import logging
from pathlib import Path
from typing import Union, Optional, Dict, List, TypeVar, Tuple, Iterable

from .constants import NOTFOUND, NotFound

# Placeholder for the user's event data
YourEventType = TypeVar('YourEventType')

# serialized data
Datum = Union[NotFound, str]

class MultiLog:
    """
    MultiLog stores a series of events, in a set of logfiles that are partitioned by tag
    The logfiles are segmented so each has at most :segment_size: lines.
    Each line consists of a sequence number, a space, the tag, another space, and the serialized event
    """

    NOTFOUND = NOTFOUND

    def __init__(self, storage_dir: Union[Path, str], segment_size: int = 10000):
        """
        :storage_dir: is the directory to store log files in
        :segment_size: is how many records to store per file; the default is 10000,
        so if average change size is 1KB, that's a 10MB file
        """
        self.__dir = storage_dir if isinstance(storage_dir, Path) else Path(storage_dir)
        logging.debug("Making a %sDB in %s", self.__class__.__name__, str(self.dir))
        if not self.dir.exists():
            self.dir.mkdir()
        self.segment_size = segment_size
        # self._cur is a dict of tag: (seqno, msg), so we can find most recent of any tag easily
        self._cur: Dict[str, Tuple[int, Datum]] = dict()
        self._seq: int = 0
        self.reload()

    @property
    def dir(self) -> Path:
        return self.__dir

    @property
    def seq(self) -> int:
        """
        The last sequence number used for a record.
        Non-writable. Zero (0) means no records yet written.
        """
        return self._seq

    def put(self, event, tag) -> int:
        """
        Save the specified :event under the specified tag.
        Return the seqno it was saved at.
        """
        seq = self._seq = self._seq + 1
        self._write(seq, tag, event)
        return seq

    def get(self, tags: Optional[List[str]] = None, seqno: Optional[int] = None) -> Union[YourEventType, NotFound]:
        """
        Fetch an event
        :tags: limit the events to those with one of these tags.  If unspecified, any will do.
        :seqno: get value at or before the specified sequence number.  If unspecified, get the current value
        if no event matches, return NOTFOUND
        """
        msgtags = self._tags() if tags is None else tags
        if seqno is None:
            result = self._get_cur(msgtags)
        elif seqno < 1:
            raise ValueError("Sequence numbers are never lower than 1")
        else:
            result = self._get_history(msgtags, seqno)
        if isinstance(result, NotFound):
            return NOTFOUND
        return result

    @staticmethod
    def _segfile_reader(fh):
        for line in fh:
            seqno, tag, jdata = line.split(' ', 2)
            #logging.debug("segfile returning %r %r %r", seqno, tag, jdata)
            yield int(seqno), tag, jdata

    def _segfiles(self, tag=None):
        prefix = '*' if tag is None else tag
        return self.dir.glob(f'{prefix}.*')

    def _tags(self):
        return set(f.name.rsplit('.', 1)[0] for f in self._segfiles())

    def _segfile_for_seqno(self, tag: str, seq: Optional[int]=None) -> Optional[Path]:
        """
        find the segment file to open to get the state as of seqno=seq.
        eg. the largest segfile under the segment specified by seq.
        If seq is None, get the latest one.
        If there is no such tag, or it's empty at or before that seq, return None
        """
        seg = seq // self.segment_size if seq is not None else None
        biggest = (-1, None)
        for f in self._segfiles(tag):
            fileseg = int(f.name.split('.')[-1])
            if seg is not None:
                if seg == fileseg:
                    logging.debug("Segfile for seq %r (seg %r) is clearly %r", seq, seg, f)
                    return f
                if fileseg > seg: continue
            if fileseg > biggest[0]:
                biggest = (fileseg, f)
        return biggest[1]

    def _tail_tagseg(self, tag: str):
        """Return a tuple of the last seqno and the latest data for the specified tag"""
        seqno, last = 0, NOTFOUND
        segfile = self._segfile_for_seqno(tag, None)
        if segfile is not None:
            with segfile.open() as f:
                *_, (seqno, _, last) = self._segfile_reader(f)
        return seqno, last

    def reload(self):
        latest = {}
        for t in self._tags():
            seqno, data = self._tail_tagseg(t)
            if seqno > latest[t][0]:
                latest[t] = (seqno, data)
        self._cur = latest
        self._seq = max(latest[t][0] for t in latest) if latest else 0

    def _segfile_for_seg(self, tag, seg) -> Path:
        """Segfile for the specified segment.  None if it doesn't exist."""
        return self.dir / f"{tag}.{seg:09}"

    def _write(self, seqno: int, tag: str, data):
        """write to a single file
        """
        # figure out the file to write to
        segfile = self._segfile_for_seg(tag, seqno // self.segment_size)
        mode = 'a' if segfile.exists() else 'w'
        # write it out
        with segfile.open(mode) as f:
            dataline = f"{seqno!s} {tag} {data}\n"
            f.write(dataline)
        logging.debug("wrote tag %r event %r as seqno %r", tag, data, seqno)
        self._cur[tag] = (seqno, data)

    def _get_cur(self, tags: Optional[List[str]]) -> Datum:
        if not self._cur:
            logging.debug(f"_cur unset, reloading")
            self.reload()
        if not self._cur:
            logging.debug(f"_cur unset after reload; empty db. NOTFOUND")
            return NOTFOUND
        if tags is None:
            which = max(self._cur.values(), key=lambda e: e[0])
            logging.debug(f"most recent of all _cur is {which})")
        else:
            which = max((self._cur[t] for t in tags), key=lambda e:e[0])
            logging.debug(f"most recent of allowed _cur is {which})")
        return which[1]

    def _get_history(self, tags: Optional[List[str]], seqno: int) -> Datum:
        msgtags = self._tags() if tags is None else tags
        logging.debug(f"looking in history of {tags!r} ({msgtags!r})")
        result = NOTFOUND
        # read from a point in history
        for t in msgtags:
            segfile = self._segfile_for_seqno(t, seqno)
            if segfile is None: continue
            logging.debug(f"history of tag {t!r} in segfile {segfile}")
            with segfile.open() as f:
                for seq, _, data in self._segfile_reader(f):
                    if seq < seqno:
                        continue
                    elif seq == seqno:
                        return data
                    else:
                        break
        return NOTFOUND # if that seqno is missing

    def read(self, start_seqno: int, tags: Optional[List[str]] = None) -> Iterable[Tuple[int, Union[YourEventType, NotFound]]]:
        """
        Return a generator that will return tuples (seqno, tag, data)
        If tags is a list, the events must have one of those tags.
        If tags is a string, it is treated as a regex that the tags must match.
        If tags is None or unspecified, all events are returned.
        If the specifie sequence number doesn't exist, NOTFOUND will be returned.
        """

        def _existing_segfiles(tags, segno):
            for tag in tags:
                segfile = self._segfile_for_seg(tag, segno)
                if segfile.exists():
                    yield tag, segfile
        if isinstance(tags, str):
            pattern = re.compile(tags)
            msgtags = [ tag for tag in self._tags() if pattern.fullmatch(tag) ]
        else:
            msgtags = self._tags() if tags is None else tags
        curseg = ( start_seqno // self.segment_size )
        while curseg < self.seq // self.segment_size:
            cursors = { tag: self._segfile_reader(f.open()) for tag, f in _existing_segfiles(msgtags, curseg) }
            latest = { k: v for k, v in  { tag: next(cursors[tag], None) for tag in cursors }.items() if v is not None }
            while cursors:
                logging.debug("segment %s: %r", curseg, latest)
                seq, tag, data = min(latest.values(), key=lambda i: i[0])
                latest[tag] = next(cursors[tag], None)
                if latest[tag] is None:
                    del latest[tag]
                    del cursors[tag]
                yield seq, tag, data
            curseg += 1


Taimo = MultiLog


class SerializingMultiLog(MultiLog):
    """
    SerializingMultiLog is a MultiLog that with put/get/read wrapped that do automatic serialization
    and deserialization for you.
    To this end, a couple things have changed:

        .put() now has 'tag' as an optional keyword argument - the default is event.__class__.__name__
        .read() now has an optional 'with_tags' keyword argument that determines whether the resultant
            generated tuples are (seqno, tag, event) or (seqno, event)
        .get() and .read() accept lists of objects as well as strings, and those objects' __name__s are
            used as the tag names to match
    """

    def __init__(self, storage_dir: Union[Path, str], serializer, deserializer, segment_size: int = 10000):
        """
        :storage_dir: is the directory to store log files in
        :segment_size: is how many records to store per file; the default is 10000,
        so if average change size is 1KB, that's a 10MB file
        """
        super().__init__(storage_dir, segment_size=segment_size)
        self.serialize = serializer
        self.deserialize = deserializer

    def put(self, event, tag=None) -> int:
        """
        Save the specified :event under the specified tag;
        if no tag is specified, use event.__class__.__name__.
        Return the seqno it was saved at.
        """
        if tag is None: tag = event.__class__.__name__
        data = self.serialize(event)
        return super().put(data, tag)

    @staticmethod
    def _xlate_tags(taglist):
        """
        translate list of strings or list of classes or mixed to a list of strings
        """
        if taglist is None:
            return None
        msgtags = []
        for tag in taglist:
            if isinstance(tag, str):
                msgtags.append(tag)
            else:
                msgtags.append(tag.__name__)
        return msgtags

    def get(self, tags: Optional[List[str]] = None, seqno: Optional[int] = None) -> Union[YourEventType, NotFound]:
        """
        Fetch an event
        :tags: limit the events to those with one of these tags.  If unspecified, any will do.
        If the list is not of strings, the tags will be their .__name__s (so passing in classes will work)
        :seqno: get value at or before the specified sequence number.  If unspecified, get the current value
        if no event matches, return NOTFOUND
        """
        msgtags = self._xlate_tags(tags)
        result = super().get(tags=msgtags, seqno=seqno)
        if result == NOTFOUND:
            return result
        return self.deserialize(result)

    def read(self, start_seqno: int, tags: Optional[List[str]] = None, with_tags: bool = False) -> Iterable[Tuple[int, Union[YourEventType, NotFound]]]:
        msgtags = self._xlate_tags(tags)
        for seq, tag, data in super().read(start_seqno, tags=tags):
            if with_tags:
                yield seq, tag, self.deserialize(data)
            else:
                yield seq, self.deserialize(data)

