
# replay me these message types from this point in time, in order [and maybe continue in real time]
# get me the latest of this message type

class EventLogSingle:
    """
    EventLog stores a series of events, written to what is essentially a single logfile.
    The logfile is segmented into at most :epoch_size: lines.
    Each line consists of a sequence number followed by a space followed by the json representation of the changes
    made.
    """

    def __init__(self, storage_dir: Union[Path, str], basename: str, serializer, deserializer, epoch_size=10000):
        """
        :storage_dir: is the directory to store log files in
        :basename: the name prefix events are stored in under storage_dir
        :serializer: a single-argument function that can serialize any events handed to write().  IMPORTANT: the output
        must NOT contain newlines!
        :deserializer: a single-argument function that can deserialize the output of :serializer:
        :epoch_size: is how many records to store per file; the default is 10000, so if average change size is 1KB, that's
        a 10MB file
        """
        self.dir = storage_dir if isinstance(storage_dir, Path) else Path(storage_dir)
        self.name = name
        logging.debug("Making a MarasaDB in %s", str(self.dir))
        if not self.dir.exists():
            self.dir.mkdir()
        self.epoch_size = epoch_size
        self._seq = self.reload()
        self.serialize = serializer
        self.deserialize = deserializer
        self._cur = NOTFOUND

    @property
    def seq(self):
        """The sequence number.  Non-writable."""
        return self._seq

    def put(self, event):
        """
        save the specified event
        return the seqno it was saved at
        """
        self._seq += 1
        self._write(self._seq, type(event).__name__, self.serialize(event))
        return self._seq

    def get(self, seqno: Optional[int]=None):
        """
        return the event at the specified
        :key: only get the value of the specified key
        :seqno: get value at or before the specified sequence number.  If not specified, get the current value
        Empty namespaces are empty, missing keys are NOTFOUND
        """
        result = NOTFOUND
        if seqno is None:
            result = self._read_cur()
        elif seqno < 1:
            raise ValueError("Sequence numbers are never lower than 1")
        else:
            result = self._read_history(seqno)
        return self.deserialize(result)

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
        If there is no such namespace, or it's empty at or before that seq, return None
        """
        seg = seq // self.epoch_size if seq is not None else None
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

    def _write(self, seqno: int, typestr, data):
        """write to a single file
        """
        # figure out the file to write to
        segfile = self._segfile_for_seg(seqno // self.epoch_size)
        mode = 'a' if segfile.exists() else 'w'
        # write it out
        with segfile.open(mode) as f:
            dataline = f"{seqno!s} {typestr} {data}\n"
            f.write(dataline)
        self._cur = data

    def reindex(self):
        index = []
        for sf in self._segfiles():
            with sf.open() as f:
                first, typestr = None, None
                for seqno, tstr, data in self._segfile_reader(f):
                    if first is None: first = seqno
                    if typestr is None: typestr = tstr
                index.append((first, typestr, f.name))
        self._indexes = sorted(index, key=lambda i: i[0])

    def reload(self):
        latest = 0
        with self._segfile_for_seq() as f:
            seqno, state = self._read_ns(ns)
            latest = max(latest, seqno)
        return latest

    def _read_cur(self):
        if self._cur == NOTFOUND:
            # repop the cache by reading all namespaces
            last = self.reload()
            if self.seq and last != self.seq:
                raise IOError  # database inconsistent
        return self._cur

    @staticmethod
    def _segfile_reader(fh):
        for line in fh:
            seqno, typestr, jdata = line.split(' ', 2)
            logging.debug("segfile returning %r %r %r", seqno, typestr, jdata)
            yield int(seqno), typestr, jdata

    def _read_history(self, seqno):
        if seqno == self.seq:
            return self._read_cur()
        logging.debug("looking in history")
        result = NOTFOUND
        # read from a point in history
        with self._segfile_for_seq(seqno).open() as f:
            for seq, _, data in self._segfile_reader(f):
                if seq < seqno:
                    continue
                elif seq == seqno:
                    return data
                else:
                    break
        return NOTFOUND

    def read(self, start_seqno: int, typenames=None):
        """
        return a generator that will return the initial event and all subsequent events
        of the specified type names (or all types if typenames is unspecified)
        If the specifie sequence number doesn't exist, NOTFOUND will be returned
        """
        segfile = self._segfile_for_seq(start_seqno)
        # if nonexistant, send NOTFOUND
        if segfile is None:
            yield start_seqno, NOTFOUND
            return

        # send the partial segment the staring seqno is in
        with segfile.open() as f:
            for seq, tstr, data in self._segfile_reader(f):
                if seq < start_seqno:
                    continue
                if typenames is not None and tstr not in typenames:
                    continue
                yield seq, self.deserialize(data)

        # now send subsequent segments
        curseg = ( start_seqno // self.epoch_size )
        ## use a lambda for lastseg b/c self.seq could change while looping
        lastseg = lambda : self.seq // self.epoch_size
        while curseg < lastseg():
            curseg += 1
            segfile = self._segfile_for_seg(curseg)
            if not segfile.exists(): continue
            with segfile.open() as f:
                for seq, tstr, data in self._segfile_reader(f):
                    if typenames is not None and tstr not in typenames:
                        continue
                    yield seq, self.deserialize(data)





