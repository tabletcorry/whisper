import json
import os

def janus_open(name, mode='r', buffering=-1):
    janus_name = name + ".janus"
    if not os.path.exists(janus_name):
        return open(name, mode, buffering)

    with open(janus_name, 'r') as janus_file:
        janus_config = json.load(janus_file)

    version = janus_config['version']
    assert version == 1

    archive_files = []
    for archive in janus_config['archives']:
        archive_files.append(open(archive, 'r+b'))

    return Janus(name, archive_files)


def janus_create(name):
    import whisper
    metadata = {
        'version': 1,
        'archives': [
        ]
    }
    with open(name, 'r+b') as original:
        info = whisper.__readHeader(original)

        slice_name = "{0}.janus.{1}.{2}".format(name, "cold", 0)
        with open(slice_name, 'wb') as janus_slice:
            janus_slice.write(original.read(info['archives'][0]['offset']))
            metadata['archives'].append(slice_name)

        total_archives = len(info['archives'])
        for i, archive in enumerate(info['archives']):
            if i + 1 == total_archives:
                temp = "cold"
            else:
                temp = "hot"
            slice_name = "{0}.janus.{1}.{2}".format(name, temp, i + 1)
            with open(slice_name, 'wb') as janus_slice:
                original.seek(archive['offset'])
                janus_slice.write(original.read(archive['size']))
            metadata['archives'].append(slice_name)

    os.remove(name)
    with open(name + ".janus", 'wb') as meta:
        json.dump(metadata, meta)


class Janus(object):
    def __init__(self, name, archive_files):
        """


        :type name: str
        :type archive_files: list
        """
        self.name = name
        self.archive_files = archive_files
        self.virtual_position = 0

        self.current_file = self.archive_files[0]
        self.file_maxima = []
        total_offset = 0
        for archive_file in self.archive_files:
            archive_file_size = os.path.getsize(archive_file.name)
            self.file_maxima.append(archive_file_size + total_offset)
            total_offset += archive_file_size

        self.current_min = 0
        self.current_max = self.file_maxima[0]
        self.total_max = self.file_maxima[-1]

    def read(self, size=-1):
        if size < 0:
            # Whisper does not use negative reads
            raise NotImplementedError("Non-negative read sizes only")
        read_bytes = self.current_file.read(size)
        assert len(read_bytes) == size
        self.virtual_position += size
        if self.total_max != self.virtual_position == self.current_max:
                self._switch_file(self.virtual_position)
        return read_bytes

    def tell(self):
        return self.virtual_position

    def _switch_file(self, offset):
        for i, maxima in enumerate(self.file_maxima):
            if offset >= maxima:
                continue
            self.current_file = self.archive_files[i]
            if i == 0:
                self.current_min = 0
            else:
                self.current_min = self.file_maxima[i - 1]
            self.current_max = maxima
            self.virtual_position = offset
            self.current_file.seek(offset - self.current_min)
            break
        else:
            raise ValueError("Seek outside of all archives")

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            if offset >= self.current_max or offset < self.current_min:
                self._switch_file(offset)
            else:
                self.current_file.seek(offset - self.current_min)
                self.virtual_position = offset
        elif whence == os.SEEK_CUR:
            raise NotImplementedError()
        elif whence == os.SEEK_END:
            raise NotImplementedError()
        else:
            raise NotImplementedError()

    def fileno(self):
        raise NotImplementedError()
        #return self.current_file.fileno()

    def write(self, data):
        self.virtual_position += len(data)
        return self.current_file.write(data)

    def flush(self):
        for archive in self.archive_files:
            archive.flush()

    def close(self):
        for archive in self.archive_files:
            archive.close()
