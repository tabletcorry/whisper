import json
import os

CONFIG_CACHE = False
_configCache = {}

def janus_open(name, mode='r', buffering=-1):
    janus_name = name + ".janus"
    if not os.path.exists(janus_name):
        return open(name, mode, buffering)

    janus_config = _configCache.get(janus_name)
    if janus_config is None:
        with open(janus_name, 'r') as janus_file:
            janus_config = json.load(janus_file)
        if CONFIG_CACHE:
            _configCache[janus_name] = janus_config

    version = janus_config['version']
    assert version == 1

    archive_files = []
    for archive, size in janus_config['archives']:
        archive_files.append((archive, size))

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
            metadata['archives'].append((slice_name, info['archives'][0]['offset']))

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
            metadata['archives'].append((slice_name, archive['size']))

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
        self.archive_files = map(lambda x: x[0], archive_files)
        self.virtual_position = 0

        self.current_file = self.archive_files[0]
        self.file_maxima = []
        total_offset = 0
        for archive_file, size in archive_files:
            self.file_maxima.append(size + total_offset)
            total_offset += size

        self.current_min = 0
        self.current_max = self.file_maxima[0]
        self.total_max = self.file_maxima[-1]

    def read(self, size=-1):
        if size < 0:
            # Whisper does not use negative reads
            raise NotImplementedError("Non-negative read sizes only")
        if type(self.current_file) is unicode:
            self.current_file = open(self.current_file, 'r+b')
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
            if offset - self.current_min != 0:
                if type(self.current_file) is unicode:
                    self.current_file = open(self.current_file, 'r+b')
                self.current_file.seek(offset - self.current_min)
            break
        else:
            raise ValueError("Seek outside of all archives")

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            if offset >= self.current_max or offset < self.current_min:
                self._switch_file(offset)
            else:
                if type(self.current_file) is unicode:
                    self.current_file = open(self.current_file, 'r+b')
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
        if type(self.current_file) is unicode:
            self.current_file = open(self.current_file, 'r+b')
        return self.current_file.write(data)

    def flush(self):
        for archive in self.archive_files:
            archive.flush()

    def close(self):
        for archive in self.archive_files:
            if type(archive) is file:
                archive.close()
