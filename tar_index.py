import json
import re
import sys
import tarfile

from ordereddict import OrderedDict

import requests

class TarEntry(object):
  BLOCK_SIZE = 512

  def __init__(self, offset):
    self.offset = offset

  def data_offset(self):
    return self.offset + TarEntry.BLOCK_SIZE

  @classmethod
  def from_string(cls, string, offset):
    try:
      entry = tarfile.TarInfo.frombuf(string)
    except:
      return None

    entry.offset = offset
    entry.data_offset = offset + TarEntry.BLOCK_SIZE

    return entry


# Lists the contents of a tar file using HTTP 1.1 Range requests.
class TarIndex(object):
  BLOCK_SIZE = 512
  BATCH_SIZE = 10

  def __init__(self, url):
    self.url = url
    self._headers = []
    self._pos = 0
    self._full_size = None

  def bytes_read(self):
    return self._pos or self._full_size

  def full_size(self):
    if self._full_size is None:
      self._fetch_next_headers()
    return self._full_size

  def __iter__(self):
    self._fetch_next_headers()
    headers = self._headers
    while headers:
      for header in headers:
        yield header
      headers = self._fetch_next_headers()

  def _fetch_next_headers(self):
    if self._pos is None:
      return None

    errors = 0
    response = None
    while response is None and errors < 5:
      try:
        range_header = "bytes=%d-%d" % (self._pos, self._pos + (TarIndex.BATCH_SIZE * TarIndex.BLOCK_SIZE - 1))
        response = requests.get(self.url, headers={"Range": range_header})
        match = re.search("/([0-9]+)$", response.headers["Content-Range"])
        self._full_size = int(match.group(1))
        self.url = response.url
      except:
        print >>sys.stderr, "Error, retry."
        response = None
        errors += 1
    if response is None:
      raise Exception("HTTP error.")

    if response.status_code != requests.codes.ok:
      response.raise_for_status()

    data = response.content

    new_headers = []
    offset = 0
    while offset + TarIndex.BLOCK_SIZE <= len(data) and not self._pos is None:
      header = TarEntry.from_string(data[offset:(offset + TarIndex.BLOCK_SIZE)], self._pos)
      if header is None:
        self._pos = None
      else:
        entry_size = TarIndex.BLOCK_SIZE + header.size + (TarIndex.BLOCK_SIZE - (header.size % TarIndex.BLOCK_SIZE)) % TarIndex.BLOCK_SIZE
        offset += entry_size
        self._pos += entry_size
        self._headers.append(header)
        new_headers.append(header)

    if self._pos is None or self._full_size < self._pos + TarIndex.BLOCK_SIZE:
      self._pos = None

    if new_headers:
      return new_headers
    else:
      return None


# e.g. "http://archive.org/download/archiveteam-fortunecity-00000026/00000026.tar"
url = sys.argv[1]

ti = TarIndex(url)
print >>sys.stderr, "%s, %d MB" % (url, ti.full_size() / (1024*1024))
entries = []
prev_progress = None
for entry in ti:
  progress = ((100 * ti.bytes_read()) / ti.full_size())
  if progress != prev_progress:
    print >>sys.stderr, " %d%%" % progress
    prev_progress = progress

  if entry.isfile():
    d = OrderedDict()
    d["name"] = entry.name
    d["offset"] = entry.offset
    d["size"] = entry.size
    d["range"] = [entry.data_offset, entry.data_offset + entry.size - 1]
    entries.append(d)

d = OrderedDict()
d["url"] = url
d["size"] = ti.full_size()
d["files"] = entries
print json.dumps(d)

