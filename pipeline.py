import time
import os
import os.path
import shutil
import glob
import hashlib

from seesaw.project import *
from seesaw.config import *
from seesaw.item import *
from seesaw.task import *
from seesaw.pipeline import *
from seesaw.externalprocess import *
from seesaw.tracker import *

VERSION = "20120809.01"

class PrepareItem(SimpleTask):
  def __init__(self):
    SimpleTask.__init__(self, "PrepareItem")

  def process(self, item):
    item_name = item["item_name"]
    item["urlhash"] = hashlib.sha1(item_name).hexdigest()

class DeleteFiles(SimpleTask):
  def __init__(self):
    SimpleTask.__init__(self, "DeleteFiles")

  def process(self, item):
    os.unlink("data/%(urlhash)s.json.gz" % item)

def calculate_item_id(item):
  return "null"


project = Project(
  title = "Indexing ArchiveTeam archives",
  project_html = """
    <h2>Indexing ArchiveTeam archives <span class="links"><a href="http://archive.org/details/archiveteam">Website</a> &middot; <a href="http://tracker.archiveteam.org/warcindex/">Leaderboard</a></span></h2>
    <p>Help to index the files collected by ArchiveTeam.</p>
  """
)

pipeline = Pipeline(
  GetItemFromTracker("http://tracker.archiveteam.org/warcindex", downloader),
  PrepareItem(),
  ExternalProcess("CreateIndex", [ "./make-index.sh", ItemInterpolation("%(item_name)s"), ItemInterpolation("data/%(urlhash)s.json.gz") ]),
  PrepareStatsForTracker(
    defaults = { "downloader": downloader, "version": VERSION },
    file_groups = {
      "index": [ ItemInterpolation("data/%(urlhash)s.json.gz") ]
    },
    id_function = calculate_item_id
  ),
  LimitConcurrent(1,
    RsyncUpload(
      target = ConfigInterpolation("tracker.archiveteam.org::warcindex/%s/", downloader),
      target_source_path = ItemInterpolation("data/"),
      files = [
        ItemInterpolation("%(urlhash)s.json.gz")
      ]
    ),
  ),
  SendDoneToTracker(
    tracker_url = "http://tracker.archiveteam.org/warcindex",
    stats = ItemValue("stats")
  ),
  DeleteFiles()
)

