# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '07 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from pathlib import Path
import os
import requests
from json.decoder import JSONDecodeError
import json
import hashlib
from requests.exceptions import Timeout
from ceda_directory_tree import DatasetNode

from typing import Optional, Tuple, List

import logging
from cci_os_worker import logstream

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

class SpotMapping():
    """
    Downloads the spot mapping from the cedaarchiveapp.
    Makes two queryable dicts:
        - spot2pathmapping = provide spot and return file path
        - path2spotmapping = provide a file path and the spot will be returned
    """
    url = "http://cedaarchiveapp.ceda.ac.uk/cedaarchiveapp/fileset/download_conf/"
    spot2pathmapping = {}
    path2spotmapping = {}

    # Remove logging message when running script
    logging.getLogger("requests").setLevel(logging.WARNING)

    def __init__(self, test=False, spot_file=None, sep='='):

        logging.info("Initialising spots mapping with test: {} and spot file: {}".format(test, spot_file))

        if test:
            self.spot2pathmapping['spot-1400-accacia'] = "/badc/accacia"
            self.spot2pathmapping['abacus'] = "/badc/abacus"

        elif spot_file:
            with open(spot_file) as reader:
                spot_mapping = reader.readlines()

            self._build_mapping(spot_mapping, sep=sep)

        else:
            self._download_mapping()

    def __iter__(self):
        return iter(self.spot2pathmapping)

    def __len__(self):
        return len(self.spot2pathmapping)

    def _download_mapping(self):
        """
        Download the mapping from the cedaarchiveapp and build mappings.
        """

        logging.info("Downloading spots from {}".format(self.url))

        response = requests.get(self.url)

        if response.status_code != 200:
            logging.error("Error getting mapping from cedaarchiveapp status code: {}, reason: {}".format(response.status_code, response.reason))
            raise Exception("bad response status code: {}, reason: {}".format(response.status_code, response.reason))

        spot_mapping = response.text.split('\n')

        self._build_mapping(spot_mapping)

    def _build_mapping(self, spot_mapping, sep=None):
        """
        Build the spot mapping dictionaries
        :param spot_mapping: list of mappings
        """

        for line in spot_mapping:
            if not line.strip(): continue
            spot, path = line.strip().split(sep)

            if spot in ("spot-2502-backup-test",): continue
            self.spot2pathmapping[spot] = path
            self.path2spotmapping[path] = spot

    def get_archive_root(self, key):
        """

        :param key: Provide the spot
        :return: Returns the directory mapped to that spot
        """

        # Get the path to the spot
        archive_root = self.spot2pathmapping.get(key)

        return archive_root

    def get_spot(self, key):
        """
        The directory stored in elasticsearch is the basename for the specific file. The directory stored on the spots
        page is further up the directory structure but there is no common cut off point as it depends on how many files there
        are in each dataset. This function recursively starts at the end of the directory stored in elasticsearch
        and gradually moves back up the file structure until it finds a match in the path2spot dict.

        :param key: Provide a filename or directory
        :return: Returns the spot which encompasses that file or directory.
        """

        archive_path = self.get_archive_path(key)

        if archive_path:
            while (archive_path not in self.path2spotmapping) and (archive_path != '/'):
                archive_path = os.path.dirname(archive_path)

        if archive_path == '/' or archive_path is None:
            return None

        return self.path2spotmapping[archive_path]

    def get_spot_from_storage_path(self, path):
        """
        Extract the spot name from the storage path

        :param path: Path to test
        :return: spot name and path suffix
        """

        # Setup output
        spot, suffix = None, None

        try:
            storage_suffix = path.split('/archive/')[1]
            spot = storage_suffix.split('/')[0]
            suffix = storage_suffix.split(spot + '/')[1]

        except IndexError:
            logging.warning("Error getting spot from: {}".format(path))

        return spot, suffix

    def get_archive_path(self, path):
        storage_path = os.path.realpath(path)

        spot, suffix = self.get_spot_from_storage_path(storage_path)

        spot_path = self.get_archive_root(spot)

        try:
            archive_path = os.path.join(spot_path, suffix)

        # Joining None produces an AttributeError in py2 and a TypeError py3
        except (AttributeError, TypeError):
            archive_path = spot_path

        return archive_path

    def is_archive_path(self, path):
        """
        Archive path refers to the location in the archive where the file exists.
        In the archive there are 3 path types:
            - storage path: the actual location of the file eg. /datacentre/archvol3/pan125/archive/namblex/data/...
            - archive path: the path with the spot mapping replacing the storage prefix eg. /badc/namblex/data/aber-radar-1290mhz/20020831/
            - symlink path: an alternative route to the file as displayed using pydap eg. badc/ncas-observations/data/man-radar-1290mhz/previous-versions/2002/08/31/

        This function takes the input path, gets the storage path, replaces the storage prefix with spot mapping to get the archive
        path and compares it to the input path. Returns True if input path is archive path.

        :param path: file path to test
        :return: Bool
        """

        return path == self.get_archive_path(path)

def process_observations(results):
    """
    Convert the result list into a mapping object
    :param results: list of observation json objects
    :return: object map
    """

    processed_map = {}
    for result in results:

        # Skip results where the publication state is working
        if result.get("publicationState") == "working":
            continue

        # Skip where the result_field value is None
        if result["result_field"] is None:
            continue

        data_path = result["result_field"]["dataPath"].rstrip("/")

        try:
            processed_map[data_path] = {
                "title": result["title"],
                "url": f'https://catalogue.ceda.ac.uk/uuid/{result["uuid"]}',
                "record_type": "Dataset",
            }
        except TypeError:
            continue

    return processed_map

def generate_moles_mapping(api_url, mapping=None):
    """
    Use the MOLES v2 API to generate a mapping from dataset path to moles record

    :param api_url: MOLES api URL
    :param mapping: Used for recursive functionality
    :return: Mapping dict
    """

    # Set the dictionary on first calling
    if not mapping:
        mapping = {}

    # Get the api response
    try:
        response = requests.get(api_url).json()
    except JSONDecodeError as e:
        import sys

        raise ConnectionError(
            f"Could not connect to {api_url} to get moles mapping"
        ) from e

    # Turn response into mapping object
    mapping.update(process_observations(response["results"]))

    if not response["next"]:
        return mapping
    else:
        return generate_moles_mapping(response["next"], mapping)

def load_moles_mapping(mapping_file):
    """
    Load a json document and condition it to match same as from API
    """

    data = {}

    with open(mapping_file) as reader:
        json_doc = json.load(reader)

    # Loop through and remove trailing slash from paths
    for k, v in json_doc.items():
        data[k.rstrip("/")] = v

    return data

class PathTools:
    def __init__(
        self,
        moles_mapping_url: str = "http://api.catalogue.ceda.ac.uk/api/v2/observations.json/",
        mapping_file: Optional[str] = None,
        spot_file: str = None,
    ):

        if os.path.isfile('moles_mapping.json'):
            mapping_file = 'moles_mapping.json'

        self.spots = SpotMapping(spot_file=spot_file)

        self.moles_mapping_url = moles_mapping_url

        if mapping_file:
            self.moles_mapping = load_moles_mapping(mapping_file)
        else:
            self.moles_mapping = generate_moles_mapping(self.moles_mapping_url)

        #DEBUG
        #with open('moles_mapping.json','w') as f:
            #f.write(json.dumps(self.moles_mapping))

        # Setup the matching tree
        self.tree = DatasetNode()
        for path in self.moles_mapping:
            self.tree.add_child(path)

    def generate_path_metadata(
        self, path: str
    ) -> Tuple[Optional[dict], Optional[bool]]:
        """
        Take path and process it to generate metadata as used in ceda directories index
        :param path: path to retrieve metadata for
        :return:
        """

        path = Path(path)

        try:
            if not path.exists():
                return None, None
        except PermissionError:
            return None, None

        # See if the path is a symlink and directory
        link = path.is_symlink()
        isdir = path.is_dir()

        # Set the archive path
        archive_path = path

        # If the path is a link, we need to find the path to the actual data
        if link:
            link_path = os.readlink(path)
            if not link_path.startswith(("/datacentre", "..")):
                archive_path = link_path
            elif link_path.startswith(".."):
                count = link_path.count("../")
                link_path = link_path.lstrip("./")
                archive_path = path.parents[count] / link_path

        # Create the metadata
        meta = {
            "depth": len(path.parts) - 1,
            "dir": path.name,
            "path": str(path),
            "archive_path": str(archive_path),
            "link": link,
            "type": "dir" if isdir else "file",
        }

        # Retrieve the appropriate MOLES record
        if isdir:
            record = self.get_moles_record_metadata(str(path))

            # If a MOLES record is found, add the metadata
            if record and record["title"]:
                meta["title"] = record["title"]
                meta["url"] = record["url"]
                meta["record_type"] = record["record_type"]

        return meta, meta["link"]

    def get_moles_record_metadata(self, path: str) -> Optional[dict]:
        """
        Try and find metadata for a MOLES record associated with the path.

        :param path: Directory path
        :return: Dictionary containing MOLES title, url and record_type
        """

        # Condition path - remove trailing slash
        path = path.rstrip("/")

        # Search the tree
        match = self.tree.search_name(path)
        if match:
            result = self.moles_mapping.get(path)

            if result:
                return result

        return self._get_moles_record_metadata_data_from_api(path)

    def _get_moles_record_metadata_data_from_api(self, path: str) -> Optional[dict]:
        """
        Request metadata from the API, this is used as a last resort

        :param path: Path to retrieve metadata for
        :return: Metadata dict | None
        """

        url = f"http://api.catalogue.ceda.ac.uk/api/v0/obs/get_info{path}"
        try:
            response = requests.get(url, timeout=10)
        except Timeout:
            return

        # Update moles mapping
        if response:
            self.moles_mapping[path] = response.json()
            return response.json()

    @staticmethod
    def get_readme(path: str) -> Optional[str]:
        """
        Search in directory for a README file and read the contents
        :param path: Directory path
        :return: Readme contents
        """
        if not os.path.exists(path):
            return

        if "00README" in os.listdir(path):
            with open(os.path.join(path, "00README"), errors="replace") as reader:
                content = reader.read()

            return content.encode(errors="ignore").decode()

    def update_mapping(self) -> bool:

        successful = True
        # Update the moles mapping
        try:
            self.spots._download_mapping()
            self.moles_mapping = requests.get(self.moles_mapping_url, timeout=30).json()
        except (ValueError, Timeout):
            successful = False

        return successful

    @classmethod
    def generate_id(cls, path: str) -> str:
        """
        Take a path, encode to utf-8 (ignoring non-utf8 chars) and return hash

        :param path: filepath to hash
        :return: hash hexdigest of sha1 hash
        """

        return hashlib.sha1(path.encode(errors="ignore")).hexdigest()