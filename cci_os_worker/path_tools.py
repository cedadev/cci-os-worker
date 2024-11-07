# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '07 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from pathlib import Path
from ceda_elasticsearch_tools.core.log_reader import SpotMapping
import os
import requests
from json.decoder import JSONDecodeError
import json
import hashlib
from requests.exceptions import Timeout
from directory_tree import DatasetNode

from typing import Optional, Tuple, List

class PathTools:
    def __init__(
        self,
        moles_mapping_url: str = "http://api.catalogue.ceda.ac.uk/api/v2/observations.json/",
        mapping_file: Optional[str] = None,
    ):

        self.spots = SpotMapping()

        self.moles_mapping_url = moles_mapping_url

        if mapping_file:
            self.moles_mapping = load_moles_mapping(mapping_file)
        else:
            self.moles_mapping = generate_moles_mapping(self.moles_mapping_url)

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