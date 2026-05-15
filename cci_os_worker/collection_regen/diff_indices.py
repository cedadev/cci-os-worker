# encoding: utf-8
"""
Generate events based on the diff between two indices.

This script takes two opensearch collections indices and diffs them.
There are three possible events:
    1. A new dataset has been added
    2. A dataset has been removed
    3. A DRSID has changed on a dataset between the two indices


Usage:
-----

usage: diff_indices.py [-h] [--url URL] source_index dest_index token

Generate a list of dictionaries containing events

positional arguments:
  source_index  name of source index
  dest_index    name of destination index
  token         add authentication token

optional arguments:
  -h, --help    show this help message and exit
  --url URL     URL of the request location

"""
__author__ = 'Richard Smith'
__date__ = '16 Aug 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


import argparse
import json
import requests
from elasticsearch import Elasticsearch
from datetime import datetime
from elasticsearch.helpers import scan
from typing import Dict, List
import enum
import attr
import ssl

from cci_facet_scanner.utils.elasticsearch import es_connection_kwargs

BASE_QUERY = {
    '_source': {
        'includes': ['collection_id','drsId', 'title']
    }
}


def command_line_args() -> argparse.Namespace:
    """Get the command line args"""
    parser = argparse.ArgumentParser(description="Generate a list of dictionaries containing events")

    parser.add_argument("source_index", help="name of source index")
    parser.add_argument("dest_index", help="name of destination index")
    parser.add_argument("token", help="add authentication token")
    parser.add_argument("--url", help="URL of the request location", default="http://localhost:8000/api/events/")

    parser.add_argument(
        '--es-api-key',
        dest='ES_API_KEY',
        required=True,
        help='Elasticsearch API key to allow write'
    )
    parser.add_argument(
        '--host',
        dest='HOST',
        required=False,
        default='https://elasticsearch.ceda.ac.uk',
        help='Elasticsearch Host'
    )

    return parser.parse_args()


class EventAction(str, enum.Enum):
    """Enum class to hold event actions"""
    added='added'
    removed='removed'
    updated='updated'


@attr.s
class Event:
    """Constructor class to build events"""
    collection_id = attr.ib()
    collection_title = attr.ib()
    action = attr.ib(default=EventAction.added)
    datetime = attr.ib(default=datetime.today().strftime('%Y-%m-%d'))

    def to_json(self):
        return json.dumps(self.__dict__)


class EventGenerator:
    """Processor to generate and push collection events"""

    def __init__(self, args):
        self.args = args
        self.es = Elasticsearch(
            **es_connection_kwargs(
                hosts=[args.HOST],
                api_key=args.ES_API_KEY
            )
        )

        self.source_collections = self._get_data(args.source_index)
        self.dest_collections = self._get_data(args.dest_index)

    def _get_data(self, index: str) -> Dict[str, Dict]:
        """Retrieve the collections from elasticsearch

        :param index: Index to retrieve from
        """
        results = scan(self.es, index=index, query=BASE_QUERY)
        return {res['_id']:res['_source'] for res in results}

    def _get_headers(self) -> Dict:
        """Generate headers for events endpoint."""

        return {
            'Authorization': f'Token {self.args.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

    def new_datasets(self) -> List[Event]:
        """Datasets present in dest index but not source"""
        ids = set(self.dest_collections.keys()) - set(self.source_collections.keys())

        events = []
        for id in ids:
            title = self.dest_collections[id].get('title')
            events.append(
                Event(
                    collection_id=id,
                    collection_title=title
                )
            )

        return events

    def removed_datasets(self) -> List[Event]:
        """Datasets present in source index but not dest"""
        ids = set(self.source_collections.keys()) - set(self.dest_collections.keys())

        events = []
        for id in ids:
            title = self.source_collections[id].get('title')
            events.append(
                Event(
                    collection_id=id,
                    collection_title=title,
                    action=EventAction.removed
                )
            )

        return events

    def updated_datasets(self) -> List[Event]:
        """
        Datasets which existed in both the source and destination indices
        but for which the DRSIds are different in the destination index when
        compared with the source.
        """
        events = []

        for dataset in self.dest_collections:
            if dataset in self.source_collections:
                source_drs = set(self.source_collections[dataset].get('drsId', ()))
                dest_drs = set(self.dest_collections[dataset].get('drsId', ()))

                # Check if there are any different DRS IDs
                if dest_drs - source_drs:
                    events.append(
                        Event(
                            collection_id=dataset,
                            collection_title=self.dest_collections[dataset]['title'],
                            action=EventAction.updated
                        )
                    )

        return events

    def upload_events(self, events: List[Event]) -> None:
        """Upload events to events endpoint."""
        events = [event.__dict__ for event in events]
        events_json = json.dumps(events)
        print(events_json)

        r = requests.post(
            self.args.url,
            data=events_json,
            headers=self._get_headers(),
        )

        print(f'HTTP Response {r.status_code}')  # HTTP response
        if r.status_code >= 300:
            raise Exception(f'Error connecting to opensearch server {r.json()}')

    def get_events(self) -> List[Event]:
        """
        Get the different event types
        """
        added = self.new_datasets()
        removed = self.removed_datasets()
        updated = self.updated_datasets()

        return added + removed + updated

    def process_events(self) -> None:
        """
        Main process to generate events and POST them.
        """

        events = self.get_events()

        print('#### GENERATED EVENTS ####')
        for event in events:
            print(event, '\n')

        self.upload_events(events)


def main():

    args = command_line_args()

    coll_events = EventGenerator(args)
    coll_events.process_events()

if __name__ == '__main__':
    main()
