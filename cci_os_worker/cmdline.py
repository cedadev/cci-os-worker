# encoding: utf-8
__author__ = 'Daniel Westwood'
__date__ = '05 Nov 2024'
__copyright__ = 'Copyright 2024 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'daniel.westwood@stfc.ac.uk'

from .facet_scan import FacetUpdateHandler

def get_command_line_arguments():
    """
    Arguments from the command line:
     - datafile_path: Path to the 'datasets.txt' file
     - conf: Config file to connect to ES.
     - skip_facet
     - skip_tag
     - skip_fbi
    """

    parser = argparse.ArgumentParser(description='Entrypoint for the CCI OS Worker on the CMD Line')
    parser.add_argument('datafile_path', type=str, help='Path to the "datasets.txt" file')
    parser.add_argument('conf', type=str, help='Path to Yaml config file for Elasticsearch')

    parser.add_argument('--skip-facet', dest='skip_facet', action='store_true', help='Skip the Facet Scan')
    parser.add_argument('--skip-tag', dest='skip_tag', action='store_true', help='Skip the Tag Scan')
    parser.add_argument('--skip-fbi', dest='skip_fbi', action='store_true', help='Skip the Fbi Updates')

    parser.add_argument('-d','--dryrun', dest=dryrun, action='store_true', help='Perform in dryrun mode')
    parser.add_argument('-t','--test', dest=test, action='store_true', help='Perform in test/staging mode')

    args = parser.parse_args()

    return **{
        'datafile_path': args.datafile_path,
        'conf': args.conf,
        'skip_facet': args.skip_facet,
        'skip_tag': args.skip_tag,
        'skip_fbi': args.skip_fbi,
        'dryrun': args.dryrun,
        'test': args.test
    }

def load_config(conf):
    """
    Load yaml config
    """
    return {}

def main():
    """
    Entrypoint for the CCI OS Worker on the command line.
    By default will run facet scan, tag scan and fbi update 
    on all files.
    """

    # Fetch all command line arguments
    datafile_path, conf, skip_facet, \
    skip_tag, skip_fbi, dryrun, test = get_command_line_arguments()

    # Load config information
    conf = load_config(conf)

    if not skip_facet:
        fs = FacetUpdateHandler(conf, dryrun=dryrun, test=test)
        fs.process_deposits(datafile_path)
    if not skip_tag:
        ts = TagUpdateHandler(conf, dryrun=dryrun, test=test)
    if not skip_fbi:
        fb = FbiUpdateHandler(conf, dryrun=dryrun, test=test)


    



if __name__ == '__main__':
    main()


