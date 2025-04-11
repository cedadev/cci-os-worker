

class TestConsistency:

    def test_core(self):

        from cci_os_worker.errors import HandlerError, DocMetadataError
        from cci_os_worker.all_facets import (
            FacetUpdateHandler,
            _get_command_line_args,
            main
        )

        from cci_os_worker.path_tools import PathTools

        from cci_os_worker.utils import (
            set_verbose,
            load_config,
            load_datasets,
            UpdateHandler
        )

        assert 1==1

    def test_netcdf(self):

        from cci_os_worker.filehandlers.generic_file import GenericFile
        from cci_os_worker.filehandlers.geojson import GeoJSONGenerator
        from cci_os_worker.filehandlers.netcdf_file import NetCdfFile
        from cci_os_worker.filehandlers.util import Parameter