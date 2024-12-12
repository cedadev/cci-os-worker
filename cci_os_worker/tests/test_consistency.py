

class TestConsistency:

    def test_core(self):

        from cci_os_worker.errors import HandlerError, DocMetadataError
        from cci_os_worker.facet_scan import (
            FacetUpdateHandler,
            _get_command_line_args,
            facet_main
        )
        from cci_os_worker.fbi_update import (
            FBIUpdateHandler,
            get_bytes_from_file,
            get_file_header,
            _get_command_line_args,
            fbi_main
        )

        from cci_os_worker.path_tools import PathTools
        
        from cci_os_worker.run_all import (
            get_command_line_arguments,
            load_config,
            main
        )

        from cci_os_worker.utils import (
            set_verbose,
            load_config,
            load_datasets,
            UpdateHandler
        )

        assert 1==1

    def test_netcdf(self):

        from cci_os_worker.netcdf_handler.generic_file import GenericFile
        from cci_os_worker.netcdf_handler.geojson import GeoJSONGenerator
        from cci_os_worker.netcdf_handler.netcdf_file import NetCdfFile
        from cci_os_worker.netcdf_handler.util import Parameter