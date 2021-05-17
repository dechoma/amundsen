# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
from os import listdir
from os.path import isfile, join
from typing import List

from apache_atlas.client.base_client import AtlasClient

from pyhocon import ConfigTree

from databuilder.publisher.base_publisher import Publisher

LOGGER = logging.getLogger(__name__)


class AtlasCSVPublisher(Publisher):

    def __init__(self) -> None:
        super(AtlasCSVPublisher, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        self._atlas_client =  AtlasClient('http://localhost:21000', ('admin', 'admin'))

    def _list_files(self, conf: ConfigTree, path_key: str) -> List[str]:
        """
        List files from directory
        :param conf:
        :param path_key:
        :return: List of file paths
        """
        if path_key not in conf:
            return []

        path = conf.get_string(path_key)
        return [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    def publish_impl(self) -> None:
        pass

    def get_scope(self) -> str:
        return 'publisher.atlas_csv_publisher'
