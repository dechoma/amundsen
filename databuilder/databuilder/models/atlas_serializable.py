# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import abc
from typing import Union  # noqa: F401

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship

GUID = 'guid'
NAME = 'name'
QUALIFIED_NAME = 'qualifiedName'
URL = 'url'
TYPE_NAME = 'typeName'
ID = 'id'
DESCRIPTION = 'description'
CREATED_TIMESTAMP = 'createdTimestamp'
LAST_MODIFIED_TIMESTAMP = 'lastModifiedTimestamp'
CLUSTER = 'cluster'
PRODUCT = 'product'
TABLES = 'tables'
GROUP = 'group'
OWNED_BY = 'ownedBy'


class AtlasSerializable(object, metaclass=abc.ABCMeta):
    """
    A Serializable abstract class asks subclass to implement next node or
    next relation in dict form so that it can be serialized to CSV file.

    Any model class that needs to be pushed to a graph database should inherit this class.
    """

    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def create_next_atlas_entity(self) -> Union[AtlasEntity, None]:
        """
        Creates GraphNode the process that consumes this class takes the output
        serializes to the desired graph database.

        :return: a GraphNode or None if no more records to serialize
        """
        raise NotImplementedError

    @abc.abstractmethod
    def create_next_atlas_relation(self) -> Union[AtlasRelationship, None]:
        """
        Creates GraphRelationship the process that consumes this class takes the output
        serializes to the desired graph database.

        :return: a GraphRelationship or None if no more record to serialize
        """
        raise NotImplementedError

    def next_atlas_entity(self) -> Union[AtlasEntity, None]:
        entity_dict = self.create_next_atlas_entity()
        if not entity_dict:
            return None

        #self._validate_node(entity_dict)
        return entity_dict

    def next_atlas_relation(self) -> Union[AtlasRelationship, None]:
        relation_dict = self.create_next_atlas_relation()
        if not relation_dict:
            return None

        #self._validate_relation(relation_dict)
        return relation_dict
