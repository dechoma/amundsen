# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import time
import pandas
import logging
from os import listdir
from os.path import isfile, join
from typing import List

from collections import defaultdict

from apache_atlas.client.base_client import AtlasClient
from apache_atlas.exceptions import AtlasServiceException
from apache_atlas.model.instance import AtlasEntity, AtlasEntitiesWithExtInfo, AtlasRelatedObjectId, AtlasObjectId
from apache_atlas.model.relationship import AtlasRelationship
from common.amundsen_common.utils.atlas_utils import AtlasCommonParams, AtlasSerializedRelationshipFields, AtlasSerializedEntityFields

from pyhocon import ConfigTree

from databuilder.publisher.base_publisher import Publisher

LOGGER = logging.getLogger(__name__)


class AtlasCSVPublisher(Publisher):
    ENTITY_DIR_PATH = 'entity_files_directory'
    # A directory that contains CSV files for relationships
    RELATION_DIR_PATH = 'relation_files_directory'
    # A transaction size that determines how often it commits.
    ATLAS_ENTITY_CREATE_BATCH_SIZE = 'batch_size'

    def __init__(self) -> None:
        super(AtlasCSVPublisher, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        self._atlas_client = AtlasClient('http://localhost:21000', ('admin', 'admin'))
        self._entity_files = self._list_files(conf, AtlasCSVPublisher.ENTITY_DIR_PATH)
        self._relationship_files = self._list_files(conf, AtlasCSVPublisher.RELATION_DIR_PATH)
        self._config = conf

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
        return sorted([join(path, f) for f in listdir(path) if isfile(join(path, f))])

    def publish_impl(self) -> None:
        """
        Publishes Entities first and then Relations
        :return:
        """
        LOGGER.info('Creating entities using Entity files: %s', self._entity_files)
        for entity_file in self._entity_files:
            entities = self._create_entities(entity_file=entity_file)
            self._sync_entities_to_atlas(entities=entities)

        LOGGER.info('Creating relations using relation files: %s', self._relationship_files)
        for relation_file in self._relationship_files:
            self._create_relations(relation_file=relation_file)

    def _create_relations(self, relation_file: str) -> None:
        """
        Go over the relation file and attach relation to entities
        :param relation_file:
        :return:
        """

        with open(relation_file, 'r', encoding='utf8') as relation_csv:
            for relation_record in pandas.read_csv(relation_csv, na_filter=False).to_dict(orient='records'):
                LOGGER.warning(relation_record)
                relation = self._create_relation(relation_record)
                try:
                    self._atlas_client.relationship.create_relationship(relation)
                except AtlasServiceException:
                    pass

    def _get_atlas_object_id_by_qn(self, entity_type, qn):
        return AtlasObjectId(attrs={
            AtlasCommonParams.type_name: entity_type,
            AtlasCommonParams.unique_attributes: {
                AtlasCommonParams.qualified_name: qn
            }
        })

    def _get_atlas_related_object_id_by_qn(self, entity_type, qn) -> AtlasRelatedObjectId:
        return AtlasRelatedObjectId(attrs={
            AtlasCommonParams.type_name: entity_type,
            AtlasCommonParams.unique_attributes: {
                AtlasCommonParams.qualified_name: qn
            }
        })

    def _create_relation(self, relation_dict):
        relation = AtlasRelationship({AtlasCommonParams.type_name: relation_dict[AtlasSerializedRelationshipFields.relation_type]})
        relation.end1 = self._get_atlas_object_id_by_qn(relation_dict[AtlasSerializedRelationshipFields.entity_type_1]
                                                        , relation_dict[AtlasSerializedRelationshipFields.qualified_name_1])
        relation.end2 = self._get_atlas_object_id_by_qn(relation_dict[AtlasSerializedRelationshipFields.entity_type_2]
                                                        , relation_dict[AtlasSerializedRelationshipFields.qualified_name_2])

        return relation

    def _create_entities(self, entity_file: str) -> List[AtlasEntity]:
        """
        Go over the node file and try creating unique index
        :param entity_file:
        :return:
        """
        LOGGER.info('Creating entities')
        entities_to_create = []
        with open(entity_file, 'r', encoding='utf8') as entity_csv:
            for entity_record in pandas.read_csv(entity_csv, na_filter=False).to_dict(orient='records'):
                LOGGER.warning(entity_record)
                entities_to_create.append(self._create_entity_from_dict(entity_record))

        return entities_to_create

    def _extract_entity_relations_details(self, relation_details):
        """
        Generate relation details from relation_attr#related_entity_type#related_qualified_name
        """
        relations = relation_details.split(AtlasSerializedEntityFields.relationships_separator)
        for relation in relations:
            relation_splitted = relation.split(AtlasSerializedEntityFields.relationships_kv_separator)
            yield relation_splitted[0], relation_splitted[1], relation_splitted[2]

    def _create_entity_from_dict(self, entity_dict) -> AtlasEntity:
        type_name = {AtlasCommonParams.type_name: entity_dict[AtlasCommonParams.type_name]}
        entity = AtlasEntity(type_name)
        entity.attributes = entity_dict
        relationships = entity_dict.get(AtlasSerializedEntityFields.relationships)
        if relationships:
            relations = dict()
            for relation_attr, rel_type, rel_qn in self._extract_entity_relations_details(relationships):
                related_obj = self._get_atlas_related_object_id_by_qn(rel_type, rel_qn)
                relations[relation_attr] = related_obj
            LOGGER.error(relations)
            entity.relationshipAttributes = relations
        return entity

    def _chunks(self, lst):
        """Yield successive n-sized chunks from lst."""
        n = self._config.get_int(AtlasCSVPublisher.ATLAS_ENTITY_CREATE_BATCH_SIZE)
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def _sync_entities_to_atlas(self, entities):
        entities_chunks = self._chunks(entities)
        for entity_chunk in entities_chunks:
            LOGGER.warning("CREATING CHUNK")
            for ent in entity_chunk:
                LOGGER.warning(ent.__dict__)
            chunk = AtlasEntitiesWithExtInfo()
            chunk.entities = entity_chunk
            self._atlas_client.entity.create_entities(chunk)


    def get_scope(self) -> str:
        return 'publisher.atlas_csv_publisher'
