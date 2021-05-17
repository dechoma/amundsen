# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import (
    Any, Dict, Optional,
)

from databuilder.models.atlas_entity import AtlasEntity
from databuilder.models.atlas_relationship import AtlasRelationship
from databuilder.models.atlas_serializable import (
    GUID, TYPE_NAME,
)
from databuilder.publisher.neo4j_csv_publisher import UNQUOTED_SUFFIX


def serialize_entity(entity: Optional[AtlasEntity]) -> Dict[str, Any]:
    if entity is None:
        return {}

    entity_dict = {
        TYPE_NAME: entity.typeName
    }
    for key, value in entity.attributes.items():
        key_suffix = '-elo'
        formatted_key = f'{key}{key_suffix}'
        entity_dict[formatted_key] = value
    return entity_dict


def serialize_relationship(relationship: Optional[AtlasRelationship]) -> Dict[str, Any]:
    if relationship is None:
        return {}

    relationship_dict = {
        "entityType1": relationship.entityType1,
        "entityQualifiedName1": relationship.entityQualifiedName1,
        "entityType2": relationship.entityType2,
        "entityQualifiedName2": relationship.entityQualifiedName2,
    }
    for key, value in relationship.attributes.items():
        key_suffix = '-elo'
        formatted_key = f'{key}{key_suffix}'
        relationship_dict[formatted_key] = value

    return relationship_dict



def _get_neo4j_suffix_value(value: Any) -> str:
    if isinstance(value, int):
        return UNQUOTED_SUFFIX

    if isinstance(value, bool):
        return UNQUOTED_SUFFIX

    return ''
