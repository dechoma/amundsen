# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from collections import namedtuple

AtlasRelationship = namedtuple(
    'AtlasRelationship',
    [
        'entityType1',
        'entityType2',
        'entityQualifiedName1',
        'entityQualifiedName2',
        'attributes'
    ]
)