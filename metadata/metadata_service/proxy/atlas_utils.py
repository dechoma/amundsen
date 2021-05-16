import abc
import re
from typing import Any, Dict, Optional


class Status:
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class AtlasKey(abc.ABC):
    """
    Class for unification of entity keys between Atlas and Amundsen ecosystems.

    Since Atlas can be populated both by tools from 'Atlas world' (like Apache Atlas Hive hook/bridge) and Amundsen
    Databuilder (and each of the approach has a different way to render unique identifiers) we need such class
    to serve as unification layer.
    """

    def __init__(self, raw_id: str, source: Optional[str] = None) -> None:
        self._raw_identifier = raw_id
        self._source = source

    @property
    def is_qualified_name(self) -> bool:
        """
        Property assessing whether raw_id is qualified name.

        :returns: -
        """
        if self.atlas_qualified_name_regex.match(self._raw_identifier):
            return True
        else:
            return False

    @property
    def is_amundsen_key(self) -> bool:
        """
        Property assessing whether raw_id is amundsen key.

        :returns: -
        """
        if self.amundsen_key_regex.match(self._raw_identifier):
            return True
        else:
            return False

    def get_details(self) -> Dict[str, str]:
        """
        Collect as many details from key (either qn or amundsen key)

        :returns: dictionary of entity properties derived from key
        """
        if self.is_qualified_name:
            return self.get_details_from_qualified_name()
        elif self.is_amundsen_key:
            return self.get_details_from_key()
        else:
            raise ValueError(f'Value is neither valid qualified name nor amundsen key: {self._raw_identifier}')

    def _get_details(self, pattern: Any) -> Dict[str, str]:
        """
        Helper function collecting data from regex match

        :returns: dictionary of matched regex groups with their values
        """
        try:
            result = pattern.match(self._raw_identifier).groupdict()

            return result
        except KeyError:
            raise KeyError

    def get_details_from_qualified_name(self) -> Dict[str, str]:
        """
        Collect as many details from qualified name

        :returns: dictionary of entity properties derived from qualified name
        """
        try:
            return self._get_details(self.atlas_qualified_name_regex)
        except KeyError:
            raise ValueError(f'This is not valid qualified name: {self._raw_identifier}')

    def get_details_from_key(self) -> Dict[str, str]:
        """
        Collect as many details from amundsen key

        :returns: dictionary of entity properties derived from amundsen key
        """
        try:
            return self._get_details(self.amundsen_key_regex)
        except KeyError:
            raise ValueError(f'This is not valid qualified name: {self._raw_identifier}')

    @property
    @abc.abstractmethod
    def atlas_qualified_name_regex(self) -> Any:
        """
        Regex for validating qualified name (and collecting details from qn parts)

        :returns: -
        """
        pass

    @property
    @abc.abstractmethod
    def amundsen_key_regex(self) -> Any:
        """
        Regex for validating amundsen key (and collecting details from key parts)

        :returns: -
        """
        pass

    @property
    @abc.abstractmethod
    def qualified_name(self) -> str:
        """
        Properly formatted qualified name

        :returns: -
        """
        pass

    @property
    @abc.abstractmethod
    def amundsen_key(self) -> str:
        """
        Properly formetted amundsen key

        :returns: -
        """
        pass


class AtlasTableKey(AtlasKey):
    @property
    def atlas_qualified_name_regex(self) -> Any:
        return re.compile(r'^(?P<db>.*?)\.(?P<table>.*)@(?P<cluster>.*?)$', re.X)

    @property
    def amundsen_key_regex(self) -> Any:
        return re.compile(r'^(?P<source>.*?)://(?P<cluster>.*?)\.(?P<db>.*?)\/(?P<table>.*?)$', re.X)

    @property
    def qualified_name(self) -> str:
        if not self.is_qualified_name:
            spec = self.get_details_from_key()

            db = spec['db']
            table = spec['table']
            cluster = spec['cluster']

            return f'{db}.{table}@{cluster}'
        else:
            return self._raw_identifier

    @property
    def amundsen_key(self) -> str:
        if self.is_qualified_name:
            spec = self.get_details_from_qualified_name()

            db = spec['db']
            table = spec['table']
            cluster = spec['cluster']

            return f'{self._source}://{cluster}.{db}/{table}'
        elif self.is_amundsen_key:
            return self._raw_identifier
        else:
            raise ValueError(f'Value is neither qualified name nor amundsen key: {self._raw_identifier}')


class AtlasColumnKey(AtlasKey):
    @property
    def atlas_qualified_name_regex(self) -> Any:
        return re.compile(r'^(?P<db>.*?)\.(?P<table>.*)\.(?P<column>.*?)@(?P<cluster>.*?)$', re.X)

    @property
    def amundsen_key_regex(self) -> Any:
        return re.compile(r'^(?P<source>.*?)://(?P<cluster>.*?)\.(?P<db>.*?)\/(?P<table>.*?)\/(?P<column>.*)$', re.X)

    @property
    def qualified_name(self) -> str:
        if self.is_amundsen_key:
            spec = self.get_details_from_key()

            db = spec['db']
            table = spec['table']
            cluster = spec['cluster']
            column = spec['column']

            return f'{db}.{table}.{column}@{cluster}'
        elif self.is_qualified_name:
            return self._raw_identifier
        else:
            raise ValueError(f'Value is neither qualified name nor amundsen key: {self._raw_identifier}')

    @property
    def amundsen_key(self) -> str:
        if self.is_qualified_name:
            spec = self.get_details_from_qualified_name()

            db = spec['db']
            table = spec['table']
            cluster = spec['cluster']
            column = spec['column']

            source = self._source.replace('column', 'table') if self._source else ''

            return f'{source}://{cluster}.{db}/{table}/{column}'
        elif self.is_amundsen_key:
            return self._raw_identifier
        else:
            raise ValueError(f'Value is neither qualified name nor amundsen key: {self._raw_identifier}')
