import unittest

from metadata_service.proxy.atlas_utils import AtlasColumnKey, AtlasTableKey


class TestAtlasUtils(unittest.TestCase):
    def test_table_key(self) -> None:
        params = [
            ('hive_table://gold.database_name/table_name',
             None,
             'hive_table://gold.database_name/table_name',
             'database_name.table_name@gold',
             dict(source='hive_table', cluster='gold', db='database_name', table='table_name'),
             False,
             True),
            ('database_name.table_name@gold',
             'hive_table',
             'hive_table://gold.database_name/table_name',
             'database_name.table_name@gold',
             dict(cluster='gold', db='database_name', table='table_name'),
             True,
             False)
        ]

        for key, source, amundsen_key, qualified_name, details, is_key_qualified_name, is_key_amundsen_key in params:
            with self.subTest():
                result = AtlasTableKey(key, source=source)

                self.assertEqual(result.amundsen_key, amundsen_key)
                self.assertEqual(result.qualified_name, qualified_name)
                self.assertEqual(result.is_qualified_name, is_key_qualified_name)
                self.assertEqual(result.is_amundsen_key, is_key_amundsen_key)
                self.assertDictEqual(result.get_details(), details)

    def test_table_column_key(self) -> None:
        params = [
            ('hive_table://gold.database_name/table_name/column_name',
             None,
             'hive_table://gold.database_name/table_name/column_name',
             'database_name.table_name.column_name@gold',
             dict(source='hive_table', cluster='gold', db='database_name', table='table_name', column='column_name'),
             False,
             True),
            ('database_name.table_name.column_name@gold',
             'hive_table',
             'hive_table://gold.database_name/table_name/column_name',
             'database_name.table_name.column_name@gold',
             dict(cluster='gold', db='database_name', table='table_name', column='column_name'),
             True,
             False)
        ]

        for key, source, amundsen_key, qualified_name, details, is_key_qualified_name, is_key_amundsen_key in params:
            with self.subTest():
                result = AtlasColumnKey(key, source=source)

                self.assertEqual(result.amundsen_key, amundsen_key)
                self.assertEqual(result.qualified_name, qualified_name)
                self.assertEqual(result.is_qualified_name, is_key_qualified_name)
                self.assertEqual(result.is_amundsen_key, is_key_amundsen_key)
                self.assertDictEqual(result.get_details(), details)


if __name__ == '__main__':
    unittest.main()
