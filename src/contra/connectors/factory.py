from contra.connectors.filesystem import FilesystemConnector
from contra.connectors.s3 import S3Connector


class ConnectorFactory:
    """Factory to select the correct connector based on data source."""

    @staticmethod
    def from_source(source: str):
        source_lower = source.lower()

        if source_lower.startswith("s3://"):
            from contra.connectors.s3 import S3Connector
            return S3Connector()
        # elif source_lower.startswith("postgres://") or source_lower.startswith("snowflake://"):
        #     from contra.connectors.sql import SQLConnector
        #     return SQLConnector()
        else:
            return FilesystemConnector()
        

