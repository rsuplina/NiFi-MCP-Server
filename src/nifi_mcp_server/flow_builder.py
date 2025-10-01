"""
Flow Builder - Intelligent NiFi flow construction with requirements gathering

This module provides high-level flow building capabilities that understand
common data integration patterns and guide users through requirements.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class FlowRequirement:
    """Represents a requirement that must be gathered from the user"""
    name: str
    description: str
    required: bool = True
    default: Optional[str] = None
    validation: Optional[str] = None  # Validation hint
    example: Optional[str] = None
    

@dataclass
class FlowTemplate:
    """Template for building a specific type of flow"""
    name: str
    description: str
    requirements: List[FlowRequirement] = field(default_factory=list)
    processor_types: List[str] = field(default_factory=list)  # Processors needed
    

class FlowPatternLibrary:
    """Library of common NiFi flow patterns with their requirements"""
    
    @staticmethod
    def sql_server_to_iceberg() -> FlowTemplate:
        """SQL Server to Iceberg data migration flow"""
        return FlowTemplate(
            name="SQL Server to Iceberg",
            description="Extract data from SQL Server and write to Iceberg tables",
            requirements=[
                # SQL Server Source
                FlowRequirement(
                    name="sql_server_host",
                    description="SQL Server hostname or IP address",
                    example="sqlserver.example.com"
                ),
                FlowRequirement(
                    name="sql_server_port",
                    description="SQL Server port",
                    default="1433",
                    example="1433"
                ),
                FlowRequirement(
                    name="sql_server_database",
                    description="Database name to query",
                    example="ProductionDB"
                ),
                FlowRequirement(
                    name="sql_server_username",
                    description="SQL Server username",
                    example="nifi_user"
                ),
                FlowRequirement(
                    name="sql_server_password",
                    description="SQL Server password (will be stored as sensitive property)",
                    example="********"
                ),
                FlowRequirement(
                    name="sql_query",
                    description="SQL query to extract data",
                    example="SELECT * FROM customers WHERE updated_date > ?",
                    required=False
                ),
                FlowRequirement(
                    name="sql_table",
                    description="Table name (if not using custom query)",
                    example="customers",
                    required=False
                ),
                
                # Iceberg Target
                FlowRequirement(
                    name="iceberg_catalog_uri",
                    description="Iceberg catalog URI",
                    example="thrift://iceberg-catalog:9083"
                ),
                FlowRequirement(
                    name="iceberg_warehouse_path",
                    description="Iceberg warehouse path (S3 or HDFS)",
                    example="s3://my-bucket/warehouse"
                ),
                FlowRequirement(
                    name="iceberg_table_name",
                    description="Target Iceberg table name",
                    example="default.customers"
                ),
                
                # Flow Configuration
                FlowRequirement(
                    name="schedule_interval",
                    description="How often to run the flow",
                    default="1 hour",
                    example="5 min, 1 hour, 1 day"
                ),
                FlowRequirement(
                    name="batch_size",
                    description="Number of records per batch",
                    default="10000",
                    example="1000, 10000, 100000"
                ),
            ],
            processor_types=[
                "org.apache.nifi.processors.standard.GenerateTableFetch",
                "org.apache.nifi.processors.standard.ExecuteSQLRecord",
                "org.apache.nifi.dbcp.DBCPConnectionPool",
                "org.apache.nifi.serialization.JsonRecordSetWriter",
                "org.apache.nifi.serialization.AvroReader",
                # Iceberg processors would be custom/extension
                "org.apache.nifi.processors.iceberg.PutIceberg"
            ]
        )
    
    @staticmethod
    def kafka_to_s3() -> FlowTemplate:
        """Kafka to S3 data pipeline"""
        return FlowTemplate(
            name="Kafka to S3",
            description="Consume from Kafka and write to S3 with optional transformations",
            requirements=[
                FlowRequirement(
                    name="kafka_brokers",
                    description="Kafka broker addresses (comma-separated)",
                    example="broker1:9092,broker2:9092"
                ),
                FlowRequirement(
                    name="kafka_topic",
                    description="Kafka topic to consume from",
                    example="events"
                ),
                FlowRequirement(
                    name="kafka_consumer_group",
                    description="Consumer group ID",
                    example="nifi-s3-consumer"
                ),
                FlowRequirement(
                    name="s3_bucket",
                    description="S3 bucket name",
                    example="my-data-lake"
                ),
                FlowRequirement(
                    name="s3_prefix",
                    description="S3 key prefix/path",
                    example="raw/events/"
                ),
                FlowRequirement(
                    name="file_format",
                    description="Output file format",
                    default="parquet",
                    example="parquet, json, avro"
                ),
            ],
            processor_types=[
                "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
                "org.apache.nifi.processors.aws.s3.PutS3Object",
            ]
        )
    
    @staticmethod
    def rest_api_to_database() -> FlowTemplate:
        """REST API to Database integration"""
        return FlowTemplate(
            name="REST API to Database",
            description="Fetch data from REST API and load into database",
            requirements=[
                FlowRequirement(
                    name="api_url",
                    description="REST API endpoint URL",
                    example="https://api.example.com/v1/data"
                ),
                FlowRequirement(
                    name="api_auth_type",
                    description="Authentication type",
                    default="none",
                    example="none, bearer, basic"
                ),
                FlowRequirement(
                    name="api_token",
                    description="API token/key (if auth required)",
                    required=False,
                    example="Bearer xyz123..."
                ),
                FlowRequirement(
                    name="db_host",
                    description="Database hostname",
                    example="postgres.example.com"
                ),
                FlowRequirement(
                    name="db_port",
                    description="Database port",
                    example="5432"
                ),
                FlowRequirement(
                    name="db_name",
                    description="Database name",
                    example="analytics"
                ),
                FlowRequirement(
                    name="db_table",
                    description="Target table name",
                    example="api_data"
                ),
            ],
            processor_types=[
                "org.apache.nifi.processors.standard.InvokeHTTP",
                "org.apache.nifi.processors.standard.ConvertRecord",
                "org.apache.nifi.processors.standard.PutDatabaseRecord",
            ]
        )
    
    @staticmethod
    def file_watcher_to_processing() -> FlowTemplate:
        """Watch directory and process files"""
        return FlowTemplate(
            name="File Watcher to Processing",
            description="Monitor directory for files and process them",
            requirements=[
                FlowRequirement(
                    name="source_directory",
                    description="Directory to monitor for files",
                    example="/data/incoming"
                ),
                FlowRequirement(
                    name="file_pattern",
                    description="File pattern to match",
                    default=".*\\.csv",
                    example=".*\\.csv, .*\\.json, data-.*\\.txt"
                ),
                FlowRequirement(
                    name="target_directory",
                    description="Directory to write processed files",
                    example="/data/processed"
                ),
                FlowRequirement(
                    name="archive_directory",
                    description="Directory to archive original files",
                    example="/data/archive",
                    required=False
                ),
            ],
            processor_types=[
                "org.apache.nifi.processors.standard.GetFile",
                "org.apache.nifi.processors.standard.UpdateAttribute",
                "org.apache.nifi.processors.standard.PutFile",
            ]
        )
    
    @classmethod
    def get_template(cls, pattern_name: str) -> Optional[FlowTemplate]:
        """Get a flow template by name or description match"""
        patterns = {
            "sql_server_to_iceberg": cls.sql_server_to_iceberg,
            "sql_to_iceberg": cls.sql_server_to_iceberg,
            "database_to_iceberg": cls.sql_server_to_iceberg,
            "kafka_to_s3": cls.kafka_to_s3,
            "kafka_s3": cls.kafka_to_s3,
            "rest_api_to_database": cls.rest_api_to_database,
            "api_to_db": cls.rest_api_to_database,
            "file_watcher": cls.file_watcher_to_processing,
            "file_monitoring": cls.file_watcher_to_processing,
        }
        
        pattern_key = pattern_name.lower().replace(" ", "_").replace("-", "_")
        if pattern_key in patterns:
            return patterns[pattern_key]()
        
        # Try fuzzy matching
        for key, func in patterns.items():
            if key in pattern_key or pattern_key in key:
                return func()
        
        return None
    
    @classmethod
    def list_available_templates(cls) -> List[Dict[str, str]]:
        """List all available flow templates"""
        return [
            {"name": "SQL Server to Iceberg", "key": "sql_server_to_iceberg"},
            {"name": "Kafka to S3", "key": "kafka_to_s3"},
            {"name": "REST API to Database", "key": "rest_api_to_database"},
            {"name": "File Watcher to Processing", "key": "file_watcher"},
        ]


class FlowBuilderGuide:
    """Guides users through building flows by gathering requirements"""
    
    @staticmethod
    def identify_pattern(user_request: str) -> Optional[FlowTemplate]:
        """
        Analyze user request and identify matching flow pattern
        
        Examples:
            "Build a flow from SQL Server to Iceberg"
            "I need to move data from Kafka to S3"
            "Create a REST API to database pipeline"
        """
        request_lower = user_request.lower()
        
        # Pattern matching keywords
        if any(word in request_lower for word in ["sql server", "sqlserver", "mssql"]) and \
           any(word in request_lower for word in ["iceberg"]):
            return FlowPatternLibrary.get_template("sql_server_to_iceberg")
        
        if "kafka" in request_lower and "s3" in request_lower:
            return FlowPatternLibrary.get_template("kafka_to_s3")
        
        if any(word in request_lower for word in ["api", "rest", "http"]) and \
           any(word in request_lower for word in ["database", "db", "postgres", "mysql"]):
            return FlowPatternLibrary.get_template("rest_api_to_database")
        
        if any(word in request_lower for word in ["file", "directory", "watch", "monitor"]):
            return FlowPatternLibrary.get_template("file_watcher")
        
        return None
    
    @staticmethod
    def format_requirements_for_user(template: FlowTemplate) -> str:
        """
        Format requirements as a user-friendly prompt
        
        Returns a message that Claude can use to ask the user for information.
        """
        msg = f"To build a **{template.name}** flow, I need the following information:\n\n"
        msg += f"{template.description}\n\n"
        
        required_reqs = [r for r in template.requirements if r.required]
        optional_reqs = [r for r in template.requirements if not r.required]
        
        if required_reqs:
            msg += "**Required Information:**\n"
            for req in required_reqs:
                msg += f"- **{req.name.replace('_', ' ').title()}**: {req.description}\n"
                if req.example:
                    msg += f"  Example: `{req.example}`\n"
                if req.default:
                    msg += f"  Default: `{req.default}`\n"
            msg += "\n"
        
        if optional_reqs:
            msg += "**Optional Information:**\n"
            for req in optional_reqs:
                msg += f"- **{req.name.replace('_', ' ').title()}**: {req.description}\n"
                if req.example:
                    msg += f"  Example: `{req.example}`\n"
                if req.default:
                    msg += f"  Default: `{req.default}`\n"
            msg += "\n"
        
        msg += "Please provide these details and I'll build the flow for you!"
        return msg
    
    @staticmethod
    def validate_requirements(template: FlowTemplate, user_values: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Validate that all required values are provided
        
        Returns (is_valid, list_of_missing_fields)
        """
        missing = []
        
        for req in template.requirements:
            if req.required:
                value = user_values.get(req.name)
                if not value or value.strip() == "":
                    missing.append(req.name)
        
        return (len(missing) == 0, missing)


class FlowPositioner:
    """Helps position processors on the canvas in a visually pleasing way"""
    
    @staticmethod
    def linear_flow(processor_count: int, start_x: float = 100, start_y: float = 200, 
                    spacing_x: float = 350) -> List[Tuple[float, float]]:
        """Position processors in a horizontal line"""
        positions = []
        for i in range(processor_count):
            positions.append((start_x + (i * spacing_x), start_y))
        return positions
    
    @staticmethod
    def branching_flow(main_count: int, branch_count: int, 
                      start_x: float = 100, start_y: float = 200) -> List[Tuple[float, float]]:
        """Position processors for a flow with branching logic"""
        positions = []
        
        # Main flow (horizontal)
        for i in range(main_count):
            positions.append((start_x + (i * 350), start_y))
        
        # Branches (vertical from last main processor)
        branch_x = start_x + ((main_count - 1) * 350) + 350
        branch_spacing = 150
        branch_start_y = start_y - (branch_count * branch_spacing / 2)
        
        for i in range(branch_count):
            positions.append((branch_x, branch_start_y + (i * branch_spacing)))
        
        return positions


# Convenience function for MCP integration
def analyze_flow_request(user_request: str) -> Dict[str, Any]:
    """
    Analyze a user's flow building request and return guidance
    
    Returns:
        {
            'pattern_found': bool,
            'template': FlowTemplate or None,
            'requirements_prompt': str,
            'suggested_processors': List[str]
        }
    """
    template = FlowBuilderGuide.identify_pattern(user_request)
    
    if template:
        return {
            'pattern_found': True,
            'template_name': template.name,
            'template_description': template.description,
            'requirements_prompt': FlowBuilderGuide.format_requirements_for_user(template),
            'required_processors': template.processor_types,
            'requirement_count': len([r for r in template.requirements if r.required])
        }
    else:
        return {
            'pattern_found': False,
            'template_name': None,
            'available_templates': FlowPatternLibrary.list_available_templates(),
            'message': "I couldn't identify a specific pattern. Available templates: " + 
                      ", ".join([t['name'] for t in FlowPatternLibrary.list_available_templates()])
        }

