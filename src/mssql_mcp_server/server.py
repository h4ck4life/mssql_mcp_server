import asyncio
import logging
import os
import aioodbc
from mcp.server import Server
from mcp.types import Resource, Tool, TextContent
from pydantic import AnyUrl
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mssql_mcp_server")

# Constants for timeout handling
DEFAULT_QUERY_TIMEOUT = 120  # seconds
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "driver": os.getenv("MSSQL_DRIVER", "SQL Server"),
        "server": os.getenv("MSSQL_HOST", "localhost"),
        "user": os.getenv("MSSQL_USER"),
        "password": os.getenv("MSSQL_PASSWORD"),
        "database": os.getenv("MSSQL_DATABASE"),
        "query_timeout": int(os.getenv("MSSQL_QUERY_TIMEOUT", str(DEFAULT_QUERY_TIMEOUT)))
    }
    if not all([config["user"], config["password"], config["database"]]):
        logger.error("Missing required database configuration. Please check environment variables:")
        logger.error("MSSQL_USER, MSSQL_PASSWORD, and MSSQL_DATABASE are required")
        raise ValueError("Missing required database configuration")
    
    connection_string = (
        f"Driver={config['driver']};"
        f"Server={config['server']};"
        f"UID={config['user']};"
        f"PWD={config['password']};"
        f"Database={config['database']};"
        f"Connection Timeout={int(os.getenv('MSSQL_CONNECTION_TIMEOUT', '60'))};"
        f"Query Timeout={config['query_timeout']};"
    )

    return config, connection_string

@asynccontextmanager
async def safe_db_connection(connection_string, operation_name):
    """Async database connection context manager with retry logic."""
    attempts = 0
    last_error = None
    
    while attempts < MAX_RETRY_ATTEMPTS:
        try:
            connection = await aioodbc.connect(dsn=connection_string)
            try:
                yield connection
                return  # Success, exit the context manager
            finally:
                try:
                    await connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
        except Exception as e:
            last_error = e
            attempts += 1
            logger.warning(f"Database connection error during {operation_name} (attempt {attempts}/{MAX_RETRY_ATTEMPTS}): {e}")
            if attempts < MAX_RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_DELAY)  # Use async sleep
    
    # If we get here, all attempts failed
    logger.error(f"All database connection attempts failed for {operation_name}: {last_error}")
    raise RuntimeError(f"Database connection failed after {MAX_RETRY_ATTEMPTS} attempts: {last_error}")

async def execute_query(connection_string, query, fetch_results=True, params=None):
    """Execute a query with timeout handling - fully async version."""
    try:
        async with safe_db_connection(connection_string, f"query: {query[:50]}...") as conn:
            async with conn.cursor() as cursor:
                try:
                    # Set async timeout
                    await asyncio.wait_for(
                        cursor.execute(query, params or []),
                        timeout=DEFAULT_QUERY_TIMEOUT
                    )
                    
                    if fetch_results:
                        columns = [desc[0] for desc in cursor.description] if cursor.description else []
                        rows = await cursor.fetchall()
                        return columns, rows
                    else:
                        await conn.commit()
                        return None, cursor.rowcount
                except asyncio.TimeoutError:
                    logger.error(f"Query timed out after {DEFAULT_QUERY_TIMEOUT} seconds: {query[:100]}...")
                    raise RuntimeError("Query timed out. Please simplify your query or add more specific filters.")
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise

# Initialize server
app = Server("mssql_mcp_server")

# @app.list_resources()
# async def list_resources() -> list[Resource]:
#     """List MSSQL tables as resources."""
#     config, connection_string = get_db_config()
#     try:
#         columns, tables = await execute_query(
#             connection_string, 
#             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
#         )
#         
#         logger.info(f"Found {len(tables)} tables")
#         
#         resources = []
#         for table in tables:
#             table_name = table[0]  # Extract string from tuple
#             resources.append(
#                 Resource(
#                     uri=f"mssql://{table_name}/data",
#                     name=f"Table: {table_name}",
#                     mimeType="text/plain",
#                     description=f"Data in table: {table_name}"
#                 )
#             )
#         return resources
#     except Exception as e:
#         logger.error(f"Failed to list resources: {str(e)}")
#         # Return empty list instead of failing completely
#         return []

# @app.read_resource()
# async def read_resource(uri: AnyUrl) -> str:
#     """Read table contents."""
#     config, connection_string = get_db_config()
#     uri_str = str(uri)
#     logger.info(f"Reading resource: {uri_str}")
#     
#     if not uri_str.startswith("mssql://"):
#         raise ValueError(f"Invalid URI scheme: {uri_str}")
#         
#     parts = uri_str[8:].split('/')
#     table = parts[0]
#     
#     try:
#         columns, rows = await execute_query(
#             connection_string,
#             f"SELECT TOP 100 * FROM {table}"
#         )
#         
#         result = [",".join(map(str, row)) for row in rows]
#         return "\n".join([",".join(columns)] + result)
#     except Exception as e:
#         logger.error(f"Database error reading resource {uri}: {str(e)}")
#         return f"Error reading table {table}: {str(e)}"

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available MSSQL tools."""
    logger.info("Listing tools...")
    return [
        Tool(
            name="execute_sql",
            description="Execute an SQL query on the MSSQL server",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The SQL query to execute"
                    }
                },
                "required": ["query"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Execute SQL commands."""
    config, connection_string = get_db_config()
    logger.info(f"Calling tool: {name} with arguments: {arguments}")
    
    if name != "execute_sql":
        return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    query = arguments.get("query")
    if not query:
        return [TextContent(type="text", text="Query is required")]
    
    try:
        # Special handling for listing tables in MSSQL
        if query.strip().upper() == "SHOW TABLES":
            columns, tables = await execute_query(
                connection_string,
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
            )
            result = [f"Tables_in_{config['database']}"]  # Header
            result.extend([table[0] for table in tables])
            return [TextContent(type="text", text="\n".join(result))]
        
        # Regular SELECT queries
        elif query.strip().upper().startswith("SELECT"):
            columns, rows = await execute_query(connection_string, query)
            result = [",".join(map(str, row)) for row in rows]
            return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
        
        # Non-SELECT queries
        else:
            _, rowcount = await execute_query(connection_string, query, fetch_results=False)
            return [TextContent(type="text", text=f"Query executed successfully. Rows affected: {rowcount}")]
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error executing SQL '{query}': {error_message}")
        return [TextContent(type="text", text=f"Error executing query: {error_message}")]

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    logger.info("Starting MSSQL MCP server...")
    try:
        config, _ = get_db_config()
        logger.info(f"Database config: {config['server']}/{config['database']} as {config['user']}")
        
        async with stdio_server() as (read_stream, write_stream):
            try:
                await app.run(
                    read_stream,
                    write_stream,
                    app.create_initialization_options()
                )
            except Exception as e:
                logger.error(f"Server error: {str(e)}", exc_info=True)
                # Don't exit immediately, allow graceful restart
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Startup error: {str(e)}", exc_info=True)
    finally:
        logger.info("MSSQL MCP server stopped")

if __name__ == "__main__":
    asyncio.run(main())