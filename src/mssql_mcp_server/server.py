import asyncio
import logging
import os
import aioodbc
from aioodbc.pool import create_pool
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

# Database connection pool
db_pool = None  # Will be initialized during startup

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

async def init_db_pool(connection_string):
    """Initialize the database connection pool."""
    # Configure pool size based on environment variables or use reasonable defaults
    min_size = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
    max_size = int(os.getenv("DB_POOL_MAX_SIZE", "20"))
    
    logger.info(f"Initializing database connection pool (min={min_size}, max={max_size})")
    return await create_pool(
        dsn=connection_string,
        minsize=min_size,
        maxsize=max_size,
        echo=False,  # Set to True for detailed SQL logging (development only)
        pool_recycle=3600  # Recycle connections every hour to prevent stale connections
    )

async def execute_query(query, fetch_results=True, params=None):
    """Execute a query using the connection pool."""
    global db_pool
    
    if db_pool is None:
        logger.error("Database pool not initialized")
        raise RuntimeError("Database connection pool not initialized")
    
    try:
        async with db_pool.acquire() as conn:
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
    config, _ = get_db_config()
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
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
            )
            result = [f"Tables_in_{config['database']}"]  # Header
            result.extend([table[0] for table in tables])
            return [TextContent(type="text", text="\n".join(result))]
        
        # Regular SELECT queries
        elif query.strip().upper().startswith("SELECT"):
            columns, rows = await execute_query(query)
            result = [",".join(map(str, row)) for row in rows]
            return [TextContent(type="text", text="\n".join([",".join(columns)] + result))]
        
        # Non-SELECT queries
        else:
            _, rowcount = await execute_query(query, fetch_results=False)
            return [TextContent(type="text", text=f"Query executed successfully. Rows affected: {rowcount}")]
            
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error executing SQL '{query}': {error_message}")
        return [TextContent(type="text", text=f"Error executing query: {error_message}")]

async def shutdown_pool():
    """Gracefully close the connection pool."""
    global db_pool
    if db_pool:
        logger.info("Closing database connection pool")
        db_pool.close()
        await db_pool.wait_closed()
        db_pool = None
        logger.info("Database connection pool closed")

async def main():
    """Main entry point to run the MCP server."""
    from mcp.server.stdio import stdio_server
    
    global db_pool
    
    logger.info("Starting MSSQL MCP server...")
    try:
        config, connection_string = get_db_config()
        logger.info(f"Database config: {config['server']}/{config['database']} as {config['user']}")
        
        # Initialize the connection pool
        db_pool = await init_db_pool(connection_string)
        logger.info("Database connection pool initialized")
        
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
        # Ensure pool is closed on shutdown
        await shutdown_pool()
        logger.info("MSSQL MCP server stopped")

if __name__ == "__main__":
    asyncio.run(main())