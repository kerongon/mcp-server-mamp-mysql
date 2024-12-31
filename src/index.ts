#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListResourcesRequestSchema,
  ListResourceTemplatesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import mysql from 'mysql2/promise';

interface TableRow {
  TABLE_NAME: string;  // Changed from table_name to TABLE_NAME to match MySQL's actual column name
}

interface ColumnRow {
  COLUMN_NAME: string;  // Changed to match MySQL's actual column names
  DATA_TYPE: string;
}

// Get environment variables
const {
  MYSQL_HOST,
  MYSQL_PORT,
  MYSQL_USER,
  MYSQL_PASS,
  MYSQL_DB,
  MYSQL_SOCKET,
  MYSQL_POOL_LIMIT = '10'
} = process.env;

// Validate required environment variables
if (!MYSQL_USER) {
  throw new Error('MYSQL_USER environment variable is required');
}
if (!MYSQL_PASS) {
  throw new Error('MYSQL_PASS environment variable is required');
}
if (!MYSQL_DB) {
  throw new Error('MYSQL_DB environment variable is required');
}
if (!MYSQL_SOCKET && (!MYSQL_HOST || !MYSQL_PORT)) {
  throw new Error('Either MYSQL_SOCKET or both MYSQL_HOST and MYSQL_PORT environment variables are required');
}

class MampMysqlServer {
  private server: Server;
  private pool: mysql.Pool;

  constructor() {
    const config: mysql.PoolOptions = {
      user: MYSQL_USER,
      password: MYSQL_PASS,
      database: MYSQL_DB,
      connectionLimit: parseInt(MYSQL_POOL_LIMIT, 10),
    };

    // Use socket if provided, otherwise use host/port
    if (MYSQL_SOCKET) {
      config.socketPath = MYSQL_SOCKET;
    } else {
      config.host = MYSQL_HOST;
      config.port = parseInt(MYSQL_PORT!, 10);
    }

    this.pool = mysql.createPool(config);
    this.server = new Server(
      {
        name: 'mamp-mysql',
        version: '0.1.0',
      },
      {
        capabilities: {
          resources: {},
          tools: {},
        },
      }
    );

    this.setupResourceHandlers();
    this.setupToolHandlers();

    // Error handling
    this.server.onerror = (error) => console.error('[MCP Error]', error);
    process.on('SIGINT', async () => {
      await this.shutdown('SIGINT');
      process.exit(0);
    });
    process.on('SIGTERM', async () => {
      await this.shutdown('SIGTERM');
      process.exit(0);
    });
  }

  private async shutdown(signal: string) {
    console.log(`Received ${signal}. Shutting down...`);
    await this.pool.end();
  }

  private async executeQuery<T>(sql: string, params: any[] = []): Promise<T> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(sql, params);
      return rows as T;
    } finally {
      connection.release();
    }
  }

  private async executeReadOnlyQuery<T>(sql: string): Promise<T> {
    const connection = await this.pool.getConnection();
    try {
      // Set read-only mode
      await connection.execute("SET SESSION TRANSACTION READ ONLY");

      // Begin transaction
      await connection.beginTransaction();

      // Execute query
      const [rows] = await connection.execute(sql);

      // Rollback transaction (since it's read-only)
      await connection.rollback();

      // Reset to read-write mode
      await connection.execute("SET SESSION TRANSACTION READ WRITE");

      return rows as T;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  private setupResourceHandlers() {
    // List all tables as resources
    this.server.setRequestHandler(ListResourcesRequestSchema, async () => {
      try {
        const results = await this.executeQuery<TableRow[]>(
          'SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = ?',
          [MYSQL_DB]  // Use the actual database name from env
        );

        console.log('Query results:', results); // Debug log

        return {
          resources: results.map((row: TableRow) => ({
            uri: `mysql://${MYSQL_DB}/${row.TABLE_NAME}/schema`,
            mimeType: 'application/json',
            name: `"${row.TABLE_NAME}" database schema`,
          })),
        };
      } catch (error) {
        console.error('Error in ListResourcesRequestSchema:', error);
        throw error;
      }
    });

    // Resource templates
    this.server.setRequestHandler(
      ListResourceTemplatesRequestSchema,
      async () => ({
        resourceTemplates: [
          {
            uriTemplate: `mysql://${MYSQL_DB}/{table}/schema`,
            name: 'Table Schema',
            mimeType: 'application/json',
            description: 'Get schema information for a specific table',
          },
        ],
      })
    );

    // Read resource handler
    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      try {
        const resourceUrl = new URL(request.params.uri);
        const pathComponents = resourceUrl.pathname.split('/');
        const schema = pathComponents.pop();
        const tableName = pathComponents.pop();

        if (schema !== 'schema') {
          throw new Error('Invalid resource URI');
        }

        const results = await this.executeQuery<ColumnRow[]>(
          'SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = ? AND table_name = ?',
          [MYSQL_DB, tableName]
        );

        return {
          contents: [
            {
              uri: request.params.uri,
              mimeType: 'application/json',
              text: JSON.stringify(results, null, 2),
            },
          ],
        };
      } catch (error) {
        console.error('Error in ReadResourceRequestSchema:', error);
        throw error;
      }
    });
  }

  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: [
        {
          name: 'mysql_query',
          description: 'Run a read-only MySQL query',
          inputSchema: {
            type: 'object',
            properties: {
              sql: {
                type: 'string',
                description: 'SQL query to execute (SELECT only)',
              },
            },
            required: ['sql'],
          },
        },
      ],
    }));

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      if (request.params.name !== 'mysql_query') {
        throw new McpError(
          ErrorCode.MethodNotFound,
          `Unknown tool: ${request.params.name}`
        );
      }

      const args = request.params.arguments as { sql: string } | undefined;
      if (!args || typeof args.sql !== 'string') {
        throw new McpError(ErrorCode.InvalidParams, 'Invalid SQL query');
      }

      // Basic check to prevent write operations
      if (!args.sql.trim().toLowerCase().startsWith('select')) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          'Only SELECT queries are allowed'
        );
      }

      try {
        const results = await this.executeReadOnlyQuery(args.sql);
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(results, null, 2),
            },
          ],
        };
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `MySQL query error: ${error}`,
            },
          ],
          isError: true,
        };
      }
    });
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('MAMP MySQL MCP server running on stdio');
  }
}

const server = new MampMysqlServer();
server.run().catch(console.error);
