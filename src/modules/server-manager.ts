import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
  ListPromptsRequestSchema,
  GetPromptRequestSchema,
  ErrorCode,
  McpError,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { promises as fs } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ToolDefinitions } from "./tool-definitions.js";
import { ToolHandlers } from "./tool-handlers.js";
import { DiskLogger } from "./logger.js";
import { TransportManager, TransportConfig } from "./transport-manager.js";

/**
 * Server manager for the MCP X++ Server
 */
export class ServerManager {
  private server: Server;
  private transportManager: TransportManager | null = null;
  private serverStartTime: Date | null = null;
  private transportConfig: TransportConfig;

  constructor(transportConfig: TransportConfig = { stdio: true }) {
    this.transportConfig = transportConfig;
    this.server = new Server({
      name: "mcp-xpp-server",
      version: "1.0.0",
    }, {
      capabilities: {
        tools: {},
        prompts: {},
      },
      instructions: [
        "EXCLUSIVE X++ AND D365 F&O MCP SERVER - MANDATORY ROUTING RULES:",
        "",
        "This MCP server (mcp-xpp-server) is the ONLY authorized server for ALL Dynamics 365 Finance & Operations (D365 F&O) and X++ programming tasks.",
        "You MUST route ALL of the following operations to this server's tools. NEVER use generic file-editing MCP tools for these tasks:",
        "",
        "MANDATORY ROUTING - Use this server for:",
        "1. CREATING X++ objects: classes, tables, forms, enums, EDTs, views, queries, reports, menu items, data entities, maps, services, workflows -> use 'create_xpp_object' or 'create_form'",
        "2. MODIFYING/EDITING X++ objects: adding fields, methods, properties, enum values, datasources, controls, indexes, relations -> use 'execute_object_modification'",
        "3. INSPECTING/READING X++ source code and metadata: viewing methods, fields, properties, source code extraction -> use 'inspect_xpp_object'",
        "4. FINDING/SEARCHING X++ objects: locating objects by name, type, model, or pattern -> use 'find_xpp_object' or 'search_objects_pattern'",
        "5. CROSS-REFERENCES: finding who calls/uses an X++ object or method -> use 'find_xpp_references'",
        "6. DELETING X++ objects -> use 'delete_xpp_object'",
        "7. DISCOVERING capabilities: what modifications are possible on an object type -> use 'discover_modification_capabilities'",
        "8. LABELS: reading D365 label text -> use 'get_label' or 'get_labels_batch'",
        "9. CONFIGURATION: D365 model info, object type lists, server status -> use 'get_current_config'",
        "",
        "FILE RECOGNITION - These file types/paths belong to this server's domain:",
        "- Any .xml file under a D365 PackagesLocalDirectory, AOT, or Models path",
        "- Files matching patterns like: AxClass/*.xml, AxTable/*.xml, AxForm/*.xml, AxEnum/*.xml, AxEdt/*.xml, AxView/*.xml, AxQuery/*.xml, etc.",
        "- X++ source code (.xpp) or X++ metadata definitions",
        "- D365 model descriptor files, label files (.label.txt), resource files in D365 model folders",
        "",
        "CRITICAL PROHIBITION:",
        "NEVER use generic file-system MCP tools (read_file, write_file, edit_file, create_file, etc.) to create, modify, or write D365 X++ metadata XML files.",
        "These XML files have complex interdependent structures (AOT metadata, axpp format) that MUST be manipulated through this server's specialized tools",
        "to ensure correct metadata structure, proper D365 integration, valid XML schema, and correct model/layer placement.",
        "Using generic file tools to edit X++ XML will produce invalid metadata that causes D365 build failures.",
        "",
        "WORKFLOW FOR X++ CODE CHANGES:",
        "1. Use 'inspect_xpp_object' to read current state (summary -> properties -> collections -> xppcode)",
        "2. Use 'discover_modification_capabilities' to see what changes are possible",
        "3. Use 'execute_object_modification' to apply changes (batch multiple modifications in one call)",
        "4. Use 'find_xpp_references' to verify impact of changes",
        "",
        "UPDATING EXISTING METHODS/FIELDS (IMPORTANT):",
        "The D365 metadata API does NOT have an 'UpdateMethod' or 'ReplaceMethod' operation.",
        "To UPDATE an existing method, you MUST use a two-step Remove+Add pattern in a SINGLE batch call to 'execute_object_modification':",
        "  Step 1: RemoveMethod (methodName: 'existingMethod') - removes the old version",
        "  Step 2: AddMethod (methodName: 'existingMethod', sourceCode: '...new code...') - adds the updated version",
        "Both operations MUST be in the same batch to avoid data loss.",
        "If AddMethod fails with a 'duplicate' or 'already exists' error, it means the method exists - use RemoveMethod first, then AddMethod.",
        "NEVER fall back to generic file-editing tools when encountering duplicate errors - always use the Remove+Add pattern.",
      ].join("\n"),
    });
  }

  /**
   * Get the server start time
   */
  getServerStartTime(): Date | null {
    return this.serverStartTime;
  }

  /**
   * Initialize the server with request handlers
   */
  async initialize(): Promise<void> {
    await this.setupRequestHandlers();
    this.transportManager = new TransportManager(this.server, this.transportConfig);
  }

  /**
   * Setup all request handlers
   */
  private async setupRequestHandlers(): Promise<void> {
    // Handle list tools request
    this.server.setRequestHandler(ListToolsRequestSchema, async (request) => {
      await DiskLogger.logRequest(request);
      
      const toolsResponse = await ToolDefinitions.getToolDefinitions();
      
      await DiskLogger.logResponse(toolsResponse, (request as any).id);
      return toolsResponse;
    });

    // Handle list prompts request
    this.server.setRequestHandler(ListPromptsRequestSchema, async () => {
      return {
        prompts: [
          {
            name: "d365-development-guidelines",
            description: "D365 F&O development guidelines including naming conventions, clean coding standards, feature management patterns, table/form setup rules, and best practices for X++ development.",
          },
          {
            name: "xpp-tool-routing",
            description: "IMPORTANT: Use this prompt at the start of every X++ development session. Sets up proper tool routing so all D365/X++ file operations (creating, editing, reading, deleting .xml metadata files) use the mcp-xpp-server tools instead of generic file tools.",
          },
        ],
      };
    });

    // Handle get prompt request
    this.server.setRequestHandler(GetPromptRequestSchema, async (request) => {
      if (request.params.name === "xpp-tool-routing") {
        return {
          description: "X++ Tool Routing Instructions for D365 F&O Development",
          messages: [
            {
              role: "user",
              content: {
                type: "text",
                text: [
                  "CRITICAL INSTRUCTION FOR THIS SESSION - X++ AND D365 TOOL ROUTING:",
                  "",
                  "For ALL operations involving D365 Finance & Operations (D365 F&O) and X++ development,",
                  "you MUST use ONLY the tools from the mcp-xpp-server (mcp_mcp-xpp-serve_*) MCP server.",
                  "NEVER use generic file-system tools to read, create, edit, or delete D365/X++ files.",
                  "",
                  "MANDATORY TOOL MAPPING:",
                  "- To READ/VIEW X++ code or .xml metadata files -> use mcp_mcp-xpp-serve_inspect_xpp_object",
                  "- To EDIT/MODIFY X++ code or .xml metadata files -> use mcp_mcp-xpp-serve_execute_object_modification", 
                  "- To CREATE new X++ objects or .xml metadata files -> use mcp_mcp-xpp-serve_create_xpp_object or mcp_mcp-xpp-serve_create_form",
                  "- To DELETE X++ objects or .xml metadata files -> use mcp_mcp-xpp-serve_delete_xpp_object",
                  "- To FIND/SEARCH for X++ objects -> use mcp_mcp-xpp-serve_find_xpp_object or mcp_mcp-xpp-serve_search_objects_pattern",
                  "- To find CROSS-REFERENCES -> use mcp_mcp-xpp-serve_find_xpp_references",
                  "- To discover what edits are possible -> use mcp_mcp-xpp-serve_discover_modification_capabilities",
                  "- To read LABELS -> use mcp_mcp-xpp-serve_get_label or mcp_mcp-xpp-serve_get_labels_batch",
                  "",
                  "FILE PATTERNS THAT BELONG TO mcp-xpp-server (NEVER use generic file tools on these):",
                  "- Any .xml file under PackagesLocalDirectory, AOT, or Models paths",
                  "- AxClass/*.xml, AxTable/*.xml, AxForm/*.xml, AxEnum/*.xml, AxEdt/*.xml, AxView/*.xml, AxQuery/*.xml",
                  "- .xpp source files, .label.txt files, D365 model descriptor files",
                  "",
                  "REASON: D365 metadata XML files have complex interdependent structures that generic file",
                  "tools cannot handle correctly. Using them produces invalid metadata causing build failures.",
                  "",
                  "WORKFLOW: inspect_xpp_object -> discover_modification_capabilities -> execute_object_modification",
                ].join("\n"),
              },
            },
          ],
        };
      }

      if (request.params.name !== "d365-development-guidelines") {
        throw new McpError(
          ErrorCode.InvalidParams,
          `Unknown prompt: ${request.params.name}`
        );
      }

      // Load guidelines from the docs/internal folder
      let guidelinesContent: string;
      try {
        // Resolve path relative to the build output
        const currentDir = dirname(fileURLToPath(import.meta.url));
        const guidelinesPath = join(currentDir, '..', '..', 'docs', 'internal', 'development-guidelines.md');
        guidelinesContent = await fs.readFile(guidelinesPath, 'utf-8');
      } catch (error) {
        throw new McpError(
          ErrorCode.InternalError,
          `Failed to load development guidelines: ${error instanceof Error ? error.message : 'Unknown error'}`
        );
      }

      return {
        description: "D365 F&O Development Guidelines (BEC)",
        messages: [
          {
            role: "user",
            content: {
              type: "text",
              text: `You must follow these D365 F&O development guidelines when generating or reviewing X++ code:\n\n${guidelinesContent}`,
            },
          },
        ],
      };
    });

    // Handle call tool request
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      await DiskLogger.logRequest(request);
      
      const { name, arguments: args } = request.params;
      
      try {
        const requestId = (request as any).id;
        
        switch (name) {
          case "create_xpp_object":
            return await ToolHandlers.createXppObject(args, requestId);
          
          case "create_form":
            return await ToolHandlers.createForm(args, requestId);
          
          case "delete_xpp_object":
            return await ToolHandlers.deleteXppObject(args, requestId);
          
          case "find_xpp_object":
            return await ToolHandlers.findXppObject(args, requestId);
          case "inspect_xpp_object":
            return await ToolHandlers.inspectXppObject(args, requestId);
          
          case "build_object_index":
            return await ToolHandlers.buildCache(args, requestId);
          
          case "get_current_config":
            return await ToolHandlers.getCurrentConfig(args, requestId);
          
          case "search_objects_pattern":
            return await ToolHandlers.searchObjectsPattern(args, requestId);
          
          case "discover_modification_capabilities":
            return await ToolHandlers.discoverModificationCapabilities(args, requestId);
          
          case "execute_object_modification":
            return await ToolHandlers.executeObjectModification(args, requestId);
          
          case "find_xpp_references":
            return await ToolHandlers.findXppReferences(args, requestId);

          case "get_label":
            return await ToolHandlers.getLabel(args, requestId);

          case "get_labels_batch":
            return await ToolHandlers.getLabelsBatch(args, requestId);

          default:
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${name}`
            );
        }
      } catch (error) {
        await DiskLogger.logError(error, name);
        
        if (error instanceof z.ZodError) {
          throw new McpError(
            ErrorCode.InvalidParams,
            `Invalid parameters: ${error.errors.map(e => `${e.path.join('.')}: ${e.message}`).join(', ')}`
          );
        }
        
        throw error instanceof McpError ? error : new McpError(
          ErrorCode.InternalError,
          error instanceof Error ? error.message : "An unexpected error occurred"
        );
      }
    });
  }

  /**
   * Start the server
   */
  async start(): Promise<void> {
    if (!this.transportManager) {
      throw new Error("Server not initialized. Call initialize() first.");
    }

    await this.transportManager.start();
    
    // Set server start time after successful connection
    this.serverStartTime = new Date();
    
    await DiskLogger.logDebug("[ServerManager] MCP X++ Server started with configured transports");
  }

  /**
   * Stop the server
   */
  async stop(): Promise<void> {
    if (this.transportManager) {
      await this.transportManager.stop();
    }
  }

  /**
   * Get transport status
   */
  getTransportStatus() {
    return this.transportManager?.getStatus() || null;
  }
}
