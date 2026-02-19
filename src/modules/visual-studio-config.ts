// =============================================================================
// VISUAL STUDIO D365 EXTENSION CONFIGURATION
// =============================================================================
// Helper module to access VS D365 extension path configuration
// Supports VS2022 and VS2026+

import { AppConfig } from "./app-config.js";
import { join } from "path";
import { promises as fs } from "fs";
import { existsSync } from "fs";

// Supported Visual Studio versions (newest first for priority detection)
// VS2026 uses internal version "18" as folder name, VS2022 uses "2022"
const VS_VERSIONS = ["18", "2022"];
const VS_EDITIONS = ["Professional", "Enterprise", "Community"];
const VS_PROGRAM_DIRS = ["C:\\Program Files\\Microsoft Visual Studio", "C:\\Program Files (x86)\\Microsoft Visual Studio"];

/**
 * Build all candidate VS extension paths for supported versions
 */
function buildVSExtensionSearchPaths(): string[] {
  const paths: string[] = [];
  for (const version of VS_VERSIONS) {
    for (const programDir of VS_PROGRAM_DIRS) {
      for (const edition of VS_EDITIONS) {
        paths.push(join(programDir, version, edition, "Common7", "IDE", "Extensions"));
      }
    }
  }
  return paths;
}

/**
 * Auto-detect VS D365 extension GUID by scanning the Extensions directory
 * Scans VS2026 first, then VS2022 fallback
 */
export async function autoDetectVSExtensionPath(): Promise<string | undefined> {
  const commonPaths = buildVSExtensionSearchPaths();

  for (const basePath of commonPaths) {
    if (!existsSync(basePath)) {
      continue;
    }

    try {
      const directories = await fs.readdir(basePath, { withFileTypes: true });
      
      for (const dir of directories) {
        if (!dir.isDirectory()) {
          continue;
        }

        // Check if this directory contains D365 extension markers
        const extensionPath = join(basePath, dir.name);
        const d365TemplatesPath = join(extensionPath, "Templates", "ProjectItems", "FinanceOperations", "Dynamics 365 Items");
        
        if (existsSync(d365TemplatesPath)) {
          // Verify it contains D365 template structure (directories with ZIP files)
          try {
            const templateDirs = await fs.readdir(d365TemplatesPath, { withFileTypes: true });
            
            for (const dir of templateDirs) {
              if (dir.isDirectory()) {
                // Check if this directory contains ZIP template files
                try {
                  const dirPath = join(d365TemplatesPath, dir.name);
                  const files = await fs.readdir(dirPath);
                  const hasZipFiles = files.some(file => file.endsWith('.zip'));
                  
                  if (hasZipFiles) {
                    return extensionPath;
                  }
                } catch {
                  // Continue checking other directories
                  continue;
                }
              }
            }
          } catch {
            // Continue searching if we can't read this directory
            continue;
          }
        }
      }
    } catch {
      // Continue with next path if we can't read this directory
      continue;
    }
  }

  return undefined;
}

/**
 * Get VS extension path with auto-detection fallback
 */
export async function getVSExtensionPathWithAutoDetect(): Promise<string | undefined> {
  // First try the configured path
  const configuredPath = getVSExtensionPath();
  if (configuredPath && existsSync(configuredPath)) {
    return configuredPath;
  }

  // If not configured or doesn't exist, try auto-detection
  return await autoDetectVSExtensionPath();
}

/**
 * Get the configured VS extension path
 */
export function getVSExtensionPath(): string | undefined {
  return AppConfig.getVSExtensionPath();
}

/**
 * Get the full path to the VS templates directory with auto-detection
 */
export async function getVSTemplatesPath(): Promise<string | undefined> {
  const extensionPath = await getVSExtensionPathWithAutoDetect();
  if (!extensionPath) {
    return undefined;
  }
  
  return join(extensionPath, "Templates", "ProjectItems", "FinanceOperations", "Dynamics 365 Items");
}

/**
 * Get the full path to the VS templates directory (synchronous - uses configured path only)
 */
export function getVSTemplatesPathSync(): string | undefined {
  const extensionPath = getVSExtensionPath();
  if (!extensionPath) {
    return undefined;
  }
  
  return join(extensionPath, "Templates", "ProjectItems", "FinanceOperations", "Dynamics 365 Items");
}

/**
 * Get the full path to a VS template ZIP file
 * @param templateName - Name of the template (e.g., "Class", "Table", "Form")
 * @returns Full path to the template ZIP file if VS extension path is configured
 */
export async function getVSTemplatePath(templateName: string): Promise<string | undefined> {
  const templatesPath = await getVSTemplatesPath();
  if (!templatesPath) {
    return undefined;
  }
  
  return join(templatesPath, `${templateName}.zip`);
}

/**
 * Get the path to extracted template icon
 * @param templateName - Name of the template
 * @param iconFileName - Name of the icon file (with extension)
 * @returns Path to the icon file if available
 */
export async function getVSIconPath(templateName: string, iconFileName: string): Promise<string | undefined> {
  const templatesPath = await getVSTemplatesPath();
  if (!templatesPath) {
    return undefined;
  }
  
  // Icons are typically in the same directory structure as templates
  return join(templatesPath, templateName, iconFileName);
}

/**
 * Check if VS extension path is configured and accessible
 */
export async function isVSExtensionAvailable(): Promise<boolean> {
  const templatesPath = await getVSTemplatesPath();
  if (!templatesPath) {
    return false;
  }
  
  try {
    await fs.access(templatesPath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Get available VS template names by scanning the extension directory
 */
export async function getAvailableVSTemplates(): Promise<string[]> {
  const templatesPath = await getVSTemplatesPath();
  if (!templatesPath) {
    return [];
  }
  
  try {
    const files = await fs.readdir(templatesPath, { recursive: true });
    return files
      .filter(file => file.endsWith('.zip'))
      .map(file => file.replace('.zip', ''))
      .sort();
  } catch {
    return [];
  }
}

/**
 * VS Template Categories mapping for enhanced organization
 */
export const VS_TEMPLATE_CATEGORIES = {
  'Analytics': ['AggregateDataEntity', 'AggregateDimension', 'AggregateMeasurement', 'KPI'],
  'Business Process and Workflow': ['WorkflowApproval', 'WorkflowAutomatedTask', 'WorkflowCategory', 'WorkflowTask', 'WorkflowType'],
  'Code': ['Class', 'Interface', 'Macro', 'RunnableClass', 'TestClass'],
  'Configuration': ['ConfigKey', 'ConfigKeyGroup', 'LicenseCode'],
  'Data Model': ['CompositeDataEntityView', 'DataEntityView', 'Map', 'Query', 'Table', 'TableCollection', 'View'],
  'Data Types': ['BaseEnum', 'EdtString', 'EdtInt', 'EdtReal'],
  'Labels And Resources': ['LabelFiles', 'Resource', 'PCFControlResource'],
  'Reports': ['Report', 'ReportEmbeddedImage'],
  'Security': ['SecurityDuty', 'SecurityPolicy', 'SecurityPrivilege', 'SecurityRole'],
  'Services': ['Service', 'ServiceGroup'],
  'User Interface': ['Form', 'Menu', 'MenuItemAction', 'MenuItemDisplay', 'MenuItemOutput', 'Tile']
};

/**
 * Get the category for a given template name
 */
export function getTemplateCategory(templateName: string): string | undefined {
  for (const [category, templates] of Object.entries(VS_TEMPLATE_CATEGORIES)) {
    if ((templates as string[]).includes(templateName)) {
      return category;
    }
  }
  return undefined;
}
