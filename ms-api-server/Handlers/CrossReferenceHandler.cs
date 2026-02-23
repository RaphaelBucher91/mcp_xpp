using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using D365MetadataService.Models;
using Serilog;

namespace D365MetadataService.Handlers
{
    /// <summary>
    /// Handler for cross-reference lookups against the D365 XRef SQL LocalDB database.
    /// Discovers the XRef database dynamically based on the application version from PackagesLocalDirectory.
    /// Supports finding: who calls a method, who references a type, class hierarchy, interface implementations, etc.
    /// </summary>
    public class CrossReferenceHandler : BaseRequestHandler
    {
        private readonly D365Configuration _config;
        private string _connectionString;
        private bool _isInitialized;
        private readonly object _initLock = new();

        // CrossReferenceKind enum values (from Microsoft.Dynamics.AX.Framework.Xlnt.XReference)
        private static class XRefKind
        {
            public const int Any = 0;
            public const int MethodCall = 1;
            public const int TypeReference = 2;
            public const int InterfaceImplementation = 3;
            public const int ClassExtended = 4;
            public const int TestCall = 5;
            public const int Property = 6;
            public const int Attribute = 7;
            public const int TestHelperCall = 8;
            public const int Tag = 9;
            public const int MethodOverride = 10;
        }

        private static readonly Dictionary<int, string> KindNames = new()
        {
            { XRefKind.Any, "Any" },
            { XRefKind.MethodCall, "MethodCall" },
            { XRefKind.TypeReference, "TypeReference" },
            { XRefKind.InterfaceImplementation, "InterfaceImplementation" },
            { XRefKind.ClassExtended, "ClassExtended" },
            { XRefKind.TestCall, "TestCall" },
            { XRefKind.Property, "Property" },
            { XRefKind.Attribute, "Attribute" },
            { XRefKind.TestHelperCall, "TestHelperCall" },
            { XRefKind.Tag, "Tag" },
            { XRefKind.MethodOverride, "MethodOverride" }
        };

        public CrossReferenceHandler(ServiceConfiguration config, ILogger logger) : base(logger)
        {
            _config = config?.D365Config ?? throw new ArgumentNullException(nameof(config));
        }

        public override string SupportedAction => "crossreference";

        /// <summary>
        /// Public method to eagerly initialize the cross-reference database connection at startup.
        /// Logs verbose details about the discovered database.
        /// </summary>
        public void TryInitialize()
        {
            Logger.Information("[CrossReference] Attempting to discover and connect to XRef database...");
            Logger.Information("[CrossReference] PackagesLocalDirectory: {Path}", _config.PackagesLocalDirectory ?? "(not set)");

            var initialized = EnsureInitialized();
            if (initialized)
            {
                Logger.Information("[CrossReference] Cross-reference database initialized successfully");
            }
            else
            {
                Logger.Warning("[CrossReference] Cross-reference database is NOT available. Cross-reference lookups will fail until the XRef database is built in Visual Studio (Build > Update Cross References).");
            }
        }

        protected override async Task<ServiceResponse> HandleRequestAsync(ServiceRequest request)
        {
            var validationError = ValidateRequest(request);
            if (validationError != null)
                return validationError;

            // Ensure XRef database is discovered and connected
            if (!EnsureInitialized())
            {
                return ServiceResponse.CreateError(
                    "Cross-reference database not available. Ensure the D365 XRef database has been built in Visual Studio (Build > Update Cross References).");
            }

            var parameters = request.Parameters;

            // Extract parameters
            var objectPath = parameters.ContainsKey("objectPath") ? parameters["objectPath"]?.ToString() : null;
            var objectType = parameters.ContainsKey("objectType") ? parameters["objectType"]?.ToString() : null;
            var objectName = parameters.ContainsKey("objectName") ? parameters["objectName"]?.ToString() : null;
            var memberName = parameters.ContainsKey("memberName") ? parameters["memberName"]?.ToString() : null;
            var referenceKind = parameters.ContainsKey("referenceKind") ? parameters["referenceKind"]?.ToString() : null;
            var direction = parameters.ContainsKey("direction") ? parameters["direction"]?.ToString() : "usedBy";
            var maxResults = parameters.ContainsKey("maxResults") ? Convert.ToInt32(parameters["maxResults"]) : 50;

            // Build the XRef path if not provided directly
            if (string.IsNullOrEmpty(objectPath))
            {
                objectPath = BuildXRefPath(objectType, objectName, memberName);
            }

            if (string.IsNullOrEmpty(objectPath))
            {
                return ServiceResponse.CreateError(
                    "Either 'objectPath' or 'objectType'+'objectName' must be provided. " +
                    "Example objectPath: '/Tables/CustTable/Methods/find', '/Classes/SalesFormLetter'");
            }

            // Parse reference kind filter
            int kindFilter = ParseReferenceKind(referenceKind);

            var kindName = kindFilter >= 0 ? GetKindName(kindFilter) : "All";
            Logger.Information("[CrossReference] Starting lookup - Object: {ObjectPath}, Direction: {Direction}, Kind: {Kind}, MaxResults: {Max}",
                objectPath, direction, kindName, maxResults);

            try
            {
                CrossReferenceResult result;

                if (direction?.ToLowerInvariant() == "uses")
                {
                    // What does this object USE/call?
                    result = await FindOutgoingReferences(objectPath, kindFilter, maxResults);
                }
                else
                {
                    // What USES this object? (default - "usedBy")
                    result = await FindIncomingReferences(objectPath, kindFilter, maxResults);
                }

                Logger.Information("[CrossReference] Completed lookup - Object: {ObjectPath}, Direction: {Direction}, ReferencesFound: {Found}, TotalAvailable: {Total}",
                    objectPath, direction, result.TotalFound, result.TotalAvailable);

                if (result.SummaryByKind != null && result.SummaryByKind.Count > 0)
                {
                    foreach (var kvp in result.SummaryByKind)
                    {
                        Logger.Information("[CrossReference]   - {Kind}: {Count}", kvp.Key, kvp.Value);
                    }
                }
                else
                {
                    Logger.Information("[CrossReference]   No references found for {ObjectPath}", objectPath);
                }

                return ServiceResponse.CreateSuccess(result);
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "[CrossReference] Failed to query cross-references for {ObjectPath}", objectPath);
                return ServiceResponse.CreateError($"Cross-reference query failed: {ex.Message}");
            }
        }

        #region XRef Database Discovery

        /// <summary>
        /// Discovers and connects to the XRef database.
        /// Prioritizes databases matching the version from PackagesLocalDirectory.
        /// Falls back to the most recently modified database.
        /// </summary>
        private bool EnsureInitialized()
        {
            if (_isInitialized)
                return true;

            lock (_initLock)
            {
                if (_isInitialized)
                    return true;

                try
                {
                    _connectionString = DiscoverXRefDatabase();
                    if (string.IsNullOrEmpty(_connectionString))
                    {
                        Logger.Warning("[CrossReference] No XRef database found on LocalDB");
                        return false;
                    }

                    // Verify connectivity
                    using var conn = new SqlConnection(_connectionString);
                    conn.Open();
                    Logger.Information("[CrossReference] Database connection verified successfully");
                    Logger.Information("[CrossReference] Connection string: {ConnectionString}",
                        _connectionString.Replace("Integrated Security=true", "***"));
                    _isInitialized = true;
                    return true;
                }
                catch (Exception ex)
                {
                    Logger.Error(ex, "[CrossReference] Failed to initialize XRef database connection");
                    return false;
                }
            }
        }

        /// <summary>
        /// Discovers the correct XRef database by:
        /// 1. Extracting version number from PackagesLocalDirectory path
        /// 2. Listing all XRef_* databases on LocalDB
        /// 3. Matching by version number (dots stripped)
        /// 4. Falling back to the latest database
        /// </summary>
        private string DiscoverXRefDatabase()
        {
            var version = ExtractVersionFromPath(_config.PackagesLocalDirectory);
            Logger.Information("[CrossReference] PackagesLocalDirectory path: {Path}", _config.PackagesLocalDirectory ?? "(not set)");
            Logger.Information("[CrossReference] Extracted version from path: {Version}", version ?? "(none - version could not be parsed)");

            var server = @"(localdb)\MSSQLLocalDB";
            Logger.Information("[CrossReference] Connecting to LocalDB instance: {Server}", server);
            var masterConn = $"Server={server};Database=master;Integrated Security=true";

            try
            {
                var xrefDatabases = new List<(string Name, DateTime Created)>();

                using (var conn = new SqlConnection(masterConn))
                {
                    conn.Open();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = "SELECT name, create_date FROM sys.databases WHERE name LIKE 'XRef_%' ORDER BY create_date DESC";
                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        xrefDatabases.Add((reader.GetString(0), reader.GetDateTime(1)));
                    }
                }

                if (xrefDatabases.Count == 0)
                {
                    Logger.Warning("[CrossReference] No XRef databases found on LocalDB instance. Ensure Visual Studio has built cross references (Build > Update Cross References).");
                    return null;
                }

                Logger.Information("[CrossReference] Found {Count} XRef database(s) on LocalDB:", xrefDatabases.Count);
                foreach (var db in xrefDatabases)
                {
                    Logger.Information("[CrossReference]   - {DbName} (created: {Created:yyyy-MM-dd HH:mm:ss})", db.Name, db.Created);
                }

                // Strategy 1: Match by version number (strip dots from version)
                if (!string.IsNullOrEmpty(version))
                {
                    var versionStripped = version.Replace(".", "");
                    var versionMatch = xrefDatabases.FirstOrDefault(d =>
                        d.Name.Contains(versionStripped));

                    if (!string.IsNullOrEmpty(versionMatch.Name))
                    {
                        Logger.Information("[CrossReference] Matched XRef database by version {Version}: {DbName}",
                            version, versionMatch.Name);
                        var connStr = $"Server={server};Database={versionMatch.Name};Integrated Security=true";
                        Logger.Information("[CrossReference] Using connection: {ConnectionString}", connStr.Replace("Integrated Security=true", "***"));
                        return connStr;
                    }

                    Logger.Warning("[CrossReference] No XRef database matched version {Version} (stripped: {Stripped}), falling back to latest",
                        version, versionStripped);
                }

                // Strategy 2: Use the most recently created database
                var latestDb = xrefDatabases.First();
                Logger.Information("[CrossReference] No version match available, using latest XRef database: {DbName} (created: {Created:yyyy-MM-dd HH:mm:ss})",
                    latestDb.Name, latestDb.Created);
                var fallbackConnStr = $"Server={server};Database={latestDb.Name};Integrated Security=true";
                Logger.Information("[CrossReference] Using connection: {ConnectionString}", fallbackConnStr.Replace("Integrated Security=true", "***"));
                return fallbackConnStr;
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "[CrossReference] Failed to discover XRef databases on LocalDB. Is SQL Server LocalDB installed?");
                return null;
            }
        }

        /// <summary>
        /// Extracts version number from a PackagesLocalDirectory path.
        /// Example: "C:\...\Dynamics365\10.0.2345.140\PackagesLocalDirectory" -> "10.0.2345.140"
        /// </summary>
        private static string ExtractVersionFromPath(string path)
        {
            if (string.IsNullOrEmpty(path))
                return null;

            // Walk up the path looking for a version-like segment (digits and dots)
            var parts = path.Replace('/', '\\').Split('\\');
            foreach (var part in parts)
            {
                // Match version pattern: digits.digits.digits.digits (with optional segments)
                if (System.Text.RegularExpressions.Regex.IsMatch(part, @"^\d+\.\d+\.\d+(\.\d+)?$"))
                {
                    return part;
                }
            }

            return null;
        }

        #endregion

        #region Cross-Reference Queries

        /// <summary>
        /// Find what references (uses) the target object path. ("Who calls/uses this?")
        /// </summary>
        private async Task<CrossReferenceResult> FindIncomingReferences(string targetPath, int kindFilter, int maxResults)
        {
            var result = new CrossReferenceResult
            {
                TargetPath = targetPath,
                Direction = "usedBy",
                KindFilter = kindFilter >= 0 ? GetKindName(kindFilter) : "All"
            };

            var query = @"
                SELECT TOP (@maxResults)
                    n_src.Path AS SourcePath,
                    n_tgt.Path AS TargetPath,
                    r.Kind,
                    r.[Line],
                    r.[Column]
                FROM [References] r
                JOIN [Names] n_src ON r.SourceId = n_src.Id
                JOIN [Names] n_tgt ON r.TargetId = n_tgt.Id
                WHERE n_tgt.Path = @targetPath" +
                (kindFilter >= 0 ? " AND r.Kind = @kind" : "") +
                " ORDER BY r.Kind, n_src.Path";

            await ExecuteReferenceQuery(query, result, targetPath, kindFilter, maxResults, isIncoming: true);

            return result;
        }

        /// <summary>
        /// Find what the source object path references/calls. ("What does this use?")
        /// </summary>
        private async Task<CrossReferenceResult> FindOutgoingReferences(string sourcePath, int kindFilter, int maxResults)
        {
            var result = new CrossReferenceResult
            {
                TargetPath = sourcePath,
                Direction = "uses",
                KindFilter = kindFilter >= 0 ? GetKindName(kindFilter) : "All"
            };

            var query = @"
                SELECT TOP (@maxResults)
                    n_src.Path AS SourcePath,
                    n_tgt.Path AS TargetPath,
                    r.Kind,
                    r.[Line],
                    r.[Column]
                FROM [References] r
                JOIN [Names] n_src ON r.SourceId = n_src.Id
                JOIN [Names] n_tgt ON r.TargetId = n_tgt.Id
                WHERE n_src.Path = @sourcePath" +
                (kindFilter >= 0 ? " AND r.Kind = @kind" : "") +
                " ORDER BY r.Kind, n_tgt.Path";

            await ExecuteReferenceQuery(query, result, sourcePath, kindFilter, maxResults, isIncoming: false);

            return result;
        }

        private async Task ExecuteReferenceQuery(string query, CrossReferenceResult result,
            string pathParam, int kindFilter, int maxResults, bool isIncoming)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            cmd.Parameters.AddWithValue("@maxResults", maxResults);

            if (isIncoming)
                cmd.Parameters.AddWithValue("@targetPath", pathParam);
            else
                cmd.Parameters.AddWithValue("@sourcePath", pathParam);

            if (kindFilter >= 0)
                cmd.Parameters.AddWithValue("@kind", kindFilter);

            // Use explicit using block so the reader is closed before the count query
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    var entry = new CrossReferenceEntry
                    {
                        SourcePath = reader["SourcePath"]?.ToString(),
                        TargetPath = reader["TargetPath"]?.ToString(),
                        Kind = GetKindName(Convert.ToInt32(reader["Kind"])),
                        Line = Convert.ToInt32(reader["Line"]),
                        Column = Convert.ToInt32(reader["Column"])
                    };

                    result.References.Add(entry);
                }
            } // Reader is closed here

            result.TotalFound = result.References.Count;

            // Get a count of total matches (if results were capped)
            if (result.TotalFound >= maxResults)
            {
                using var countCmd = conn.CreateCommand();
                countCmd.CommandText = isIncoming
                    ? "SELECT COUNT(*) FROM [References] r JOIN [Names] n ON r.TargetId = n.Id WHERE n.Path = @path" +
                      (kindFilter >= 0 ? " AND r.Kind = @kind" : "")
                    : "SELECT COUNT(*) FROM [References] r JOIN [Names] n ON r.SourceId = n.Id WHERE n.Path = @path" +
                      (kindFilter >= 0 ? " AND r.Kind = @kind" : "");
                countCmd.Parameters.AddWithValue("@path", pathParam);
                if (kindFilter >= 0)
                    countCmd.Parameters.AddWithValue("@kind", kindFilter);

                result.TotalAvailable = Convert.ToInt32(await countCmd.ExecuteScalarAsync());
            }
            else
            {
                result.TotalAvailable = result.TotalFound;
            }

            // Group by kind for summary
            result.SummaryByKind = result.References
                .GroupBy(r => r.Kind)
                .ToDictionary(g => g.Key, g => g.Count());
        }

        #endregion

        #region Path Building & Parsing

        /// <summary>
        /// Builds an XRef logical path from object type, name, and optional member.
        /// Maps D365 metadata types to XRef path prefixes.
        /// </summary>
        private static string BuildXRefPath(string objectType, string objectName, string memberName)
        {
            if (string.IsNullOrEmpty(objectName))
                return null;

            // Map object type to XRef path prefix
            var prefix = MapObjectTypeToXRefPrefix(objectType);
            if (string.IsNullOrEmpty(prefix))
                return null;

            // Labels use a special path format: /Labels/@LabelFileID:LabelID
            if (prefix == "Labels")
            {
                // Ensure the @ prefix is present
                var labelRef = objectName.StartsWith("@") ? objectName : "@" + objectName;
                return $"/Labels/{labelRef}";
            }

            var path = $"/{prefix}/{objectName}";

            if (!string.IsNullOrEmpty(memberName))
            {
                // Determine member type - methods are most common
                path += $"/Methods/{memberName}";
            }

            return path;
        }

        /// <summary>
        /// Maps D365 Ax* type names to XRef path prefixes.
        /// </summary>
        private static string MapObjectTypeToXRefPrefix(string objectType)
        {
            if (string.IsNullOrEmpty(objectType))
                return "Classes"; // Default to Classes

            // Normalize: strip "Ax" prefix if present
            var normalized = objectType.StartsWith("Ax", StringComparison.OrdinalIgnoreCase)
                ? objectType.Substring(2)
                : objectType;

            return normalized.ToLowerInvariant() switch
            {
                "class" => "Classes",
                "table" => "Tables",
                "form" => "Forms",
                "view" => "Views",
                "query" => "Queries",
                "enum" or "enumeration" => "Enums",
                "edt" or "extendeddatatype" => "Edts",
                "menuitemdisplay" => "MenuItemDisplays",
                "menuitemaction" => "MenuItemActions",
                "menuitemoutput" => "MenuItemOutputs",
                "menu" => "Menus",
                "map" => "Maps",
                "dataentityview" or "dataentity" => "DataEntityViews",
                "securityrole" => "SecurityRoles",
                "securityduty" => "SecurityDuties",
                "securityprivilege" => "SecurityPrivileges",
                "label" or "labelfile" or "labels" => "Labels",
                "report" => "Reports",
                "service" => "Services",
                "servicegroup" => "ServiceGroups",
                "tableextension" => "TableExtensions",
                "classextension" => "ClassExtensions",
                "formextension" => "FormExtensions",
                _ => normalized.EndsWith("s") ? normalized : normalized + "s"
            };
        }

        private static int ParseReferenceKind(string kind)
        {
            if (string.IsNullOrEmpty(kind))
                return -1; // Any

            return kind.ToLowerInvariant() switch
            {
                "any" or "all" => -1,
                "methodcall" or "method" or "call" => XRefKind.MethodCall,
                "typereference" or "type" or "reference" => XRefKind.TypeReference,
                "interfaceimplementation" or "interface" or "implements" => XRefKind.InterfaceImplementation,
                "classextended" or "extends" or "inheritance" => XRefKind.ClassExtended,
                "testcall" or "test" => XRefKind.TestCall,
                "property" => XRefKind.Property,
                "attribute" => XRefKind.Attribute,
                "methodoverride" or "override" => XRefKind.MethodOverride,
                "tag" => XRefKind.Tag,
                _ => int.TryParse(kind, out var numeric) ? numeric : -1
            };
        }

        private static string GetKindName(int kind)
        {
            return KindNames.TryGetValue(kind, out var name) ? name : $"Unknown({kind})";
        }

        #endregion
    }
}
