using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using D365MetadataService.Models;
using Microsoft.Dynamics.AX.Framework.Xlnt.XReference;
using Microsoft.Dynamics.Framework.Tools.Configuration;
using Newtonsoft.Json.Linq;
using Serilog;

namespace D365MetadataService.Services
{
    /// <summary>
    /// Service that wraps the D365 ICrossReferenceProvider API from
    /// Microsoft.Dynamics.AX.Framework.Xlnt.XReference.XReferenceProviders.dll
    /// to query cross-references without direct SQL database access.
    /// 
    /// The provider handles all database connectivity internally through the
    /// CrossReferenceProviderFactory, using the same SQL LocalDB instance that
    /// Visual Studio uses for cross-reference data.
    /// </summary>
    public class CrossReferenceService
    {
        private readonly ILogger _logger;
        private ICrossReferenceProvider _provider;
        private bool _isInitialized;
        private readonly object _initLock = new();

        // Expose the concrete type for extended features like GetPathsLikePattern
        private SqlDbCrossReferenceProvider _sqlProvider;

        public CrossReferenceService(ILogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Whether the cross-reference provider has been successfully initialized.
        /// </summary>
        public bool IsInitialized => _isInitialized;

        /// <summary>
        /// The name of the discovered XRef database, if initialized.
        /// </summary>
        public string DatabaseName => _sqlProvider?.DbName;

        /// <summary>
        /// Eagerly initialize the cross-reference provider at startup.
        /// Reads configuration from the D365 VS extension ConfigurationHelper
        /// to determine the correct server and database for the active environment
        /// (UDE uses LocalDB, CHE uses localhost/DYNAMICSXREFDB).
        /// </summary>
        public bool TryInitialize()
        {
            _logger.Information("[CrossReference] Attempting to initialize XRef provider via D365 ConfigurationHelper...");

            return EnsureInitialized();
        }

        /// <summary>
        /// Ensures the provider is initialized (thread-safe, lazy initialization).
        /// </summary>
        public bool EnsureInitialized()
        {
            if (_isInitialized)
                return true;

            lock (_initLock)
            {
                if (_isInitialized)
                    return true;

                try
                {
                    var (server, dbName) = ResolveXRefConfiguration();

                    if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(dbName))
                    {
                        _logger.Warning("[CrossReference] Could not determine XRef server/database from D365 configuration");
                        return false;
                    }

                    _logger.Information("[CrossReference] Creating provider - Server: {Server}, Database: {DbName}", server, dbName);

                    _provider = CrossReferenceProviderFactory.CreateSqlCrossReferenceProvider(server, dbName);

                    // Keep reference to concrete type for extended methods
                    _sqlProvider = _provider as SqlDbCrossReferenceProvider;

                    if (!_provider.IsXRefDbInitialized())
                    {
                        _logger.Warning("[CrossReference] XRef database {DbName} on {Server} is not initialized. " +
                            "Run 'Build > Update Cross References' in Visual Studio.", dbName, server);
                        return false;
                    }

                    _logger.Information("[CrossReference] Cross-reference provider initialized successfully");
                    _logger.Information("[CrossReference] Database: {DbName}, Server: {Server}",
                        _sqlProvider?.DbName ?? dbName, _sqlProvider?.Server ?? server);

                    _isInitialized = true;
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.Error(ex, "[CrossReference] Failed to initialize cross-reference provider");
                    return false;
                }
            }
        }

        /// <summary>
        /// Auto-detect which types an object exists as in the XRef database, then query
        /// cross-references for all discovered types in a single call.
        /// For example, "InventServiceLevel_BEC" might exist as both /Tables/... and /Forms/...
        /// This eliminates the AI guessing the object type and making multiple round-trips.
        /// </summary>
        public CrossReferenceResult FindReferencesAutoDetect(string objectName, string memberName,
            string direction, CrossReferenceKind kindFilter, int maxResults,
            bool includeMembers, int maxMemberResults)
        {
            var detectedPaths = AutoDetectObjectPaths(objectName);

            if (detectedPaths.Count == 0)
            {
                _logger.Warning("[CrossReference] Auto-detect found no matching types for '{ObjectName}'", objectName);
                return new CrossReferenceResult
                {
                    TargetPath = objectName,
                    Direction = direction,
                    KindFilter = kindFilter == CrossReferenceKind.Any ? "All" : kindFilter.ToString(),
                    DetectedTypes = Array.Empty<string>(),
                    TypeResults = new List<TypeCrossReferenceResult>()
                };
            }

            var detectedTypes = detectedPaths.Select(p => p.Split('/')[1]).ToArray();
            _logger.Information("[CrossReference] Auto-detected {Count} types for '{ObjectName}': {Types}",
                detectedPaths.Count, objectName, string.Join(", ", detectedTypes));

            var typeResults = new List<TypeCrossReferenceResult>();
            int totalFound = 0;
            int totalAvailable = 0;
            var allReferences = new List<CrossReferenceEntry>();
            var combinedSummary = new Dictionary<string, int>();

            foreach (var rootPath in detectedPaths)
            {
                var typePath = !string.IsNullOrEmpty(memberName) ? $"{rootPath}/Methods/{memberName}" : rootPath;
                var typeLabel = rootPath.Split('/')[1]; // e.g., "Tables", "Forms"

                CrossReferenceResult typeResult;
                if (includeMembers && string.IsNullOrEmpty(memberName))
                {
                    typeResult = FindReferencesWithMembers(typePath, direction, kindFilter, maxResults, maxMemberResults);
                }
                else
                {
                    typeResult = FindReferences(typePath, direction, kindFilter, maxResults);
                }

                totalFound += typeResult.TotalFound;
                totalAvailable += typeResult.TotalAvailable;
                allReferences.AddRange(typeResult.References);

                // Merge summary
                foreach (var kvp in typeResult.SummaryByKind)
                {
                    if (combinedSummary.ContainsKey(kvp.Key))
                        combinedSummary[kvp.Key] += kvp.Value;
                    else
                        combinedSummary[kvp.Key] = kvp.Value;
                }

                typeResults.Add(new TypeCrossReferenceResult
                {
                    TypePath = typePath,
                    DetectedType = typeLabel,
                    TotalFound = typeResult.TotalFound,
                    TotalAvailable = typeResult.TotalAvailable,
                    SummaryByKind = typeResult.SummaryByKind,
                    References = typeResult.References,
                    MemberResults = typeResult.MemberResults
                });
            }

            return new CrossReferenceResult
            {
                TargetPath = objectName,
                Direction = direction,
                KindFilter = kindFilter == CrossReferenceKind.Any ? "All" : kindFilter.ToString(),
                TotalFound = totalFound,
                TotalAvailable = totalAvailable,
                SummaryByKind = combinedSummary,
                References = allReferences,
                DetectedTypes = detectedTypes,
                TypeResults = typeResults
            };
        }

        /// <summary>
        /// Discovers which XRef root paths exist for an object name.
        /// Uses GetPathsLikePattern to search across all type prefixes.
        /// Returns paths like ["/Tables/CustTable", "/Forms/CustTable"].
        /// </summary>
        private List<string> AutoDetectObjectPaths(string objectName)
        {
            if (_sqlProvider == null)
            {
                _logger.Warning("[CrossReference] Cannot auto-detect types - SqlDbCrossReferenceProvider not available");
                return new List<string>();
            }

            try
            {
                // Search for all root-level paths ending with the object name
                // Pattern: /%/ObjectName  matches /Tables/ObjectName, /Classes/ObjectName, etc.
                var pattern = $"/%/{objectName}";
                var paths = _sqlProvider.GetPathsLikePattern(pattern)
                    .Where(p =>
                    {
                        // Ensure exact match: path should be /<Type>/<ObjectName> (3 segments)
                        var segments = p.Split('/');
                        return segments.Length == 3 && segments[2].Equals(objectName, StringComparison.OrdinalIgnoreCase);
                    })
                    .Distinct()
                    .OrderBy(p => p)
                    .ToList();

                return paths;
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "[CrossReference] Failed to auto-detect types for '{ObjectName}'", objectName);
                return new List<string>();
            }
        }

        /// <summary>
        /// Find references to/from an object path, optionally including all its members.
        /// When includeMembers=true, uses GetPathsLikePattern to discover actual member paths
        /// in the XRef database and fetches cross-references for each one in a single call.
        /// This eliminates round-trips from the AI guessing method names.
        /// </summary>
        public CrossReferenceResult FindReferencesWithMembers(string objectPath, string direction,
            CrossReferenceKind kindFilter, int maxResults, int maxMemberResults)
        {
            // First, get cross-references for the object itself
            var result = FindReferences(objectPath, direction, kindFilter, maxResults);

            // Discover actual members from the XRef database
            var memberPaths = DiscoverMemberPaths(objectPath);
            if (memberPaths.Count == 0)
            {
                _logger.Information("[CrossReference] No member paths found for {ObjectPath}", objectPath);
                result.MemberResults = new List<MemberCrossReferenceResult>();
                return result;
            }

            _logger.Information("[CrossReference] Discovered {Count} member paths for {ObjectPath}", memberPaths.Count, objectPath);

            var memberResults = new List<MemberCrossReferenceResult>();
            foreach (var memberPath in memberPaths)
            {
                try
                {
                    var memberName = ExtractMemberName(memberPath, objectPath);
                    var memberRefs = FindReferences(memberPath, direction, kindFilter, maxMemberResults);

                    memberResults.Add(new MemberCrossReferenceResult
                    {
                        MemberPath = memberPath,
                        MemberName = memberName,
                        TotalFound = memberRefs.TotalFound,
                        TotalAvailable = memberRefs.TotalAvailable,
                        SummaryByKind = memberRefs.SummaryByKind,
                        References = memberRefs.References
                    });
                }
                catch (Exception ex)
                {
                    _logger.Warning(ex, "[CrossReference] Failed to get references for member path: {MemberPath}", memberPath);
                }
            }

            result.MemberResults = memberResults;
            return result;
        }

        /// <summary>
        /// Discovers actual member paths (methods, fields, etc.) for an object
        /// by querying the XRef database with a LIKE pattern.
        /// For example, /Tables/CustTable -> finds /Tables/CustTable/Methods/find, etc.
        /// Only returns immediate children paths (methods/fields), not deeply nested ones.
        /// </summary>
        private List<string> DiscoverMemberPaths(string objectPath)
        {
            if (_sqlProvider == null)
            {
                _logger.Warning("[CrossReference] Cannot discover member paths - SqlDbCrossReferenceProvider not available");
                return new List<string>();
            }

            try
            {
                // Use SQL LIKE pattern to find all sub-paths
                var pattern = objectPath + "/%";
                var allPaths = _sqlProvider.GetPathsLikePattern(pattern).ToList();

                // Filter to only immediate member paths (e.g., /Tables/X/Methods/Y, /Tables/X/Fields/Y)
                // Exclude deeper nested paths (e.g., /Tables/X/Methods/Y/SubPath)
                var objectSegmentCount = objectPath.Split('/').Length;
                var memberPaths = allPaths
                    .Where(p =>
                    {
                        var segments = p.Split('/');
                        // We want paths like /Tables/X/Methods/Y (objectSegments + 2)
                        // i.e., one category level (Methods/Fields) + one name level
                        return segments.Length == objectSegmentCount + 2;
                    })
                    .Distinct()
                    .OrderBy(p => p)
                    .ToList();

                return memberPaths;
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "[CrossReference] Failed to discover member paths for {ObjectPath}", objectPath);
                return new List<string>();
            }
        }

        /// <summary>
        /// Extracts the member name from a full member path.
        /// Example: "/Tables/CustTable/Methods/find" with basePath "/Tables/CustTable" -> "Methods/find"
        /// </summary>
        private static string ExtractMemberName(string memberPath, string basePath)
        {
            if (memberPath.StartsWith(basePath + "/"))
            {
                return memberPath.Substring(basePath.Length + 1);
            }
            return memberPath;
        }

        /// <summary>
        /// Find references to/from an object path.
        /// For incoming (usedBy): pass sourcePath=null, targetPath=path
        /// For outgoing (uses): pass sourcePath=path, targetPath=null
        /// </summary>
        public CrossReferenceResult FindReferences(string objectPath, string direction, 
            CrossReferenceKind kindFilter, int maxResults)
        {
            var result = new CrossReferenceResult
            {
                TargetPath = objectPath,
                Direction = direction,
                KindFilter = kindFilter == CrossReferenceKind.Any ? "All" : kindFilter.ToString()
            };

            string sourcePath = null;
            string targetPath = null;

            if (direction?.ToLowerInvariant() == "uses")
            {
                sourcePath = objectPath;
            }
            else
            {
                targetPath = objectPath;
            }

            var refs = _provider.FindReferences(sourcePath, targetPath, kindFilter);

            var entries = new List<CrossReferenceEntry>();
            int totalCount = 0;

            foreach (var xref in refs)
            {
                totalCount++;
                if (entries.Count < maxResults)
                {
                    entries.Add(new CrossReferenceEntry
                    {
                        SourcePath = xref.SourcePath,
                        TargetPath = xref.TargetPath,
                        Kind = xref.Kind.ToString(),
                        Line = xref.LineNumber,
                        Column = xref.ColumnNumber
                    });
                }
            }

            result.References = entries;
            result.TotalFound = entries.Count;
            result.TotalAvailable = totalCount;

            // Group by kind for summary
            result.SummaryByKind = entries
                .GroupBy(r => r.Kind)
                .ToDictionary(g => g.Key, g => g.Count());

            return result;
        }

        /// <summary>
        /// Get all classes that extend a given class.
        /// </summary>
        public IEnumerable<string> GetClassExtendedReference(string className)
        {
            return _provider.GetClassExtendedReference(className);
        }

        /// <summary>
        /// Get all classes that implement a given interface.
        /// </summary>
        public IEnumerable<string> GetInterfaceImplementedReference(string interfaceName)
        {
            return _provider.GetInterfaceImplementedReference(interfaceName);
        }

        /// <summary>
        /// Get all classes that have a given attribute.
        /// </summary>
        public IEnumerable<string> GetClassesWithAttribute(string attributeName)
        {
            return _provider.GetClassesWithAttribute(attributeName);
        }

        /// <summary>
        /// Find paths matching a LIKE pattern (extended feature from SqlDbCrossReferenceProvider).
        /// </summary>
        public IEnumerable<string> GetPathsLikePattern(string pattern)
        {
            if (_sqlProvider != null)
            {
                return _sqlProvider.GetPathsLikePattern(pattern);
            }

            _logger.Warning("[CrossReference] GetPathsLikePattern not available - provider is not SqlDbCrossReferenceProvider");
            return Enumerable.Empty<string>();
        }

        /// <summary>
        /// Find references to tags.
        /// </summary>
        public IEnumerable<CrossReference> FindTagReferences(string tag)
        {
            return _provider.FindTagReferences(tag);
        }

        /// <summary>
        /// Get all available tags.
        /// </summary>
        public IEnumerable<string> GetTags()
        {
            return _provider.GetTags();
        }

        /// <summary>
        /// Get all module names stored in the XRef database.
        /// </summary>
        public IEnumerable<string> SelectAllDbModules()
        {
            return _provider.SelectAllDbModules();
        }

        #region Configuration Resolution

        /// <summary>
        /// Resolves the XRef server and database name from the D365 VS extension configuration.
        /// 
        /// Strategy:
        /// 1. Try ConfigurationHelper.CurrentConfiguration (works when VS sets it)
        /// 2. If defaults are returned (CHE fallback), check InstalledConfigurationEntries
        ///    for a "CurrentMetadataConfig" JSON file (UDE environments store config as JSON)
        /// 3. Use the resolved values directly - no manual database discovery needed
        /// 
        /// UDE environments: Server = "(LocalDB)\MSSQLLocalDB", DB = "XRef_*" 
        /// CHE environments: Server = "localhost", DB = "DYNAMICSXREFDB"
        /// </summary>
        private (string Server, string DbName) ResolveXRefConfiguration()
        {
            // Strategy 1: Try the metadata config JSON file (UDE environments)
            // The InstalledConfigurationEntries contain a "CurrentMetadataConfig" entry
            // that points to a JSON config file with the UDE-specific XRef settings.
            try
            {
                var entries = ConfigurationHelper.InstalledConfigurationEntries;
                if (entries != null)
                {
                    foreach (var entry in entries)
                    {
                        if (entry.Item1 == "CurrentMetadataConfig" && !string.IsNullOrEmpty(entry.Item2))
                        {
                            var configFilePath = entry.Item2;
                            _logger.Information("[CrossReference] Found CurrentMetadataConfig: {Path}", configFilePath);

                            if (File.Exists(configFilePath) && configFilePath.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                            {
                                var result = ReadMetadataConfigJson(configFilePath);
                                if (!string.IsNullOrEmpty(result.Server) && !string.IsNullOrEmpty(result.DbName))
                                {
                                    _logger.Information("[CrossReference] Resolved from metadata config JSON - Server: {Server}, Database: {DbName}",
                                        result.Server, result.DbName);
                                    return result;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "[CrossReference] Could not read InstalledConfigurationEntries, trying CurrentConfiguration");
            }

            // Strategy 2: Fall back to ConfigurationHelper.CurrentConfiguration
            // This returns the active DevelopmentConfiguration which works for CHE
            // and may also work for UDE if VS has set it properly.
            try
            {
                var devConfig = ConfigurationHelper.CurrentConfiguration;
                if (devConfig != null)
                {
                    var server = devConfig.CrossReferencesDbServerName;
                    var dbName = devConfig.CrossReferencesDatabaseName;

                    if (!string.IsNullOrEmpty(server) && !string.IsNullOrEmpty(dbName))
                    {
                        _logger.Information("[CrossReference] Resolved from ConfigurationHelper.CurrentConfiguration - " +
                            "Server: {Server}, Database: {DbName}",
                            server, dbName);
                        return (server, dbName);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "[CrossReference] ConfigurationHelper.CurrentConfiguration failed");
            }

            _logger.Warning("[CrossReference] Could not resolve XRef configuration from D365 ConfigurationHelper. " +
                "Ensure the D365 Finance and Operations extension is installed in Visual Studio " +
                "and a development configuration is active.");
            return (null, null);
        }

        /// <summary>
        /// Reads the UDE metadata configuration JSON file to extract XRef server and database settings.
        /// UDE environments store their config as JSON (not XML like CHE environments).
        /// </summary>
        private (string Server, string DbName) ReadMetadataConfigJson(string filePath)
        {
            try
            {
                var json = File.ReadAllText(filePath);
                var config = JObject.Parse(json);

                var server = config["CrossReferencesDbServerName"]?.ToString();
                var dbName = config["CrossReferencesDatabaseName"]?.ToString();

                if (!string.IsNullOrEmpty(server) && !string.IsNullOrEmpty(dbName))
                {
                    _logger.Information("[CrossReference] Parsed metadata config JSON - Server: {Server}, Database: {DbName}", server, dbName);
                    return (server, dbName);
                }

                _logger.Warning("[CrossReference] Metadata config JSON missing CrossReferencesDbServerName or CrossReferencesDatabaseName");
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "[CrossReference] Failed to parse metadata config JSON: {Path}", filePath);
            }

            return (null, null);
        }

        #endregion
    }
}
