using System;
using System.Threading.Tasks;
using D365MetadataService.Models;
using D365MetadataService.Services;
using Microsoft.Dynamics.AX.Framework.Xlnt.XReference;
using Serilog;

namespace D365MetadataService.Handlers
{
    /// <summary>
    /// Handler for cross-reference lookups using the D365 ICrossReferenceProvider API
    /// from Microsoft.Dynamics.AX.Framework.Xlnt.XReference.XReferenceProviders.dll.
    /// 
    /// Delegates all database access to the CrossReferenceService which wraps the
    /// CrossReferenceProviderFactory API - the same API Visual Studio uses internally.
    /// No direct SQL access is performed by this handler.
    /// </summary>
    public class CrossReferenceHandler : BaseRequestHandler
    {
        private readonly CrossReferenceService _crossRefService;

        public CrossReferenceHandler(CrossReferenceService crossRefService, ILogger logger) : base(logger)
        {
            _crossRefService = crossRefService ?? throw new ArgumentNullException(nameof(crossRefService));
        }

        public override string SupportedAction => "crossreference";

        protected override async Task<ServiceResponse> HandleRequestAsync(ServiceRequest request)
        {
            var validationError = ValidateRequest(request);
            if (validationError != null)
                return validationError;

            // Ensure XRef provider is initialized
            if (!_crossRefService.EnsureInitialized())
            {
                return ServiceResponse.CreateError(
                    "Cross-reference database not available. Ensure the D365 XRef database has been built " +
                    "in Visual Studio (Build > Update Cross References).");
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
            var includeMembers = parameters.ContainsKey("includeMembers") && Convert.ToBoolean(parameters["includeMembers"]);
            var maxMemberResults = parameters.ContainsKey("maxMemberResults") ? Convert.ToInt32(parameters["maxMemberResults"]) : 20;

            // Parse reference kind filter
            var kindFilter = ParseReferenceKind(referenceKind);

            // Auto-detect mode: when no objectType and no objectPath are provided,
            // discover which types the object exists as in the XRef database
            if (string.IsNullOrEmpty(objectPath) && string.IsNullOrEmpty(objectType))
            {
                if (string.IsNullOrEmpty(objectName))
                {
                    return ServiceResponse.CreateError(
                        "Either 'objectPath' or 'objectName' must be provided. " +
                        "Example: objectName='CustTable' (type will be auto-detected)");
                }

                Logger.Information("[CrossReference] Starting auto-detect lookup - Object: {ObjectName}, Direction: {Direction}, Kind: {Kind}, MaxResults: {Max}, IncludeMembers: {IncludeMembers}",
                    objectName, direction, kindFilter.ToString(), maxResults, includeMembers);

                try
                {
                    var result = _crossRefService.FindReferencesAutoDetect(
                        objectName, memberName, direction, kindFilter, maxResults, includeMembers, maxMemberResults);

                    Logger.Information("[CrossReference] Completed auto-detect lookup - Object: {ObjectName}, DetectedTypes: [{Types}], TotalRefs: {Found}/{Total}",
                        objectName,
                        result.DetectedTypes != null ? string.Join(", ", result.DetectedTypes) : "none",
                        result.TotalFound, result.TotalAvailable);

                    if (result.TypeResults != null)
                    {
                        foreach (var typeResult in result.TypeResults)
                        {
                            Logger.Information("[CrossReference]   {Type}: {Found} references",
                                typeResult.DetectedType, typeResult.TotalAvailable);
                        }
                    }

                    return ServiceResponse.CreateSuccess(result);
                }
                catch (Exception ex)
                {
                    Logger.Error(ex, "[CrossReference] Failed auto-detect cross-reference query for {ObjectName}", objectName);
                    return ServiceResponse.CreateError($"Cross-reference query failed: {ex.Message}");
                }
            }

            // Explicit type mode: build the XRef path from objectType + objectName
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

            Logger.Information("[CrossReference] Starting lookup - Object: {ObjectPath}, Direction: {Direction}, Kind: {Kind}, MaxResults: {Max}, IncludeMembers: {IncludeMembers}",
                objectPath, direction, kindFilter.ToString(), maxResults, includeMembers);

            try
            {
                CrossReferenceResult result;

                if (includeMembers)
                {
                    // Batch mode: discover actual members and fetch all cross-references in one call
                    result = _crossRefService.FindReferencesWithMembers(objectPath, direction, kindFilter, maxResults, maxMemberResults);

                    Logger.Information("[CrossReference] Completed batch lookup - Object: {ObjectPath}, Direction: {Direction}, " +
                        "ObjectRefs: {Found}/{Total}, MembersDiscovered: {Members}",
                        objectPath, direction, result.TotalFound, result.TotalAvailable,
                        result.MemberResults?.Count ?? 0);

                    if (result.MemberResults != null)
                    {
                        foreach (var memberResult in result.MemberResults)
                        {
                            if (memberResult.TotalAvailable > 0)
                            {
                                Logger.Information("[CrossReference]   Member {MemberName}: {Found} references",
                                    memberResult.MemberName, memberResult.TotalAvailable);
                            }
                        }
                    }
                }
                else
                {
                    result = _crossRefService.FindReferences(objectPath, direction, kindFilter, maxResults);

                    Logger.Information("[CrossReference] Completed lookup - Object: {ObjectPath}, Direction: {Direction}, ReferencesFound: {Found}, TotalAvailable: {Total}",
                        objectPath, direction, result.TotalFound, result.TotalAvailable);
                }

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

        #region Path Building & Parsing

        /// <summary>
        /// Builds an XRef logical path from object type, name, and optional member.
        /// Maps D365 metadata types to XRef path prefixes.
        /// </summary>
        private static string BuildXRefPath(string objectType, string objectName, string memberName)
        {
            if (string.IsNullOrEmpty(objectName))
                return null;

            var prefix = MapObjectTypeToXRefPrefix(objectType);
            if (string.IsNullOrEmpty(prefix))
                return null;

            // Labels use a special path format: /Labels/@LabelFileID:LabelID
            if (prefix == "Labels")
            {
                var labelRef = objectName.StartsWith("@") ? objectName : "@" + objectName;
                return $"/Labels/{labelRef}";
            }

            var path = $"/{prefix}/{objectName}";

            if (!string.IsNullOrEmpty(memberName))
            {
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

        /// <summary>
        /// Parses a reference kind string into the CrossReferenceKind enum.
        /// </summary>
        private static CrossReferenceKind ParseReferenceKind(string kind)
        {
            if (string.IsNullOrEmpty(kind))
                return CrossReferenceKind.Any;

            return kind.ToLowerInvariant() switch
            {
                "any" or "all" => CrossReferenceKind.Any,
                "methodcall" or "method" or "call" => CrossReferenceKind.MethodCall,
                "typereference" or "type" or "reference" => CrossReferenceKind.TypeReference,
                "interfaceimplementation" or "interface" or "implements" => CrossReferenceKind.InterfaceImplementation,
                "classextended" or "extends" or "inheritance" => CrossReferenceKind.ClassExtended,
                "testcall" or "test" => CrossReferenceKind.TestCall,
                "property" => CrossReferenceKind.Property,
                "attribute" => CrossReferenceKind.Attribute,
                "methodoverride" or "override" => CrossReferenceKind.MethodOverride,
                "tag" => CrossReferenceKind.Tag,
                _ => Enum.TryParse<CrossReferenceKind>(kind, true, out var parsed) ? parsed : CrossReferenceKind.Any
            };
        }

        #endregion
    }
}
