using System;
using System.Threading.Tasks;
using D365MetadataService.Models;
using D365MetadataService.Services;
using Serilog;

namespace D365MetadataService.Handlers
{
    /// <summary>
    /// Handler for executing modification methods on D365 objects
    /// Phase 2: Actually perform the modifications discovered in Phase 1
    /// </summary>
    public class ExecuteObjectModificationHandler : BaseRequestHandler
    {
        private readonly D365ReflectionService _reflectionService;

        public ExecuteObjectModificationHandler(
            ILogger logger,
            D365ReflectionService reflectionService) : base(logger)
        {
            _reflectionService = reflectionService;
        }

        public override string SupportedAction => "executeObjectModification";

        protected override async Task<ServiceResponse> HandleRequestAsync(ServiceRequest request)
        {
            try
            {
                // Extract parameters
                if (!request.Parameters.TryGetValue("objectType", out var objectTypeObj) || objectTypeObj == null)
                {
                    return ServiceResponse.CreateError("objectType parameter is required");
                }

                if (!request.Parameters.TryGetValue("objectName", out var objectNameObj) || objectNameObj == null)
                {
                    return ServiceResponse.CreateError("objectName parameter is required");
                }

                if (!request.Parameters.TryGetValue("methodName", out var methodNameObj) || methodNameObj == null)
                {
                    return ServiceResponse.CreateError("methodName parameter is required");
                }

                var objectType = objectTypeObj.ToString();
                var objectName = objectNameObj.ToString();
                var methodName = methodNameObj.ToString();

                if (string.IsNullOrWhiteSpace(objectType))
                {
                    return ServiceResponse.CreateError("objectType cannot be empty");
                }

                if (string.IsNullOrWhiteSpace(objectName))
                {
                    return ServiceResponse.CreateError("objectName cannot be empty");
                }

                if (string.IsNullOrWhiteSpace(methodName))
                {
                    return ServiceResponse.CreateError("methodName cannot be empty");
                }

                // Extract model parameter (optional but important for correct model targeting)
                string model = null;
                if (request.Parameters.TryGetValue("model", out var modelObj) && modelObj != null)
                {
                    model = modelObj.ToString();
                    Logger.Information("[ExecuteModification] Model specified for modification: {Model}", model);
                }

                // Extract method parameters (optional)
                var methodParameters = new System.Collections.Generic.Dictionary<string, object>();
                if (request.Parameters.TryGetValue("parameters", out var parametersObj) && parametersObj != null)
                {
                    if (parametersObj is Newtonsoft.Json.Linq.JObject jObj)
                    {
                        foreach (var prop in jObj.Properties())
                        {
                            methodParameters[prop.Name] = prop.Value?.ToObject<object>();
                        }
                    }
                    else if (parametersObj is System.Collections.Generic.Dictionary<string, object> dict)
                    {
                        methodParameters = dict;
                    }
                }

                Logger.Information("[ExecuteModification] Starting parameter validation for {MethodName} on {ObjectType}:{ObjectName} with {ParameterCount} parameters", 
                    methodName, objectType, objectName, methodParameters.Count);

                // VALIDATION: Check if provided parameters match what the target type requires
                // This prevents silent failures and guides agents to use discovery tool
                var validationResult = await _reflectionService.ValidateModificationParametersAsync(
                    objectType, objectName, methodName, methodParameters);
                    
                if (!validationResult.IsValid)
                {
                    var errorMessage = validationResult.ErrorMessage;
                    
                    Logger.Warning("[ExecuteModification] Parameter validation failed for {MethodName} on {ObjectType}:{ObjectName}: {Error}", 
                        methodName, objectType, objectName, validationResult.ErrorMessage);
                    
                    return ServiceResponse.CreateError(errorMessage);
                }

                Logger.Information("[ExecuteModification] Parameter validation successful. Executing modification: {MethodName} on {ObjectType}:{ObjectName}", 
                    methodName, objectType, objectName);

                // Use the dynamic reflection service to execute the modification with model targeting
                var result = await _reflectionService.ExecuteObjectModificationAsync(objectType, objectName, methodName, methodParameters, model);

                Logger.Information("[ExecuteModification] Successfully executed {MethodName} on {ObjectType}:{ObjectName}", 
                    methodName, objectType, objectName);

                return ServiceResponse.CreateSuccess(result);
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "[ExecuteModification] Error executing object modification");
                return ServiceResponse.CreateError($"Error executing modification: {ex.Message}");
            }
        }
    }
}
