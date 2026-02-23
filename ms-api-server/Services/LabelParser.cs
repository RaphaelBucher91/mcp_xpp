using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Collections.Concurrent;
using Serilog;

namespace D365MetadataService.Services
{
    /// <summary>
    /// Label information including text and optional description
    /// </summary>
    public class LabelInfo
    {
        public string LabelId { get; set; }
        public string Text { get; set; }
        public string Description { get; set; }
    }

    /// <summary>
    /// Parser for D365 F&O label files from local metadata.
    /// Handles format: LabelID=Translated text
    /// Optional description lines start with semicolon (;Description)
    /// Example:
    ///   ABBYYAmount=ABBYY Amount
    ///   ;This is an optional description
    ///
    /// Label files are located at:
    ///   PackagesLocalDirectory\{Package}\{Model}\AxLabelFile\LabelResources\{Language}\{LabelFileID}.{language}.label.txt
    /// </summary>
    public class LabelParser
    {
        private readonly List<string> _searchDirectories;
        private readonly ILogger _logger;

        // Cache for parsed label files - key: filePath, value: labels dictionary
        private readonly ConcurrentDictionary<string, Dictionary<string, LabelInfo>> _labelCache = new();

        // Regex patterns for parsing
        // Label lines can be either "LabelID=text" or "@LabelFileID:LabelID=text"
        private static readonly Regex LabelLineRegex = new Regex(@"^@?([A-Za-z0-9_]+(?::[A-Za-z0-9_]+)?)=(.*)$", RegexOptions.Compiled);
        private static readonly Regex DescriptionLineRegex = new Regex(@"^\s*;(.*)$", RegexOptions.Compiled);
        private static readonly Regex LabelReferenceRegex = new Regex(@"^@([A-Za-z0-9_]+):([A-Za-z0-9_]+)$", RegexOptions.Compiled);

        public LabelParser(string packagesDirectory, string customMetadataPath, ILogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            _searchDirectories = new List<string>();

            // Custom metadata path takes priority (layering - custom overrides standard)
            if (!string.IsNullOrEmpty(customMetadataPath) && Directory.Exists(customMetadataPath))
            {
                _searchDirectories.Add(customMetadataPath);
                _logger.Information("[LabelParser] Added custom metadata path: {Path}", customMetadataPath);
            }

            if (!string.IsNullOrEmpty(packagesDirectory) && Directory.Exists(packagesDirectory))
            {
                _searchDirectories.Add(packagesDirectory);
                _logger.Information("[LabelParser] Added packages directory: {Path}", packagesDirectory);
            }

            if (_searchDirectories.Count == 0)
            {
                _logger.Warning("[LabelParser] No valid search directories configured");
            }
        }

        /// <summary>
        /// Get single label text by reference (@LabelFileID:LabelID)
        /// </summary>
        public string GetLabelText(string labelReference, string language = "en-US")
        {
            var labelInfo = GetLabelInfo(labelReference, language);
            return labelInfo?.Text;
        }

        /// <summary>
        /// Get label with description
        /// </summary>
        public LabelInfo GetLabelInfo(string labelReference, string language = "en-US")
        {
            if (string.IsNullOrWhiteSpace(labelReference))
            {
                _logger.Warning("[LabelParser] Empty label reference provided");
                return null;
            }

            // Parse label reference
            var match = LabelReferenceRegex.Match(labelReference);
            if (!match.Success)
            {
                _logger.Warning("[LabelParser] Invalid label reference format: {LabelReference}. Expected format: @LabelFileID:LabelID", labelReference);
                return null;
            }

            var labelFileId = match.Groups[1].Value;
            var labelId = match.Groups[2].Value;

            // Try to find the label file
            var labelFile = FindLabelFile(labelFileId, language);
            if (labelFile == null)
            {
                // Try fallback to en-US if not found
                if (language != "en-US")
                {
                    _logger.Information("[LabelParser] Label file not found for language {Language}, falling back to en-US", language);
                    labelFile = FindLabelFile(labelFileId, "en-US");
                }

                if (labelFile == null)
                {
                    _logger.Warning("[LabelParser] Label file not found: {LabelFileId} for language {Language}", labelFileId, language);
                    return null;
                }
            }

            // Parse the label file and get the label
            var labels = ParseLabelFile(labelFile);
            if (labels.TryGetValue(labelId, out var labelInfo))
            {
                return labelInfo;
            }

            _logger.Warning("[LabelParser] Label not found: {LabelId} in file {LabelFile}", labelId, labelFile);
            return null;
        }

        /// <summary>
        /// Get multiple labels efficiently in a single request
        /// </summary>
        public Dictionary<string, string> GetLabelsBatch(List<string> labelReferences, string language = "en-US")
        {
            var results = new Dictionary<string, string>();

            if (labelReferences == null || labelReferences.Count == 0)
            {
                return results;
            }

            // Group by label file for efficient processing (parse each file once)
            var groupedByFile = new Dictionary<string, List<(string reference, string labelId)>>();

            foreach (var reference in labelReferences)
            {
                var match = LabelReferenceRegex.Match(reference);
                if (match.Success)
                {
                    var labelFileId = match.Groups[1].Value;
                    var labelId = match.Groups[2].Value;

                    if (!groupedByFile.ContainsKey(labelFileId))
                    {
                        groupedByFile[labelFileId] = new List<(string, string)>();
                    }
                    groupedByFile[labelFileId].Add((reference, labelId));
                }
                else
                {
                    _logger.Warning("[LabelParser] Invalid label reference format: {Reference}", reference);
                }
            }

            // Process each label file once
            foreach (var kvp in groupedByFile)
            {
                var labelFileId = kvp.Key;
                var labelFile = FindLabelFile(labelFileId, language);

                if (labelFile == null && language != "en-US")
                {
                    labelFile = FindLabelFile(labelFileId, "en-US");
                }

                if (labelFile == null)
                {
                    _logger.Warning("[LabelParser] Label file not found: {LabelFileId}", labelFileId);
                    continue;
                }

                var labels = ParseLabelFile(labelFile);

                foreach (var (reference, labelId) in kvp.Value)
                {
                    if (labels.TryGetValue(labelId, out var labelInfo))
                    {
                        results[reference] = labelInfo.Text;
                    }
                    else
                    {
                        _logger.Warning("[LabelParser] Label not found: {LabelId} in file {LabelFile}", labelId, labelFile);
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Get available languages for a specific label file
        /// </summary>
        public List<string> GetAvailableLanguages(string packageName, string modelName, string labelFileId)
        {
            var languages = new List<string>();

            try
            {
                foreach (var searchDir in _searchDirectories)
                {
                    var labelResourcesPath = Path.Combine(searchDir, packageName, modelName, "AxLabelFile", "LabelResources");

                    if (!Directory.Exists(labelResourcesPath))
                    {
                        continue;
                    }

                    var languageDirs = Directory.GetDirectories(labelResourcesPath);
                    foreach (var langDir in languageDirs)
                    {
                        var language = Path.GetFileName(langDir);
                        var labelFilePath = Path.Combine(langDir, $"{labelFileId}.{language}.label.txt");

                        if (File.Exists(labelFilePath) && !languages.Contains(language))
                        {
                            languages.Add(language);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "[LabelParser] Error getting available languages for {LabelFileId}", labelFileId);
            }

            return languages;
        }

        /// <summary>
        /// Get available label files in a package/model
        /// </summary>
        public List<string> GetAvailableLabelFiles(string packageName, string modelName, string language = "en-US")
        {
            var labelFiles = new List<string>();

            try
            {
                foreach (var searchDir in _searchDirectories)
                {
                    var labelResourcesPath = Path.Combine(searchDir, packageName, modelName, "AxLabelFile", "LabelResources", language);

                    if (!Directory.Exists(labelResourcesPath))
                    {
                        continue;
                    }

                    var files = Directory.GetFiles(labelResourcesPath, "*.label.txt");
                    foreach (var file in files)
                    {
                        // Strip both .label and .txt extensions: MyLabel.en-US.label.txt -> MyLabel
                        var fileName = Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(file));
                        if (!labelFiles.Contains(fileName))
                        {
                            labelFiles.Add(fileName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "[LabelParser] Error getting available label files");
            }

            return labelFiles;
        }

        /// <summary>
        /// Clear the label cache
        /// </summary>
        public void ClearCache()
        {
            _labelCache.Clear();
            _logger.Information("[LabelParser] Label cache cleared");
        }

        /// <summary>
        /// Find a label file by label file ID and language.
        /// Searches across all configured directories with layering support
        /// (custom metadata overrides standard PackagesLocalDirectory).
        /// </summary>
        private string FindLabelFile(string labelFileId, string language)
        {
            if (_searchDirectories.Count == 0)
            {
                _logger.Warning("[LabelParser] No search directories configured for label lookup");
                return null;
            }

            try
            {
                // Search in priority order (custom first, then standard)
                // The last match wins (layering support), so we collect all and return last
                var foundFiles = new List<string>();

                foreach (var searchDir in _searchDirectories)
                {
                    if (!Directory.Exists(searchDir))
                        continue;

                    var packages = Directory.GetDirectories(searchDir);

                    foreach (var package in packages)
                    {
                        // Search in all models in this package
                        var models = Directory.GetDirectories(package);
                        foreach (var model in models)
                        {
                            var labelFilePath = Path.Combine(model, "AxLabelFile", "LabelResources", language, $"{labelFileId}.{language}.label.txt");

                            if (File.Exists(labelFilePath))
                            {
                                foundFiles.Add(labelFilePath);
                            }
                        }
                    }
                }

                // Return the last found file (custom models override standard ones)
                if (foundFiles.Count > 0)
                {
                    var selectedFile = foundFiles.Last();
                    _logger.Debug("[LabelParser] Found label file: {LabelFile}", selectedFile);
                    return selectedFile;
                }
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "[LabelParser] Error finding label file: {LabelFileId}", labelFileId);
            }

            return null;
        }

        /// <summary>
        /// Parse a label file and return all labels.
        /// Format: LabelID=Translated text
        /// ;Description (optional, next line, starts with semicolon)
        /// Example file content:
        ///   ABBYYActiveErr01=Data source name is not specified
        ///   ABBYYAmount=ABBYY Amount
        ///   ;Optional description for the label above
        /// </summary>
        private Dictionary<string, LabelInfo> ParseLabelFile(string filePath)
        {
            // Check cache first
            if (_labelCache.TryGetValue(filePath, out var cachedLabels))
            {
                return cachedLabels;
            }

            var labels = new Dictionary<string, LabelInfo>();
            LabelInfo currentLabel = null;

            try
            {
                var lines = File.ReadAllLines(filePath);

                foreach (var line in lines)
                {
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        // Empty line resets current label context
                        currentLabel = null;
                        continue;
                    }

                    // Check if it's a label line
                    var labelMatch = LabelLineRegex.Match(line);
                    if (labelMatch.Success)
                    {
                        var labelId = labelMatch.Groups[1].Value;
                        var labelText = labelMatch.Groups[2].Value;

                        currentLabel = new LabelInfo
                        {
                            LabelId = labelId,
                            Text = labelText,
                            Description = null
                        };

                        labels[labelId] = currentLabel;
                        continue;
                    }

                    // Check if it's a description line
                    var descMatch = DescriptionLineRegex.Match(line);
                    if (descMatch.Success && currentLabel != null)
                    {
                        currentLabel.Description = descMatch.Groups[1].Value;
                        continue;
                    }

                    // Unknown line format - reset context
                    currentLabel = null;
                }

                // Cache the results
                _labelCache[filePath] = labels;

                _logger.Information("[LabelParser] Parsed {Count} labels from {FilePath}", labels.Count, filePath);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "[LabelParser] Error parsing label file: {FilePath}", filePath);
            }

            return labels;
        }
    }
}
