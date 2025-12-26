using System.Text.Json;
using System.Text.RegularExpressions;

namespace ETL.HubspotService.Domain.Entities
{
    /// <summary>
    /// Helper methods for parsing HubSpot data safely
    /// </summary>
    public static class HubSpotEntityHelper
    {
        /// <summary>
        /// Safely extracts HubSpot ID from JSON element, handling both string and numeric formats
        /// </summary>
        public static string? GetHubSpotId(JsonElement element)
        {
            if (element.TryGetProperty("id", out var idEl))
            {
                if (idEl.ValueKind == JsonValueKind.Number && idEl.TryGetInt64(out var numericId))
                {
                    return numericId.ToString();
                }
                if (idEl.ValueKind == JsonValueKind.String)
                {
                    return idEl.GetString();
                }
            }
            return null;
        }

        /// <summary>
        /// Gets string property from JSON element
        /// </summary>
        public static string? GetStringProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                return property.GetString();
            }
            return null;
        }

        /// <summary>
        /// Gets DateTime property from JSON element
        /// </summary>
        public static DateTime? GetDateTimeProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                var value = property.GetString();
                if (DateTime.TryParse(value, out var dateTime))
                {
                    return dateTime;
                }
            }
            return null;
        }

        /// <summary>
        /// Gets integer property from JSON element
        /// </summary>
        public static int? GetIntProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                if (property.TryGetInt32(out var intValue))
                {
                    return intValue;
                }
            }
            return null;
        }

        /// <summary>
        /// Gets long property from JSON element
        /// </summary>
        public static long? GetLongProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                if (property.TryGetInt64(out var longValue))
                {
                    return longValue;
                }
                // Try parsing as string if numeric parse fails
                var stringValue = property.GetString();
                if (long.TryParse(stringValue, out var parsedLong))
                {
                    return parsedLong;
                }
            }
            return null;
        }

        /// <summary>
        /// Gets decimal property from JSON element
        /// </summary>
        public static decimal? GetDecimalProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                var value = property.GetString();
                if (decimal.TryParse(value, out var decimalValue))
                {
                    return decimalValue;
                }
            }
            return null;
        }
        
        public static string? StripHtml(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return value;
            }

            // Replace line break tags with newline before removing the rest
            var normalized = Regex.Replace(value, @"<(br|BR)\s*/?>", "\n", RegexOptions.Compiled);
            return Regex.Replace(normalized, "<.*?>", string.Empty, RegexOptions.Singleline | RegexOptions.Compiled).Trim();
        }

        /// <summary>
        /// Gets boolean property from JSON element
        /// </summary>
        public static bool GetBoolProperty(JsonElement properties, string propertyName)
        {
            if (properties.TryGetProperty(propertyName, out var property) && property.ValueKind != JsonValueKind.Null)
            {
                if (property.ValueKind == JsonValueKind.True || property.ValueKind == JsonValueKind.False)
                {
                    return property.GetBoolean();
                }
                // Try parsing string "true"/"false"
                var stringValue = property.GetString();
                if (bool.TryParse(stringValue, out var boolValue))
                {
                    return boolValue;
                }
            }
            return false;
        }
    }
}


