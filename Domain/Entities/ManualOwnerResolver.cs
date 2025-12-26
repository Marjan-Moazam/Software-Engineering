using System;
using System.Collections.Generic;

namespace ETL.HubspotService.Domain.Entities
{
    internal static class ManualOwnerResolver
    {
        private static readonly Dictionary<string, string> Overrides = new(StringComparer.OrdinalIgnoreCase)
        {
            ["30370889"] = "Dhasu Prasanna",
            ["30685617"] = "Marjan Moazam",
            ["827734017"] = "Epicore Mail",
            ["1016954349"] = "Service RTS",
            ["1342342618"] = "Mikael Kjærgaard",
            ["1364655067"] = "Maria del Carmen Riccio-kjærgaard",
            ["1316638712"] = "Sofie Dahlerup (Deactivated/Removed)",
            ["712401897"] = "Newsletter RTS (Deactivated/Removed)",
            ["1319774167"] = "Algori Marketing (Deactivated/Removed)",
            ["598095810"] = "Developer Account (Deactivated/Removed)",
            ["2032328179"] = "Developer Account (Deactivated/Removed)",
            ["1342332605"] = "Søren Østergaard (Deactivated/Removed)",
            ["1284170621"] = "Daniel Michalik (Deactivated/Removed)"
        };

        internal static string? Resolve(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return value;
            }

            return Overrides.TryGetValue(value, out var label) ? label : value;
        }
    }
}

