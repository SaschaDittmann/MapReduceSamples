using System;
using System.Collections.Generic;

namespace MapReduceSamples.Utils
{
    public class XmlUtils
    {
        public static Dictionary<String, String> ParseXml(String xml)
        {
            try
            {
                var attributes = new Dictionary<String, String>();

                var tokens = xml.Trim().Substring(5, xml.Trim().Length - 8).Split('"');
                for (var i = 0; i < tokens.Length - 1; i += 2)
                {
                    attributes.Add(tokens[i].Trim(' ', '='), tokens[i + 1]);
                }

                return attributes;
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
