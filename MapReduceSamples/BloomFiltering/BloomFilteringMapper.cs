using System.Linq;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace BloomFiltering
{
    public class BloomFilteringMapper : MapperBase
    {
        private BloomFilter<string> _filter; 

        public override void Initialize(MapperContext context)
        {
            base.Initialize(context);

            _filter = BloomFilter<string>.Load(@"filter.bloom");
        }

        public override void Map(string inputLine, MapperContext context)
        {
            var words = inputLine.Split(' ');
            if (words.Any(w => _filter.Contains(w)))
                context.EmitLine(inputLine);
        }
    }
}
