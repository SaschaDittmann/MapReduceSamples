using Microsoft.Hadoop.MapReduce;

namespace InvertedIndex
{
    public class InvertedIndexJob : HadoopJob<WikipediaExtractor, Concatenator>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/posts",
                OutputFolder = "output/InvertedIndex",
            };
        }
    }
}
