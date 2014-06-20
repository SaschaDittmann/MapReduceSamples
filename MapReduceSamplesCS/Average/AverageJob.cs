using Microsoft.Hadoop.MapReduce;

namespace Average
{
    public class AverageJob : HadoopJob<AverageMapper, AverageReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/Average",
            };
        }
    }
}
