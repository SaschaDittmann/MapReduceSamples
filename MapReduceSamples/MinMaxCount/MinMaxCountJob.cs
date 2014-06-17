using Microsoft.Hadoop.MapReduce;

namespace MinMaxCount
{
    public class MinMaxCountJob : HadoopJob<MinMaxCountMapper, MinMaxCountCombiner, MinMaxCountReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/MinMaxCount"
            };
        }
    }
}
