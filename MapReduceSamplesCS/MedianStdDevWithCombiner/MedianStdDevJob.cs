using Microsoft.Hadoop.MapReduce;

namespace MedianStdDevWithCombiner
{
    public class MedianStdDevJob : HadoopJob<MedianStdDevMapper, MedianStdDevCombiner, MedianStdDevReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/MedianStdDevWithCombiner"
            };
        }
    }
}
