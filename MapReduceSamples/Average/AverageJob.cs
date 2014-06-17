using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
                OutputFolder = "output/Average"
            };
        }
    }
}
