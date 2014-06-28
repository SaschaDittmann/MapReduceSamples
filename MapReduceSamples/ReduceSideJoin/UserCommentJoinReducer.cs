using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace ReduceSideJoin
{
    public class UserCommentJoinReducer : ReducerCombinerBase
    {
        private JoinType _joinType;
        public override void Initialize(ReducerCombinerContext context)
        {
            base.Initialize(context);
            _joinType = JoinType.InnerJoin;
        }

        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            var leftValues = new List<string>();
            var rightValues = new List<string>();

            foreach (var value in values)
            {
                if (value[0] == 'L')
                    leftValues.Add(value.Substring(1).Trim());
                else
                    rightValues.Add(value.Substring(1).Trim());
            }

            switch (_joinType)
            {
                case JoinType.InnerJoin:
                    if (!leftValues.Any() || !rightValues.Any())
                        return;

                    foreach (var leftValue in leftValues)
                    {
                        foreach (var rightValue in rightValues)
                        {
                            context.EmitKeyValue(leftValue, rightValue);
                        }
                    }
                    
                    break;
                case JoinType.LeftJoin:
                    foreach (var leftValue in leftValues)
                    {
                        if (rightValues.Any())
                        {
                            foreach (var rightValue in rightValues)
                            {
                                context.EmitKeyValue(leftValue, rightValue);
                            }    
                        }
                        else
                            context.EmitKeyValue(leftValue, String.Empty);
                    }

                    break;
                case JoinType.RightJoin:
                    foreach (var rightValue in rightValues)
                    {
                        if (leftValues.Any())
                        {
                            foreach (var leftValue in leftValues)
                            {
                                context.EmitKeyValue(leftValue, rightValue);
                            }
                        }
                        else
                            context.EmitKeyValue(String.Empty, rightValue);
                    }

                    break;
                case JoinType.FullOuter:
                    if (leftValues.Any())
                    {
                        foreach (var leftValue in leftValues)
                        {
                            if (rightValues.Any())
                            {
                                foreach (var rightValue in rightValues)
                                {
                                    context.EmitKeyValue(leftValue, rightValue);
                                }
                            }
                            else
                                context.EmitKeyValue(leftValue, String.Empty);
                        }
                    }
                    else
                        foreach (var rightValue in rightValues)
                        {
                            context.EmitKeyValue(String.Empty, rightValue);
                        }

                    break;
            }
            
        }
    }
}
