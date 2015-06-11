require 'java'

sparkConf = org.apache.spark.SparkConf.new.setAppName("JavaWordCount")
ctx = org.apache.spark.api.java.JavaSparkContext.new sparkConf
lines = ctx.textFile 'word-count.txt', 1

words = lines.flatMap(FlatMapFunction.new do
    def call s
        s.split
    end
end)

=begin
JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
    public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
    }
});

JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
    public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
    }
});

List<Tuple2<String, Integer>> output = counts.collect();
for (Tuple2<?,?> tuple : output) {
    System.out.println(tuple._1() + ": " + tuple._2());
}

ctx.stop
=end