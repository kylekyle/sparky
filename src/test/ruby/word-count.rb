require 'java'
require 'jruby/core_ext'

import 'scala.Tuple2'
import 'org.apache.spark.SparkConf'
import 'org.apache.spark.api.java.JavaSparkContext'
import 'org.apache.spark.api.java.function.FlatMapFunction'
import 'org.apache.spark.api.java.function.PairFunction'
import 'org.apache.spark.api.java.function.Function2'

configuration = SparkConf.new.setAppName "JavaWordCount"
context = JavaSparkContext.new configuration

lines = context.textFile 'words.txt', 1

class TheOneThatMadeIt
  include FlatMapFunction

  def call o
    puts "hello from jRuby"
  end

  become_java!
end

`rm test.jar`
TheOneThatMadeIt.become_java! '.'
`jar -cf test.jar rubyobj`
context.add_jar 'test.jar'

foo = Java::SparkyFlatMapFunction.new
foo.klass = File.read("rubyobj/TheOneThatMadeIt.class").to_java_bytes

words = lines.flatMap TheOneThatMadeIt.new
output = words.collect

output.each {|a| p a}
context.stop