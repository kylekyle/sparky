require 'jruby/core_ext'

import 'org.apache.spark.SparkConf'
import 'org.apache.commons.io.IOUtils'
import 'org.apache.spark.api.java.JavaSparkContext'
import 'org.apache.spark.api.java.function.FlatMapFunction'

configuration = SparkConf.new.setAppName "JavaWordCount"
context = JavaSparkContext.new configuration

lines = context.textFile 'words.txt', 1

class SplitLines
  include FlatMapFunction

  def call line
    line.split
  end

  become_java!
end

split = Java::SparkyFlatMapFunction.new 
split.klass = File.read('rubyobj/SplitLines.class').to_java_bytes
split.instance = Java::Sparky.serialize SplitLines.new

split = lines.flatMap split
split.collect.each {|result| p result}