require 'tmpdir'
require 'jruby/core_ext'

import 'org.apache.spark.SparkConf'
import 'org.apache.commons.io.IOUtils'
import 'org.apache.spark.api.java.JavaSparkContext'
import 'org.apache.spark.api.java.function.FlatMapFunction'

configuration = SparkConf.new.setAppName "JavaWordCount"
context = JavaSparkContext.new configuration
split = nil

Dir.mktmpdir do |dir|
	block = proc do |line|
		line.split
	end

	klass = Class.new do
	  define_method :call, &block
	  become_java! dir
	end

	java_class_name = klass.java_class.get_name
	ruby_class_name = java_class_name[/[A-Z].+/]
	java_class_path = File.join dir, *java_class_name.split('.')

	Object.const_set ruby_class_name, klass
	split = Java::Sparky.new File.read(java_class_path + '.class').to_java_bytes, Object.const_get(ruby_class_name).new
end

lines = context.textFile 'words.txt', 1
result = lines.flatMap split

result.collect.each {|result| p result}