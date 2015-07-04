require 'tmpdir'
require 'jruby/core_ext'

import 'org.apache.spark.SparkConf'
import 'org.apache.spark.api.ruby.Sparky'
import 'org.apache.spark.api.java.JavaRDD'
import 'org.apache.spark.api.java.AbstractJavaRDDLike'
import 'org.apache.spark.api.java.JavaSparkContext'

class JavaRDD
	instance_methods.each do |method|
		define_method method do |*args, &block|
			if block.nil?
				super *args
			else
				Dir.mktmpdir do |dir|
					klass = Class.new do
					  define_method :call, &block
					  become_java! dir
					end

					java_class_name = klass.java_class.get_name
					ruby_class_name = java_class_name[/[A-Z].+/]
					java_class_path = File.join dir, *java_class_name.split('.')

					Object.const_set ruby_class_name, klass
					class_bytes = File.read(java_class_path + '.class').to_java_bytes
					instance = Object.const_get(ruby_class_name).new
					
					super Sparky.new(class_bytes, instance)
				end
			end
		end
	end
end