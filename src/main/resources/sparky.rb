require 'jruby/core_ext'

import 'org.apache.spark.SparkConf'
import 'org.apache.spark.api.ruby.Sparky'
import 'org.apache.spark.api.java.JavaRDD'
import 'org.apache.spark.api.java.JavaSparkContext'

class JavaRDD
	instance_methods.each do |method|
		define_method method do |*args, &block|
			if block.nil?
				super *args
			else
        puts block.to_bytes
        super Sparky.new block 
			end
		end
	end
end

# p sc.text_file('words.txt').map{|a| p a; [a.reverse.to_java(:string)]}.collect.to_a