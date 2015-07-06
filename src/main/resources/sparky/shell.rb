require 'pry'
require 'rb-readline'

require 'sparky'
require 'sparky/version'

def sc
	@sc ||= JavaSparkContext.new SparkConf.new.set_app_name("Sparky Shell")
end

banner=<<-'BANNER'
Welcome to
      ____              __
     / __/__  ___ _____/ /___  __
     \ \/ _ \/ _ `/ __/  '_/.\/ /
   /__ / .__/\_,_/_/ /_/\_\ \  / version %s
      /_/                   /_/

Using Spark version %s, Ruby %s (JRuby %s), and Java %s (%s)
Spark context available as sc.
BANNER

jvm = java.lang.System.getProperty 'java.vm.name'
java_version = java.lang.System.getProperty 'java.version'

puts banner % [Sparky::VERSION, SPARK_VERSION, RUBY_VERSION, JRUBY_VERSION, java_version, jvm]

class Binding
	remove_method :local_variable_set
end

Pry.config.prompt_name = 'sparky'
Pry.start