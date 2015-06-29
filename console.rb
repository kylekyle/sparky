require 'pry'
require 'rb-readline'

class Binding
	remove_method :local_variable_set
end

Pry.start