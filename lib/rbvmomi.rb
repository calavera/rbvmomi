# Copyright (c) 2010 VMware, Inc.  All Rights Reserved.
module RbVmomi

require 'excon'
require 'progressbar'



# @private
# @deprecated Use +RbVmomi::VIM.connect+.
def self.connect opts
  VIM.connect opts
end

end
require 'rbvmomi/connection'
require 'rbvmomi/vim'
