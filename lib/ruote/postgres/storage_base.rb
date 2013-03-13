require 'ruote/storage/base'

Ruote::StorageBase.module_eval do
  def wait_for_notify(i, &block)
    block.call
  end
end
