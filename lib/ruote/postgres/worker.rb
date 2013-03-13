require 'ruote/worker'

Ruote::Worker.class_eval do
  def run
    @storage.wait_for_notify(6) do
      step
    end while @state != 'stopped'
  end
end
