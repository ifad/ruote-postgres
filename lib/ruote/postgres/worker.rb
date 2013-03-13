require 'ruote/worker'

Ruote::Worker.class_eval do
  def run
    if @storage.respond_to? :wait_for_notify
      @storage.wait_for_notify(60) do
        step
      end while @state != 'stopped'
    else
      step while @state != 'stopped'
    end
  end
end
