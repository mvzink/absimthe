require 'actor'

require 'rubygems'
require 'ffi-rzmq'
require 'uuid'

# I don't really want to do this monkey patching... but I kinda do
class Hash
  def to_uuid
    if self.include? :to_uuid
      return self[:to_uuid]
    end
  end
end

module Absimth

  module Agent

    def self.extended(cls)
      cls.send :include, self
      super
    end

    def initialize(opts={})
      @timestamp = 0
      self.class.class_variable_get(:@@init_hooks).each do |name,default_blk|
        self.instance_eval(&default_blk) unless opts.include?(name.to_sym)
      end
      opts.each do |k,v|
        m = "#{k}="
        if respond_to? m
          send(m, v)
        end
      end
    end

    def attribute(name, opts={})
      name = name.to_s if name.kind_of?(Symbol)

      define_method name do
        instance_variable_get("@#{name}")
      end

      define_method (name + '=') do |v|
        instance_variable_set("@#{name}", v)
      end

      default = opts[:default]
      unless default.nil?
        unless self.class_variable_defined?(:@@init_hooks)
          self.class_variable_set(:@@init_hooks, {})
        end
        ihs = self.class_variable_get(:@@init_hooks)
        ihs[name] = proc {
          if default.kind_of?(Proc)
            v = default.call
          else
            v = default
          end
          send(name + '=', v)
        }
      end
    end

    def interactions(ary=nil)
      unless self.class_variable_defined?(:@@interactions)
        self.class_variable_set(:@@interactions, Set[])
      end
      list = self.class_variable_get(:@@interactions)
      if ary
        list = list | ary
        self.class_variable_set(:@@interactions, list)
      end
      return list
    end

    def accepts_interaction?(sym)
      return self.interactions.include?(sym)
    end

    def from
      return self.instance_variable_get(:@from)
    end

    def from=(o)
      self.instance_variable_set(:@from, o)
    end

    ANY = Object.new
    def ANY.===(obj)
      true
    end

    def loop
      # TODO: Exit condition lol; probably clock limit on sim
      running = true
      while running

        messages = true
        while messages
          Actor.receive do |f|
            f.when(ANY) do |msg|
              if msg.kind_of?(Hash) and msg[:type] == :interaction
                self.from = AgentWrapper.new(*msg[:from])
                self.send(msg[:method], *msg[:args])
              else
                puts "#{self.class}(#{self.object_id}): Don't know how to handle this message: #{msg}"
              end
            end
            f.after(0.0) do
              messages = false
            end
          end
        end # done checking messages

        @timestamp += 1
        act
        sleep 1 # temporary measure :P

      end # done running
    end

  end # module Agent

  class AgentDelegate

    def uuid
      @uuid
    end

    def cls
      @cls
    end

    def send(msg)
      raise "bare AgentWrapper getting a message!"
    end
    alias_method :<<, :send

  end # class AgentDelegate

  class LocalAgentDelegate < AgentDelegate
    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
      @actor = Actor.spawn_link do
        puts "Start yo bitch up"
        begin
          Thread.current[:agent_delegate] = self
          cls.new(opts).loop
        rescue Exception => e
          logfile = File.join("logs", "#{Actor.current.object_id}.crash")
          File.open(logfile, "a") { |f|
            e.render("#{@cls}(#{Actor.current.object_id}) crashed", f, false)
          }
          puts "#{@cls}(#{Actor.current.object_id}) crashed"
        end
      end
    end

    def send(msg)
      @actor << msg
    end
    alias_method :<<, :send

  end # class LocalAgentDelegate

  class RemoteAgentDelegate < AgentDelegate

    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
    end

    def send(msg)
      msg[:to_uuid] = @uuid
      Actor[:comms] << msg
    end
    alias_method :<<, :send

  end # class RemoteAgentDelegate

  class AgentWrapper

    def initialize(cls, uuid)
      @cls = cls
      @uuid = uuid
      update_delegate
    end

    def update_delegate
      @delegate = nil
      thr = Thread.list.select { |t| !t[:agent_delegates].nil? }.first
      if thr
        @delegate = thr[:agent_delegates][@uuid]
      end
      unless @delegate
        @delegate = RemoteAgentDelegate.new(@cls, @uuid)
      end
      @delegate
    end

    def method_missing(meth, *args, &blk)
      delegate = Thread.current[:agent_delegate]
      if @cls.accepts_interaction?(meth)
        self << {
          :type => :interaction,
          :method => meth,
          :args => args,
          :from => [delegate.cls, delegate.uuid]
        }
      else
        super
      end
    end

    def send(msg)
      (@delegate or update_delegate) << msg
    end
    alias_method :<<, :send

  end # class AgentWrapper

  class Simulation
    def initialize(opts={})
      # TODO: Validate options with ZMQ
      @control_endpoint = opts[:control_endpoint]
      @comm_endpoint = opts[:comm_endpoint]
    end

  end # class Simulation

  class SimulationMaster < Simulation

    def initialize(opts={}, &blk)
      raise "No block given to simulation master" unless blk
      @spawn_blk = blk
      super
    end

    def run(t=0)
      @ctx = ZMQ::Context.create(1)
      @control_faucet = ControlFaucet.new(@ctx, @control_endpoint)

      puts "Alright, gonna do some spawning and stuff"

      self.instance_eval &@spawn_blk
      puts "Alright, we are done setting up, let's take a nap"
      sleep t

      @control.close
      @ctx.terminate

      puts "Cleaned up fine"
    end

    def spawn(cls, opts={})
      address = @control.recv
      empty = @control.recv
      ready = @control.recv
      raise "Dicks!" unless ready[:signal] == :ready
      puts "Got a ready slave, gonna send a control signal"
      agent_uuid = UUID.generate
      @control.send({
        :signal => :spawn,
        :cls => cls,
        :agent_uuid => agent_uuid
      }.merge(opts))
      puts "Cool, successfully got some chump to run an agent"
      return AgentWrapper.new(cls, agent_uuid)
    end

  end # class SimulationMaster

  class SimulationSlave < Simulation

    def initialize(opts={})
      @agent_delegates = {}
      Thread.current[:agent_delegates] = @agent_delegates
      super
    end

    def run(t=0)
      @ctx = ZMQ::Context.create(1)
      @control_sink = ControlSink.new(@ctx, @control_endpoint)
      @comms = Pipe.new(@ctx,
        :in => {:endpoint => @comm_endpoint, :type => ZMQ::SUB},
        :out => {:endpoint => @comm_endpoint, :type => ZMQ::PUB}
      )

      comm_recv_thread = Thread.new do
        puts "Alright, listening for comms"
        while (comm_msg = @comms.recv)
          puts "WHOAAA, doing some comms"
          handle_comms_msg comm_msg
        end
      end
      comm_send_actor = Actor.new do
        loop do
          msg = Actor.receive
          puts "WHOOAAAHOHOOO, sending some messages"
          if msg.respond_to? :to_uuid
            @comms.send(msg)
          end
        end
      end
      Actor.register(:comms, comm_send_actor)
      puts "Alright, listening for control signals"
      loop do
        @control_sink.send {:signal => :ready}
        control_msg = @control_sink.recv
        puts "Wowzers, getting controlled!"
        handle_control_msg control_msg
      end
      puts "Okay, guess we're done"

      @control.close
      @comms.close
      @ctx.terminate

      puts "Cleaned up safely"
    end

    def handle_control_msg(msg)
      if msg[:signal] == :spawn
        a = spawn msg.delete(:cls), msg
        @control.send({
          :uuid => msg[:uuid],
          :signal => :ok,
          :agent_uuid => a.uuid
        })
      end
    end

    def handle_comms_msg(msg)
      @agent_delegates[msg.to_uuid] << msg
    end

    def spawn(cls, opts={})
      unless @comms
        raise "SimulationSlave#spawn called without a connected comms pipe!"
      end
      # TODO: Should do the spawning wherever is most appropriate
      # For simplicity, we'll initially just rely on push/pull sockets
      aw = LocalAgentDelegate.new(cls, opts)
      @agent_delegates[aw.uuid] = aw
      @comms.subscribe(aw.uuid)
      return aw
    end

  end # class SimulationSlave

  class ControlFaucet

    def initialize(ctx, opts={})
      @socket.close
    end

  end

  class ControlSink

    def initialize(ctx, opts={})

    end

    def close
      @socket.close
    end

  end

  class FuckedPipe

    def initialize(ctx, opts={})
      raise "Need options for pipe" unless !opts.empty?
      @in_socket = ctx.socket(opts[:in][:type])
      @out_socket = ctx.socket(opts[:out][:type])
      ec @in_socket.setsockopt(ZMQ::LINGER, 0)
      ec @in_socket.connect(opts[:in][:endpoint])
      ec @out_socket.setsockopt(ZMQ::LINGER, 0)
      ec @out_socket.connect(opts[:out][:endpoint])
    end

    def close
      ec(@in_socket.close) or ec(@out_socket.close)
    end

    def send(obj)
      msg = Marshal.dump(obj)
      if @out_socket.name == "PUB"
        unless obj.respond_to? :to_uuid
          raise "Can't send a to_uuid-less object on a comms pipe!"
        end
        msg = obj.to_uuid + msg
      end
      rc = @out_socket.send_string(msg)
      return ec(rc)
    end

    def recv
      msg = ''
      rc = @in_socket.recv_string(msg)
      ec(rc)
      if @in_socket.name == "SUB"
        msg = msg[36..-1]
      end
      return load_obj(msg)
    end

    def load_obj(msg)
      return Marshal.load(msg, lambda {|obj|
        if obj.class == AgentWrapper
          obj.update_delegate
        end
      })
    end

    def subscribe(str)
      ec(@in_socket.setsockopt(ZMQ::SUBSCRIBE, str))
    end

    def ec(rc)
      if ZMQ::Util.resultcode_ok?(rc)
        true
      else
        raise "ZMQ operation failed [#{ZMQ::Util.errno}]: #{ZMQ::Util.error_string}"
      end
    end

  end # class Pipe

end # module Absimth

